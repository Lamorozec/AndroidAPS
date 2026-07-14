package app.aaps.pump.carelevo.ble

import app.aaps.pump.carelevo.ble.gatt.GattConnState
import app.aaps.pump.carelevo.ble.gatt.GattConnection
import app.aaps.pump.carelevo.ble.gatt.GattEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

/**
 * Production implementation of [BleClient].
 *
 * Correlation rules (see [BleClientContractTest] in tests for the full spec):
 * - A single [requestMutex] serializes outgoing requests — at most one in flight,
 *   whether single-response, multi-response, or streaming.
 * - Before calling [GattConnection.writeCharacteristic], the client registers a
 *   [Waiter] describing the response(s) it expects. This ordering eliminates the
 *   response-during-write race.
 * - A long-lived collector on the injected [scope] reads [GattConnection.events] and
 *   offers each [GattEvent.Notification] to the active [Waiter]; anything the waiter
 *   does not consume is forwarded to [_unsolicitedEvents].
 * - A [GattEvent.ConnectionStateChanged] with [GattConnState.DISCONNECTED] aborts the
 *   pending waiter (with [BleDisconnectedException]) atomically.
 *
 * The event collector is the single point through which every request completes, so it
 * is hardened to survive any callback failure: the [waiter] is held in an
 * [AtomicReference] (so a disconnect can atomically take-and-abort whatever request is
 * current without clobbering a concurrently-registered one), consumer-supplied
 * [BleStreamCommand.decode]/[BleStreamCommand.isTerminal] are guarded, and the collect
 * body is wrapped so no unforeseen throw can permanently stop routing.
 */
class BleClientImpl(
    private val gatt: GattConnection,
    private val writeUuid: UUID,
    @Suppress("unused")
    private val notifyUuid: UUID,
    scope: CoroutineScope
) : BleClient {

    // DROP_OLDEST + tryEmit: unsolicited fan-out (alarms/status are lossy pushes) can
    // never back-pressure the event collector and stall in-flight request correlation.
    private val _unsolicitedEvents = MutableSharedFlow<UnsolicitedMessage>(
        extraBufferCapacity = 64,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )
    override val unsolicitedEvents: SharedFlow<UnsolicitedMessage> = _unsolicitedEvents.asSharedFlow()

    private val requestMutex = Mutex()

    /**
     * The response(s) the in-flight request is waiting for. Exactly one is active at a
     * time (guarded by [requestMutex] on the caller side). Held in an [AtomicReference]
     * because the event collector aborts it on disconnect **outside** the mutex (a
     * stream holds the mutex for its whole 100-210 s duration, so the collector must
     * stay lock-free): [AtomicReference.getAndSet] takes-and-nulls atomically so a
     * disconnect can never null a freshly-registered waiter without also aborting it.
     */
    private val waiterRef = AtomicReference<Waiter?>(null)

    private sealed interface Waiter {

        /** Try to consume the notification [payload] (opcode is [opcode]). Return `true` iff consumed. */
        fun offer(opcode: Byte, payload: ByteArray): Boolean

        /** Abort this pending request (link dropped, or an unforeseen router failure). */
        fun abort(cause: Throwable)
    }

    private class SingleWaiter(
        private val expectedOpcode: Byte,
        private val correlationByte: Byte?,
        private val deferred: CompletableDeferred<ByteArray>
    ) : Waiter {

        override fun offer(opcode: Byte, payload: ByteArray): Boolean {
            if (opcode != expectedOpcode) return false
            val correlationOk = correlationByte == null ||
                (payload.size > 1 && payload[1] == correlationByte)
            if (!correlationOk) return false
            // complete() returns false if already completed (a late duplicate) — report
            // that so routeNotification forwards the extra frame to unsolicited.
            return deferred.complete(payload)
        }

        override fun abort(cause: Throwable) {
            deferred.completeExceptionally(cause)
        }
    }

    private class MultiWaiter(
        private val remaining: MutableSet<Byte>,
        private val deferred: CompletableDeferred<Map<Byte, ByteArray>>
    ) : Waiter {

        private val collected = mutableMapOf<Byte, ByteArray>()

        override fun offer(opcode: Byte, payload: ByteArray): Boolean {
            // First notification per expected opcode wins; a duplicate opcode is no
            // longer in `remaining`, so it falls through to unsolicited.
            if (!remaining.remove(opcode)) return false
            collected[opcode] = payload
            if (remaining.isEmpty()) deferred.complete(collected.toMap())
            return true
        }

        override fun abort(cause: Throwable) {
            deferred.completeExceptionally(cause)
        }
    }

    private class StreamWaiter<R : BleResponse>(
        private val expectedOpcode: Byte,
        private val decode: (ByteArray) -> R,
        private val isTerminal: (R) -> Boolean,
        private val channel: Channel<R>
    ) : Waiter {

        override fun offer(opcode: Byte, payload: ByteArray): Boolean {
            if (opcode != expectedOpcode) return false
            // Both consumer-supplied callbacks run here on the shared event collector,
            // so both are guarded: a throw must end THIS stream, never escape onto the
            // collector and brick routing for every future request.
            val decoded = try {
                decode(payload)
            } catch (t: Throwable) {
                channel.close(t)
                return true
            }
            // Channel is UNLIMITED, so a success means it was accepted; a failure means
            // it is already closed (terminal already delivered) — treat a late same-opcode
            // frame as not-consumed so it falls through to unsolicited.
            if (!channel.trySend(decoded).isSuccess) return false
            val terminal = try {
                isTerminal(decoded)
            } catch (t: Throwable) {
                channel.close(t)
                return true
            }
            if (terminal) channel.close()
            return true
        }

        override fun abort(cause: Throwable) {
            channel.close(cause)
        }
    }

    init {
        scope.launch {
            gatt.events.collect { evt ->
                try {
                    onEvent(evt)
                } catch (t: CancellationException) {
                    throw t
                } catch (t: Throwable) {
                    // The sole event router must never die. Callback throws are already
                    // guarded above; this is a last-resort backstop — abort the active
                    // request so its caller fails fast rather than hanging on a response
                    // that will never route, and keep the collector alive.
                    waiterRef.getAndSet(null)?.abort(t)
                }
            }
        }
    }

    private fun onEvent(evt: GattEvent) {
        when (evt) {
            is GattEvent.Notification           -> routeNotification(evt.payload)

            is GattEvent.ConnectionStateChanged -> {
                if (evt.state == GattConnState.DISCONNECTED) {
                    // Atomically take-and-null so a late-arriving notification falls
                    // through to unsolicited rather than hitting an aborted waiter.
                    waiterRef.getAndSet(null)?.abort(BleDisconnectedException())
                }
            }

            is GattEvent.ServicesDiscovered,
            is GattEvent.WriteAck               -> Unit
        }
    }

    private fun routeNotification(payload: ByteArray) {
        if (payload.isEmpty()) return
        val opcode = payload[0]
        if (waiterRef.get()?.offer(opcode, payload) == true) return
        // tryEmit never suspends (DROP_OLDEST) so the collector can't be back-pressured.
        _unsolicitedEvents.tryEmit(UnsolicitedMessage(opcode, payload))
    }

    override suspend fun <R : BleResponse> request(cmd: BleCommand<R>): R = requestMutex.withLock {
        val deferred = CompletableDeferred<ByteArray>()
        // Register the waiter BEFORE writing so a synchronous response from the
        // peripheral cannot race ahead of our subscription.
        waiterRef.set(SingleWaiter(cmd.expectedResponseOpcode, cmd.correlationByte, deferred))
        try {
            gatt.writeCharacteristic(writeUuid, cmd.encode())
            val payload = deferred.await()
            cmd.decode(payload)
        } finally {
            waiterRef.set(null)
        }
    }

    override suspend fun <R : BleResponse> requestMultiple(cmd: BleMultiCommand<R>): R =
        requestMutex.withLock {
            require(cmd.expectedResponseOpcodes.isNotEmpty()) {
                "expectedResponseOpcodes must not be empty"
            }
            val deferred = CompletableDeferred<Map<Byte, ByteArray>>()
            waiterRef.set(MultiWaiter(cmd.expectedResponseOpcodes.toMutableSet(), deferred))
            try {
                gatt.writeCharacteristic(writeUuid, cmd.encode())
                val responses = deferred.await()
                cmd.decode(responses)
            } finally {
                waiterRef.set(null)
            }
        }

    override fun <R : BleResponse> requestStream(cmd: BleStreamCommand<R>): Flow<R> = flow {
        requestMutex.withLock {
            val channel = Channel<R>(Channel.UNLIMITED)
            // Register BEFORE writing (same race-free ordering as request()).
            waiterRef.set(StreamWaiter(cmd.expectedResponseOpcode, cmd::decode, cmd::isTerminal, channel))
            try {
                gatt.writeCharacteristic(writeUuid, cmd.encode())
                // Emits every decoded notification; the channel is closed by the
                // StreamWaiter on the terminal response, on decode/isTerminal failure
                // (rethrown here as the close cause), or on disconnect.
                for (item in channel) {
                    emit(item)
                }
            } finally {
                waiterRef.set(null)
            }
        }
    }
}
