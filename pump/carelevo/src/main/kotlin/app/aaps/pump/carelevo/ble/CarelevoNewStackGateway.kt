package app.aaps.pump.carelevo.ble

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.carelevo.coordinator.CarelevoConnectionCoordinator
import kotlinx.coroutines.delay
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Connection-ownership border for running a command over the new coroutine [BleClient] stack from a
 * COORDINATOR (the delivery path). Drops the legacy link (suppressing its reconnect), settles, then runs one
 * command on a fresh [CarelevoBleSession] (which serializes via its own session mutex). This is the shared,
 * reusable form of the three steps `CarelevoActivationExecutor` inlines for the executor ops — see
 * `_docs/CARELEVO_DELIVERY_MIGRATION.md`.
 *
 * The caller MUST be flag-gated (`CARELEVO_USE_NEW_BLE_STACK`) and running on the CommandQueue worker thread
 * (blocked inside the op) so the worker cannot re-dial the legacy link while the new session owns the GATT —
 * the same invariant that makes the executor border safe.
 */
@Singleton
class CarelevoNewStackGateway @Inject constructor(
    private val bleSession: CarelevoBleSession,
    private val connectionCoordinator: CarelevoConnectionCoordinator,
    private val aapsLogger: AAPSLogger
) {

    /**
     * Drop the legacy link → settle → run [command] on a fresh new-transport session. [address] is the patch
     * MAC (lowercase is fine — the session uppercases it). Throws on connect/read timeout or a BLE failure
     * (the caller maps that to a failed PumpEnactResult).
     */
    suspend fun <R : BleResponse> runSingle(address: String, command: BleCommand<R>, timeoutMs: Long = DELIVERY_TIMEOUT_MS): R {
        connectionCoordinator.disconnect(NEW_BLE_SESSION_REASON)
        aapsLogger.debug(LTag.PUMPCOMM, "newStackGateway: dropped legacy link, settling before ${command::class.simpleName}")
        delay(NEW_BLE_SETTLE_MS)
        return bleSession.runSingle(address, command, timeoutMs)
    }

    companion object {

        const val NEW_BLE_SESSION_REASON = "new-ble-session"
        const val NEW_BLE_SETTLE_MS = 1000L
        const val DELIVERY_TIMEOUT_MS = 15_000L
    }
}
