package app.aaps.pump.carelevo.ble

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.pump.ble.BleAdapter
import app.aaps.core.interfaces.pump.ble.BleGatt
import app.aaps.core.interfaces.pump.ble.BleScanner
import app.aaps.core.interfaces.pump.ble.BleTransportListener
import app.aaps.core.interfaces.pump.ble.PairingState
import app.aaps.core.interfaces.pump.ble.ScannedDevice
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import java.util.UUID
import kotlin.experimental.xor
import kotlin.test.assertFailsWith

/**
 * Tests [CarelevoBleSession.runPairing] against a scripted fake pump — including the **set-time retry
 * round** the legacy stack could never test: `CarelevoConnectNewPatchUseCaseTest.
 * execute_retries_round_when_serial_is_empty` is `@Disabled` because the legacy path funnels every round
 * through one shared `PublishSubject`, so a mock's round-1 events replay into round 2. The new stack
 * registers a FRESH waiter per `requestMultiple` round, so the scenario is scriptable: round 1 answers
 * with an empty serial, round 2 with a valid one, and the retry is observable on the wire (two 0x11
 * writes).
 *
 * Uses `runTest` virtual time with the session's `sessionDispatcher` test seam — the events SharedFlow
 * has no replay, so with real dispatchers an instantly-scripted response can be emitted before the
 * client's router subscription is live and be lost (a suite-load flake); the shared test scheduler
 * makes subscription/emission ordering deterministic.
 */
@OptIn(ExperimentalCoroutinesApi::class)
internal class CarelevoBleSessionPairingTest {

    private val writeUuid: UUID = UUID.fromString("e1b40002-ffc4-4daa-a49b-1c92f99072ab")
    private val notifyUuid: UUID = UUID.fromString("e1b40003-ffc4-4daa-a49b-1c92f99072ab")
    private val aapsLogger: AAPSLogger = mock()

    private lateinit var transport: FakePairingTransport
    private lateinit var session: CarelevoBleSession

    private val spec = CarelevoBleSession.PairingSpec(
        volume = 300,
        remains = 30,
        expiry = 120,
        maxBasalSpeed = 15.0,
        maxBolusDose = 25.0,
        buzzUse = true
    )

    @BeforeEach
    fun setUp() {
        transport = FakePairingTransport()
        session = CarelevoBleSession(transport, writeUuid, notifyUuid, aapsLogger)
    }

    /** Route the session's internal scope onto this test's scheduler (see class KDoc). */
    private fun TestScope.useTestDispatcher() {
        session.sessionDispatcher = StandardTestDispatcher(testScheduler)
    }

    @Test
    fun `runPairing retries the set-time round when the serial comes back empty`() = runTest {
        useTestDispatcher()
        transport.emptySerialRounds = 1

        val result = session.runPairing("94:B2:16:1D:2F:6D", spec)

        // Retry observable on the wire: two 0x11 set-time writes (round 1 empty serial → round 2 valid).
        assertThat(transport.writes.count { it[0] == SET_TIME_OPCODE }).isEqualTo(2)
        assertThat(result.serialNumber).isEqualTo(SERIAL)
        assertThat(result.firmwareVersion).isEqualTo("T168")
        assertThat(result.modelName).isEqualTo("6776514848")
        // Colon MAC built from the 0x9B response bytes, lowercase (legacy convertBytesToHex parity).
        assertThat(result.address).isEqualTo("94:b2:16:1d:2f:6d")

        // The auth write must carry checkSumV2(key) over (MAC bytes + checksum byte), seeded with the
        // random key echoed in the 0x3B request — recompute from the captured wire traffic.
        val key = transport.writes.first { it[0] == MAC_REQUEST_OPCODE }[1]
        val expectedCheckSum = (MAC_BYTES + CHECKSUM_BYTE).fold(key) { acc, b -> acc xor b }
        val authWrite = transport.writes.first { it[0] == APP_AUTH_OPCODE }
        assertThat(authWrite[1]).isEqualTo(expectedCheckSum)
    }

    @Test
    fun `runPairing succeeds on the first round with a single set-time write`() = runTest {
        useTestDispatcher()
        transport.emptySerialRounds = 0

        val result = session.runPairing("94:B2:16:1D:2F:6D", spec)

        assertThat(transport.writes.count { it[0] == SET_TIME_OPCODE }).isEqualTo(1)
        assertThat(result.serialNumber).isEqualTo(SERIAL)
        // Full activation sequence on ONE session, in legacy order.
        assertThat(transport.writes.map { it[0] }).isEqualTo(
            listOf(MAC_REQUEST_OPCODE, APP_AUTH_OPCODE, SET_TIME_OPCODE, ALERT_ALARM_OPCODE, THRESHOLD_OPCODE)
        )
        // Threshold bundle carries the spec verbatim: remains, expiry, basal/bolus unit+centi, buzz.
        val threshold = transport.writes.first { it[0] == THRESHOLD_OPCODE }
        assertThat(threshold).isEqualTo(byteArrayOf(THRESHOLD_OPCODE, 30, 120, 15, 0, 25, 0, 0x01))
    }

    @Test
    fun `runPairing fails when the serial stays empty after all rounds`(): Unit = runTest {
        useTestDispatcher()
        transport.emptySerialRounds = Int.MAX_VALUE

        val e = assertFailsWith<IllegalStateException> {
            session.runPairing("94:B2:16:1D:2F:6D", spec)
        }

        assertThat(e.message).contains("patch info invalid")
        // Both rounds were really attempted before giving up.
        assertThat(transport.writes.count { it[0] == SET_TIME_OPCODE }).isEqualTo(2)
    }

    @Test
    fun `runPairing creates the bond when the device is not bonded`() = runTest {
        useTestDispatcher()
        transport.bonded = false

        session.runPairing("94:B2:16:1D:2F:6D", spec)

        // createBond flips the fake to bonded, so the poll exits immediately.
        assertThat(transport.createBondCalls).isEqualTo(1)
    }

    // ===== Fixtures =====

    /**
     * Scripted patch: answers each opcode like a real CareLevo, with [emptySerialRounds] initial
     * set-time rounds reporting a blank serial (13 spaces) before valid rounds.
     */
    private class FakePairingTransport : CarelevoBleTransport {

        var capturedListener: BleTransportListener? = null
        val writes = mutableListOf<ByteArray>()
        var emptySerialRounds = 0
        var bonded = true
        var createBondCalls = 0

        private var setTimeRounds = 0

        override var scanAddress: String? = null
        override var onGattError133: (() -> Unit)? = null

        override val adapter: BleAdapter = object : BleAdapter {
            override fun enable() {}
            override fun getDeviceName(address: String): String? = null
            override fun isDeviceBonded(address: String): Boolean = bonded
            override fun createBond(address: String): Boolean {
                createBondCalls++
                bonded = true
                return true
            }

            override fun removeBond(address: String) {}
        }

        override val scanner: BleScanner = object : BleScanner {
            override val scannedDevices = MutableSharedFlow<ScannedDevice>()
            override fun startScan() {}
            override fun stopScan() {}
        }

        override val gatt: BleGatt = object : BleGatt {
            override fun connect(address: String): Boolean {
                capturedListener?.onConnectionStateChanged(true)
                return true
            }

            override fun disconnect() {}
            override fun close() {}
            override fun discoverServices() {
                capturedListener?.onServicesDiscovered(true)
            }

            override fun findCharacteristics(): Boolean = true
            override fun enableNotifications() {
                capturedListener?.onDescriptorWritten()
            }

            override fun writeCharacteristic(data: ByteArray) {
                writes += data
                capturedListener?.onCharacteristicWritten()
                respondTo(data[0])
            }
        }

        private fun respondTo(opcode: Byte) {
            val listener = capturedListener ?: return
            when (opcode) {
                MAC_REQUEST_OPCODE -> listener.onCharacteristicChanged(byteArrayOf(MAC_RESPONSE_OPCODE) + MAC_BYTES + CHECKSUM_BYTE)
                APP_AUTH_OPCODE    -> listener.onCharacteristicChanged(byteArrayOf(APP_AUTH_ACK_OPCODE, RESULT_SUCCESS))
                SET_TIME_OPCODE    -> {
                    setTimeRounds++
                    val serial = if (setTimeRounds <= emptySerialRounds) BLANK_SERIAL else SERIAL
                    listener.onCharacteristicChanged(byteArrayOf(RPT1_OPCODE, RESULT_SUCCESS) + serial.toByteArray(Charsets.US_ASCII))
                    listener.onCharacteristicChanged(RPT2_FRAME)
                }

                ALERT_ALARM_OPCODE -> listener.onCharacteristicChanged(byteArrayOf(ALERT_ALARM_ACK_OPCODE, RESULT_SUCCESS))
                THRESHOLD_OPCODE   -> listener.onCharacteristicChanged(byteArrayOf(THRESHOLD_ACK_OPCODE, RESULT_SUCCESS))
            }
        }

        private val _pairingState = MutableStateFlow(PairingState())
        override val pairingState = _pairingState

        override fun updatePairingState(state: PairingState) {
            _pairingState.value = state
        }

        override fun setListener(listener: BleTransportListener?) {
            capturedListener = listener
        }
    }

    private companion object {

        const val MAC_REQUEST_OPCODE: Byte = 0x3B
        const val MAC_RESPONSE_OPCODE: Byte = 0x9B.toByte()
        const val APP_AUTH_OPCODE: Byte = 0x4B
        const val APP_AUTH_ACK_OPCODE: Byte = 0xBB.toByte()
        const val SET_TIME_OPCODE: Byte = 0x11
        const val RPT1_OPCODE: Byte = 0x93.toByte()
        const val ALERT_ALARM_OPCODE: Byte = 0x48
        const val ALERT_ALARM_ACK_OPCODE: Byte = 0xA8.toByte()
        const val THRESHOLD_OPCODE: Byte = 0x1B
        const val THRESHOLD_ACK_OPCODE: Byte = 0x7B
        const val RESULT_SUCCESS: Byte = 0x00

        val MAC_BYTES = byteArrayOf(0x94.toByte(), 0xB2.toByte(), 0x16, 0x1D, 0x2F, 0x6D)
        const val CHECKSUM_BYTE: Byte = 0xAB.toByte()

        const val SERIAL = "EO12507099001" // 13 chars = RPT1 bytes 2..14
        const val BLANK_SERIAL = "             " // 13 spaces → trims to empty → retry round

        // Real-shape 16-byte RPT2: [0]=0x94, [1]=result, [2..5]="T168", [6..10] filler, [11..15] model
        // bytes 0x43,0x4C,0x33,0x30,0x30 → decimal-quirk string "6776514848" (matches the real device).
        val RPT2_FRAME = byteArrayOf(
            0x94.toByte(), 0x00,
            0x54, 0x31, 0x36, 0x38,
            0x00, 0x00, 0x00, 0x00, 0x00,
            0x43, 0x4C, 0x33, 0x30, 0x30
        )
    }
}
