package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand
import app.aaps.pump.carelevo.ble.BleResponse

/**
 * `CMD_PUMP_RESTART_REQ` (0x27) → `CMD_PUMP_RESTART_RES` (0x87). Resumes (restarts) a paused pump —
 * **SAFETY-CRITICAL** delivery-affecting operation, so the wire layout is reproduced byte-for-byte.
 *
 * Request wire format (3 bytes): `[0] 0x27, [1] mode, [2] subId`.
 * - `mode`  — legacy `CarelevoIntegerToByteTransformerImpl(1, 4)`: range-checked 1..4, emitted as one raw
 *   byte (`mode.toByte()`).
 * - `subId` — legacy `CarelevoSubIdToByteTransformerImpl`: only 0, 1 or 4 are valid and each maps to itself
 *   (4→0x04, 1→0x01, else→0x00); any other value is rejected.
 *
 * Response (0x87): `[0] 0x87, [1] resultCode, [2] mode, [3] subId`. **`subId` is OPTIONAL** — the legacy
 * parser reads `data[3]` only when the response is longer than 3 bytes, otherwise defaults `subId = 0`
 * (variable-length reply reproduced here). The parser also fabricates a `timestamp`/`cmd` that are not on
 * the wire; those are dropped by this pure wire decoder. (The separate 0x88 `BasalRestartRpt` is async and
 * is **not** this response.)
 */
class PumpResumeCommand(
    private val mode: Int,
    private val subId: Int
) : BleCommand<PumpResumeResponse> {

    init {
        require(mode in MODE_RANGE) { "mode $mode out of range $MODE_RANGE" }
        require(subId in VALID_SUB_IDS) { "subId $subId invalid, must be one of $VALID_SUB_IDS" }
    }

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray = byteArrayOf(requestOpcode, mode.toByte(), encodeSubId(subId))

    override fun decode(responsePayload: ByteArray): PumpResumeResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return PumpResumeResponse(
            resultCode = responsePayload.u(1),
            mode = responsePayload.u(2),
            // subId is optional: only present when the reply carries a 4th byte, else 0 (legacy default).
            subId = if (responsePayload.size > SUB_ID_INDEX) responsePayload.u(SUB_ID_INDEX) else 0
        )
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x27
        const val RESPONSE_OPCODE: Byte = 0x87.toByte()

        private val MODE_RANGE = 1..4
        private val VALID_SUB_IDS = setOf(0, 1, 4)
        private const val SUB_ID_INDEX = 3
        private const val MIN_RESPONSE_LENGTH = 3

        /** Mirrors `CarelevoSubIdToByteTransformerImpl`: 4→0x04, 1→0x01, else→0x00 (identity for valid ids). */
        private fun encodeSubId(subId: Int): Byte = when (subId) {
            4    -> 0x04
            1    -> 0x01
            else -> 0x00
        }
    }
}

/**
 * Decoded response from [PumpResumeCommand]: pump [resultCode] (0 = SUCCESS), echoed [mode], and echoed
 * [subId] (0 when the reply omits the optional 4th byte).
 */
data class PumpResumeResponse(
    val resultCode: Int,
    val mode: Int,
    val subId: Int
) : BleResponse
