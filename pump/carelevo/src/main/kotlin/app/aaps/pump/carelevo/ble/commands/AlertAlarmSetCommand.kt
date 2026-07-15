package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand

/**
 * `CMD_ALERT_ALARM_SET_REQ` (0x48) → `CMD_ALERT_ALARM_SET_RES` (0xA8). Sets the patch alert/alarm mode.
 * Medium criticality.
 *
 * Request wire format (2 bytes): `[0] 0x48, [1] mode` — [mode] is written as a **raw byte**.
 *
 * **Quirk — no range validation:** legacy `setAlarmMode` encodes the argument with
 * `CarelevoIntegerToByteTransformerImpl(0, 0)`, and that transformer treats `min == max == 0` as a
 * "skip range-check" sentinel (its guard is `item !in min..max && !(min == 0 && max == 0)` — always
 * `false` when both are 0). So it applies no bounds and just returns `byteArrayOf(mode.toByte())`.
 * To stay byte-for-byte we likewise perform **no** `init {}` range check and emit `mode.toByte()`
 * verbatim (values > 0x7F wrap, exactly as the legacy `.toByte()` does).
 *
 * Response (0xA8): `[0] 0xA8, [1] resultCode` → [SimpleResultResponse] (0 = SUCCESS).
 */
class AlertAlarmSetCommand(
    private val mode: Int
) : BleCommand<SimpleResultResponse> {

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray = byteArrayOf(requestOpcode, mode.toByte())

    override fun decode(responsePayload: ByteArray): SimpleResultResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return SimpleResultResponse(responsePayload.u(1))
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x48
        const val RESPONSE_OPCODE: Byte = 0xA8.toByte()
        private const val MIN_RESPONSE_LENGTH = 2
    }
}
