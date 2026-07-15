package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand

/**
 * `CMD_TEMP_BASAL_CANCEL_REQ` (0x2D) → `CMD_TEMP_BASAL_CANCEL_RES` (0x8D). Cancels a running temp-basal
 * program. Criticality: high (a delivery-control command).
 *
 * Request wire format (1 byte): `[0] 0x2D` — legacy `manipulateCancelTempBasalInfusionProgram()` builds
 * `createMessage(byteArrayOf(CMD_TEMP_BASAL_CANCEL_REQ))` with no arguments, so the opcode is the whole frame.
 *
 * Response (0x8D): `[0] 0x8D, [1] resultCode` → [SimpleResultResponse]. The legacy parser also fabricates a
 * `timestamp` (`System.currentTimeMillis()`) and echoes `cmd` (= the opcode); neither is on the wire, so this
 * port drops them and keeps only the result code (`resultCode` 0 = SUCCESS).
 */
class TempBasalCancelCommand : BleCommand<SimpleResultResponse> {

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray = byteArrayOf(requestOpcode)

    override fun decode(responsePayload: ByteArray): SimpleResultResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return SimpleResultResponse(responsePayload.u(1))
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x2D
        const val RESPONSE_OPCODE: Byte = 0x8D.toByte()
        private const val MIN_RESPONSE_LENGTH = 2
    }
}
