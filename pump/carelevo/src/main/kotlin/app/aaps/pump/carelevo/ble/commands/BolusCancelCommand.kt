package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand
import app.aaps.pump.carelevo.ble.BleResponse

/**
 * `CMD_BOLUS_CANCEL_REQ` (0x2C) → `CMD_BOLUS_CANCEL_RES` (0x8C). Cancels the running immediate bolus.
 * Criticality: high (delivery command).
 *
 * Request wire format (1 byte): `[0] 0x2C`. Legacy `manipulateCancelImmeBolusInfusionProgram` sends
 * only the opcode — no arguments.
 *
 * Response wire format (4 bytes, matches `CarelevoProtocolBolusInfusionCancelParserImpl`):
 * ```
 * [0]      0x8C            opcode
 * [1]      resultCode
 * [2..3]   infusedAmount   = [2] + [3]/100.0   (U delivered before the cancel took effect)
 * ```
 * **Fabricated field:** the legacy parser also sets `insulinRemains = 0.0`, a constant that is *not*
 * present on the wire. It is dropped here — [BolusCancelResponse] exposes only [resultCode] and
 * [infusedAmount], which are the fields consumers use.
 */
class BolusCancelCommand : BleCommand<BolusCancelResponse> {

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray = byteArrayOf(requestOpcode)

    override fun decode(responsePayload: ByteArray): BolusCancelResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return BolusCancelResponse(
            resultCode = responsePayload.u(1),
            infusedAmount = responsePayload.u(2) + responsePayload.u(3) / CENTI
        )
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x2C
        const val RESPONSE_OPCODE: Byte = 0x8C.toByte()

        private const val MIN_RESPONSE_LENGTH = 4
        private const val CENTI = 100.0
    }
}

/**
 * Decoded response from [BolusCancelCommand]: pump [resultCode] (0 = SUCCESS) and the [infusedAmount]
 * (U) delivered before the cancel took effect. The legacy `insulinRemains = 0.0` constant is fabricated
 * (not on the wire) and intentionally omitted.
 */
data class BolusCancelResponse(
    val resultCode: Int,
    val infusedAmount: Double
) : BleResponse
