package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand
import app.aaps.pump.carelevo.ble.BleResponse

/**
 * `CMD_EXTEND_BOLUS_CANCEL_REQ` (0x29) → `CMD_EXTEND_BOLUS_CANCEL_RES` (0x89). Cancels a running
 * extended-bolus infusion program. High criticality (delivery-affecting). Single-response [BleCommand].
 *
 * Request wire format (1 byte): `[0] 0x29` — legacy `manipulateCancelExtendBolusInfusionProgram` sends
 * `createMessage(byteArrayOf(CMD_EXTEND_BOLUS_CANCEL_REQ))` with no arguments.
 *
 * Response wire format (4 bytes, matches `CarelevoProtocolExtendBolusInfusionCancelParserImpl`):
 * ```
 * [0]     0x89           opcode
 * [1]     resultCode     0 = SUCCESS in the legacy Result taxonomy
 * [2..3]  infusedAmount  = [2] + [3]/100.0   (U already delivered before the cancel)
 * ```
 * The legacy parser also fabricates a `timestamp = System.currentTimeMillis()` and echoes `command`
 * (= the opcode). Neither is on the wire, so — matching the other ported commands — this decoder omits
 * both; the wall-clock timestamp belongs to the state-apply layer, not the wire decode.
 */
class ExtendBolusCancelCommand : BleCommand<ExtendBolusCancelResponse> {

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray = byteArrayOf(requestOpcode)

    override fun decode(responsePayload: ByteArray): ExtendBolusCancelResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return ExtendBolusCancelResponse(
            resultCode = responsePayload.u(1),
            infusedAmount = responsePayload.u(2) + responsePayload.u(3) / CENTI
        )
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x29
        const val RESPONSE_OPCODE: Byte = 0x89.toByte()

        private const val MIN_RESPONSE_LENGTH = 4
        private const val CENTI = 100.0
    }
}

/** Decoded response from [ExtendBolusCancelCommand]: pump [resultCode] (0 = SUCCESS) + [infusedAmount] (U). */
data class ExtendBolusCancelResponse(
    val resultCode: Int,
    val infusedAmount: Double
) : BleResponse
