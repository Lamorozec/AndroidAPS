package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand

/**
 * `CMD_NEEDLE_INSERT_ACK` (0x19) → `CMD_CANNULA_INSERTION_RPT_ACK_RES` (0x7A). Confirms the cannula
 * insertion result back to the patch.
 *
 * Request wire format (2 bytes): `[0] 0x19, [1] flag`, where `flag = BooleanToByte(isSuccess)` →
 * `isSuccess=true → 0x00`, `isSuccess=false → 0x01`.
 *
 * **Behavioral note:** the legacy `CarelevoPatchCannulaInsertionConfirmUseCase` is WRITE-ONLY — it sends
 * 0x19 and only checks the write-ack, never awaiting 0x7A (even though a 0x7A parser exists). This command
 * models it as a proper single-response (`0x19 → 0x7A`), which is stricter and likely correct; **confirm
 * on hardware that the pump actually sends 0x7A** before relying on the awaited result. Response (0x7A):
 * `[0] 0x7A, [1] resultCode` → [SimpleResultResponse].
 */
class NeedleAckCommand(
    private val isSuccess: Boolean
) : BleCommand<SimpleResultResponse> {

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray = byteArrayOf(requestOpcode, if (isSuccess) FLAG_SUCCESS else FLAG_FAILURE)

    override fun decode(responsePayload: ByteArray): SimpleResultResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return SimpleResultResponse(responsePayload.u(1))
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x19
        const val RESPONSE_OPCODE: Byte = 0x7A
        private const val FLAG_SUCCESS: Byte = 0x00 // BooleanToByte(true)
        private const val FLAG_FAILURE: Byte = 0x01 // BooleanToByte(false)
        private const val MIN_RESPONSE_LENGTH = 2
    }
}
