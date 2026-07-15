package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand

/**
 * `CMD_PATCH_DISCARD_REQ` (0x36) → `CMD_PATCH_DISCARD_RES` (0x96). Discards (deactivates) the patch.
 * Medium-criticality write with no arguments — legacy `manipulateDiscardPatch` sends only the opcode byte.
 *
 * Request wire format (1 byte): `[0] 0x36`.
 * Response (0x96): `[0] 0x96, [1] resultCode` → [SimpleResultResponse] (0 = SUCCESS in the legacy `Result`
 * taxonomy; the legacy `CarelevoProtocolPatchDiscardParserImpl` reads `data[1]` as `result` and fabricates a
 * `timestamp`/`cmd` that are model bookkeeping, not on the wire).
 */
class PatchDiscardCommand : BleCommand<SimpleResultResponse> {

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray = byteArrayOf(requestOpcode)

    override fun decode(responsePayload: ByteArray): SimpleResultResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return SimpleResultResponse(responsePayload.u(1))
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x36
        const val RESPONSE_OPCODE: Byte = 0x96.toByte()
        private const val MIN_RESPONSE_LENGTH = 2
    }
}
