package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand

/**
 * `CMD_APP_AUTH_IND` (0x4B) → `CMD_APP_AUTH_ACK` (0xBB). The activation app-auth handshake: the app sends its
 * auth [key] and the patch acknowledges with a result code.
 *
 * Request wire format (2 bytes): `[0] 0x4B, [1] key` — [key] is written as a **raw byte**, with no transformer
 * (legacy `setAppAuth` does an inline `byteArrayOf(key)`), so no scaling/inversion is applied.
 *
 * **Opcode-collision quirk:** 0x4B is shared by both `CMD_APP_AUTH_IND` and `CMD_APP_AUTH_RPT` in the legacy
 * enum, and the request opcode itself is 0x4B — but the live reply comes back on `CMD_APP_AUTH_ACK` = **0xBB**
 * (not 0x4B, and not the neighbouring `CMD_APP_AUTH_KEY_ACK` = 0xBA), which is why [expectedResponseOpcode] is
 * 0xBB rather than echoing the request opcode.
 *
 * Response (0xBB): `[0] 0xBB, [1] result` → [SimpleResultResponse] (0 = SUCCESS). The legacy
 * `CarelevoProtocolAppAuthAckParserImpl` also emits a `timestamp` (fabricated via `System.currentTimeMillis()`,
 * not on the wire) and `cmd` (just the echoed opcode `data[0]`); both are dropped here — only the wire [result]
 * byte is carried.
 */
class AppAuthCommand(
    private val key: Int
) : BleCommand<SimpleResultResponse> {

    init {
        require(key in KEY_RANGE) { "key $key out of range $KEY_RANGE" }
    }

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray = byteArrayOf(requestOpcode, key.toByte())

    override fun decode(responsePayload: ByteArray): SimpleResultResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return SimpleResultResponse(responsePayload.u(1))
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x4B
        const val RESPONSE_OPCODE: Byte = 0xBB.toByte()

        // key is written as a raw byte (no legacy transformer); accept the full unsigned-byte range.
        private val KEY_RANGE = 0..255
        private const val MIN_RESPONSE_LENGTH = 2
    }
}
