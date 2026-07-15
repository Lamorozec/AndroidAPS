package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleResponse
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.math.roundToInt

/**
 * Shared wire encode/decode helpers for the CareLevo [app.aaps.pump.carelevo.ble.BleCommand]s, mirroring
 * the legacy `CarelevoBt*Transformer`/`CarelevoProtocol*Parser` conventions so ports stay byte-for-byte.
 *
 * (The three earliest commands — MacAddress/ImmediateBolus/InfusionInfo/PatchInfo — predate this file and
 * inline the same logic; they can adopt these helpers when next touched.)
 */

/** Unsigned byte at [index] as Int — matches every legacy parser's `.toUByte().toInt()`. */
internal fun ByteArray.u(index: Int): Int = this[index].toUByte().toInt()

/**
 * Encode a unit value as 2 bytes `[intPart, centiPart]`, HALF_UP to 2 dp — mirrors
 * `CarelevoDoubleToByteTransformerImpl` exactly (`intValue.toByte()`, `((rounded-int)*100).roundToInt()`).
 * Range validation stays at the command (the transformer's min/max belongs to the specific op).
 */
internal fun encodeUnitCenti(value: Double): ByteArray {
    val rounded = BigDecimal(value).setScale(2, RoundingMode.HALF_UP).toDouble()
    val whole = rounded.toInt()
    val centi = ((rounded - whole) * 100).roundToInt()
    return byteArrayOf(whole.toByte(), centi.toByte())
}

/** Fail fast unless [payload] starts with [opcode] and is at least [minLength] bytes. */
internal fun requireResponseFrame(payload: ByteArray, opcode: Byte, minLength: Int) {
    require(payload.isNotEmpty() && payload[0] == opcode) {
        "expected opcode 0x${"%02X".format(opcode)}, got " +
            (payload.getOrNull(0)?.let { "0x${"%02X".format(it)}" } ?: "empty")
    }
    require(payload.size >= minLength) { "response too short: ${payload.size} < $minLength" }
}

/**
 * Response carrying only the pump result code (`data[1]`) — the common 2-byte `cmd,result` reply used by
 * most write acknowledgements (buzz-mode, cancels, basal-program writes, …). `resultCode` 0 = SUCCESS in
 * the legacy `Result` taxonomy; consumers map it.
 */
data class SimpleResultResponse(val resultCode: Int) : BleResponse
