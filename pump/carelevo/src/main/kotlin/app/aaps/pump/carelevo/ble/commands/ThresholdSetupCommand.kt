package app.aaps.pump.carelevo.ble.commands

import app.aaps.pump.carelevo.ble.BleCommand

/**
 * `CMD_THRESHOLD_SETUP_REQ` (0x1B) → `CMD_THRESHOLD_SETUP_RES` (0x7B). Activation bundle that sets all
 * patch thresholds at once (legacy `setThreshold`).
 *
 * Request wire format (7 bytes): `[0] 0x1B, [1] insulinRemainsThreshold, [2] expiryThreshold,
 * [3..4] maxBasalSpeed = [int,centi], [5..6] maxBolusDose = [int,centi], [7] buzzFlag`.
 *
 * **Quirks reproduced faithfully:**
 * - `insulinRemainsThreshold` is validated 10..300 by the legacy `IntegerToByte(10,300)` but written as a
 *   single byte, so values > 255 wrap (e.g. 300 → 0x2C = 44). Preserved here.
 * - `buzzFlag` is `BooleanToByte(!buzzUse)` → `buzzUse=true → 0x01`, `buzzUse=false → 0x00` (same double
 *   inversion as [BuzzModeCommand]).
 *
 * Response (0x7B): `[0] 0x7B, [1] resultCode` → [SimpleResultResponse].
 */
class ThresholdSetupCommand(
    private val insulinRemainsThreshold: Int,
    private val expiryThreshold: Int,
    private val maxBasalSpeed: Double,
    private val maxBolusDose: Double,
    private val buzzUse: Boolean
) : BleCommand<SimpleResultResponse> {

    init {
        require(insulinRemainsThreshold in INS_REMAIN_RANGE) { "insulinRemainsThreshold out of range $INS_REMAIN_RANGE" }
        require(expiryThreshold in EXPIRY_RANGE) { "expiryThreshold out of range $EXPIRY_RANGE" }
        require(maxBasalSpeed in MAX_BASAL_SPEED_RANGE) { "maxBasalSpeed out of range $MAX_BASAL_SPEED_RANGE" }
        require(maxBolusDose in MAX_BOLUS_DOSE_RANGE) { "maxBolusDose out of range $MAX_BOLUS_DOSE_RANGE" }
    }

    override val requestOpcode: Byte = REQUEST_OPCODE
    override val expectedResponseOpcode: Byte = RESPONSE_OPCODE

    override fun encode(): ByteArray =
        byteArrayOf(requestOpcode, insulinRemainsThreshold.toByte(), expiryThreshold.toByte()) +
            encodeUnitCenti(maxBasalSpeed) +
            encodeUnitCenti(maxBolusDose) +
            byteArrayOf(if (buzzUse) BUZZ_ON else BUZZ_OFF)

    override fun decode(responsePayload: ByteArray): SimpleResultResponse {
        requireResponseFrame(responsePayload, RESPONSE_OPCODE, MIN_RESPONSE_LENGTH)
        return SimpleResultResponse(responsePayload.u(1))
    }

    companion object {

        const val REQUEST_OPCODE: Byte = 0x1B
        const val RESPONSE_OPCODE: Byte = 0x7B
        private val INS_REMAIN_RANGE = 10..300
        private val EXPIRY_RANGE = 24..167
        private val MAX_BASAL_SPEED_RANGE = 0.05..15.0
        private val MAX_BOLUS_DOSE_RANGE = 0.05..25.0
        private const val BUZZ_ON: Byte = 0x01 // buzzUse=true  → BooleanToByte(!true=false) = 0x01
        private const val BUZZ_OFF: Byte = 0x00 // buzzUse=false → BooleanToByte(!false=true) = 0x00
        private const val MIN_RESPONSE_LENGTH = 2
    }
}
