package app.aaps.pump.carelevo.coordinator

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.carelevo.common.CarelevoPatch
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.jvm.optionals.getOrNull

/**
 * The CommandQueue's connection border for a patch pump running on per-op
 * [app.aaps.pump.carelevo.ble.CarelevoBleSession] sessions: every operation opens, uses and closes its
 * OWN GATT session, so there is no resting link for the queue to manage — [isConnected] is always true
 * (matching Omnipod/Medtrum-style patch pumps) and [connect]/[disconnect] are no-ops kept only for the
 * `Pump` interface contract. [isInitialized] stays activation-based.
 */
@Singleton
class CarelevoConnectionCoordinator @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val carelevoPatch: CarelevoPatch
) {

    fun isInitialized(): Boolean {
        // Activation-based, NOT connection-based (matches Omnipod Dash / Medtrum): true once the
        // patch is paired and its operational status has been read at least once. Must stay true with
        // no link up, otherwise the loop's applyTBRRequest / applySMBRequest gate aborts with "pump
        // not initialized" during the normal resting state.
        val patchInfo = carelevoPatch.patchInfo.value?.getOrNull() ?: return false
        return patchInfo.mode != null ||
            patchInfo.runningMinutes != null ||
            patchInfo.pumpState != null
    }

    /** Always true: each op opens its own session, so the queue never has to wait on a link. */
    fun isConnected(): Boolean = true

    fun connect(reason: String) {
        aapsLogger.debug(LTag.PUMPCOMM, "connect.noop reason=$reason (per-op sessions)")
    }

    fun disconnect(reason: String) {
        aapsLogger.debug(LTag.PUMPCOMM, "disconnect.noop reason=$reason (per-op sessions)")
    }

    fun stopConnecting() {
        aapsLogger.debug(LTag.PUMPCOMM, "stopConnecting.called")
    }
}
