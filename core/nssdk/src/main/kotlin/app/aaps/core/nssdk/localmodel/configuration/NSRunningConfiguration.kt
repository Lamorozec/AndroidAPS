package app.aaps.core.nssdk.localmodel.configuration

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

/**
 * Typed payload of the NS `settings/aaps` doc's `runningConfig` block.
 *
 * Carries the master AAPS install's currently selected plugin set and their owned
 * preference values, applied by clients via [app.aaps.core.nssdk.interfaces.RunningConfiguration].
 *
 * Lives in the settings collection, not in devicestatus — though the field shape
 * was originally introduced for the (now-removed) devicestatus piggyback channel.
 */
@Serializable
data class NSRunningConfiguration(
    val pump: String? = null,
    val version: String? = null,
    val insulin: Int? = null,
    val aps: String? = null,
    val sensitivity: Int? = null,
    val smoothing: String? = null,
    val insulinConfiguration: JsonObject? = null,
    val apsConfiguration: JsonObject? = null,
    val sensitivityConfiguration: JsonObject? = null,
    val overviewConfiguration: JsonObject? = null,
    val safetyConfiguration: JsonObject? = null,
    val quickWizardConfiguration: JsonObject? = null,
    val scenesConfiguration: JsonObject? = null,
    val automationConfiguration: JsonObject? = null,
    val activeScene: NSActiveScene? = null
)

/**
 * Wire view of the currently running scene published by the master.
 *
 * Carries the scene definition id, activation timing, and the **NS identifiers** of the
 * records the scene created (TempTarget / ProfileSwitch / RunningMode / TherapyEvent).
 * Local Room IDs and the master-only `priorSmb` flag are deliberately not shipped.
 *
 * NS IDs may be null transiently while the corresponding record is still uploading on the
 * master; they fill in on subsequent publishes. Clients compute `expired` from
 * `now > activatedAt + durationMs`; master clears the entire `activeScene` block when the
 * scene is dismissed.
 */
@Serializable
data class NSActiveScene(
    val sceneId: String,
    val activatedAt: Long,
    val durationMs: Long,
    val ttNsId: String? = null,
    val psNsId: String? = null,
    val rmNsId: String? = null,
    val teNsId: String? = null
)
