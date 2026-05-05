package app.aaps.core.interfaces.scenes

/**
 * Bridge between the active-scene runtime state and the NS settings sync layer.
 *
 * Master reads [activeSceneSnapshot] to publish; client calls [applyActiveScene] when a
 * snapshot arrives over the wire. Domain-typed (no nssdk dependency) — the consumer
 * (`RunningConfigurationImpl`) maps to/from the NS wire DTO.
 *
 * The snapshot carries the scene definition id, activation timing, and the **NS
 * identifiers** of records the scene created. Local Room IDs and master-only revert
 * state (`priorSmb`) are deliberately omitted. NS ids may be null while a record is
 * still uploading.
 */
interface ActiveSceneSync {

    /** Current active scene as a wire-shaped snapshot, or null when no scene is active. */
    fun activeSceneSnapshot(): ActiveSceneSnapshot?

    /**
     * Apply a snapshot received from the master.
     *
     * Passing null clears the local active-scene state. Unknown `sceneId` is treated as
     * "clear" rather than throwing — the scene definition may not have synced yet, and we
     * prefer no banner over a stale one. NS-id → local-Room-id resolution happens
     * asynchronously (suspend lookups internal to the implementer); chips populate when
     * the underlying records are present.
     */
    fun applyActiveScene(snapshot: ActiveSceneSnapshot?)
}

data class ActiveSceneSnapshot(
    val sceneId: String,
    val activatedAt: Long,
    val durationMs: Long,
    val ttNsId: String? = null,
    val psNsId: String? = null,
    val rmNsId: String? = null,
    val teNsId: String? = null
)
