package app.aaps.core.interfaces.sync

import app.aaps.core.interfaces.nsclient.NSAlarm

/**
 * Plugin providing communication with Nightscout server
 */
interface NsClient : Sync {

    /**
     * NS URL
     */
    val address: String

    /**
     * Set plugin in paused state
     */
    fun pause(newState: Boolean)

    /**
     * Initiate new round of upload/download
     *
     * @param reason identification of caller
     */
    fun resend(reason: String)

    /**
     * Used data sync selector
     */
    val dataSyncSelector: DataSyncSelector

    /**
     * Version of NS server
     * @return Returns detected version of NS server
     */
    fun detectedNsVersion(): String?

    enum class Collection { ENTRIES, TREATMENTS, FOODS, PROFILE, SETTINGS }

    /**
     * First load downloads all data; next loads use srvModified for sync.
     *
     * @return true while inside the first load
     */
    fun isFirstLoad(collection: Collection): Boolean = true

    /**
     * Update newest loaded timestamp for entries collection (first load)
     * Update newest srvModified (sync loads)
     *
     * @param latestReceived timestamp
     *
     */
    fun updateLatestBgReceivedIfNewer(latestReceived: Long)

    /**
     * Update newest loaded timestamp for treatments collection (first load)
     * Update newest srvModified (sync loads)
     *
     * @param latestReceived timestamp
     *
     */
    fun updateLatestTreatmentReceivedIfNewer(latestReceived: Long)

    /**
     * Send alarm confirmation to NS
     *
     * @param originalAlarm alarm to be cleared
     * @param silenceTimeInMilliseconds silence alarm for specified duration
     */
    fun handleClearAlarm(originalAlarm: NSAlarm, silenceTimeInMilliseconds: Long)

    /**
     * Clear synchronization status
     *
     * Next synchronization will start from scratch
     */
    suspend fun resetToFullSync()
}