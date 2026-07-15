package app.aaps.pump.carelevo.command

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.pump.PumpEnactResult
import app.aaps.core.interfaces.queue.CustomCommand
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.pump.carelevo.ble.CarelevoBleSession
import app.aaps.pump.carelevo.ble.commands.BuzzModeCommand
import app.aaps.pump.carelevo.ble.commands.InfusionThresholdCommand
import app.aaps.pump.carelevo.ble.commands.NoticeThresholdCommand
import app.aaps.pump.carelevo.ble.BleCommand
import app.aaps.pump.carelevo.ble.commands.AdditionalPrimingCommand
import app.aaps.pump.carelevo.ble.commands.AlarmClearCommand
import app.aaps.pump.carelevo.ble.commands.PatchDiscardCommand
import app.aaps.pump.carelevo.ble.commands.PumpResumeCommand
import app.aaps.pump.carelevo.ble.commands.PumpStopCommand
import app.aaps.pump.carelevo.ble.commands.NeedleStatusCommand
import app.aaps.pump.carelevo.ble.commands.SafetyCheckCommand
import app.aaps.pump.carelevo.ble.commands.SetTimeCommand
import app.aaps.pump.carelevo.ble.commands.SimpleResultResponse
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.keys.CarelevoBooleanPreferenceKey
import app.aaps.pump.carelevo.coordinator.CarelevoConnectionCoordinator
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.bt.SafetyCheckResult
import app.aaps.pump.carelevo.domain.model.bt.SafetyCheckResultModel
import app.aaps.pump.carelevo.domain.model.patch.NeedleCheckSuccess
import app.aaps.pump.carelevo.domain.type.SafetyProgress
import app.aaps.pump.carelevo.domain.usecase.alarm.AlarmClearPatchDiscardUseCase
import app.aaps.pump.carelevo.domain.usecase.alarm.AlarmClearRequestUseCase
import app.aaps.pump.carelevo.domain.usecase.alarm.model.AlarmClearUseCaseRequest
import app.aaps.pump.carelevo.domain.usecase.basal.CarelevoSetBasalProgramUseCase
import app.aaps.pump.carelevo.domain.usecase.basal.model.SetBasalProgramRequestModel
import app.aaps.pump.carelevo.domain.usecase.infusion.CarelevoPumpResumeUseCase
import app.aaps.pump.carelevo.domain.usecase.infusion.CarelevoPumpStopUseCase
import app.aaps.pump.carelevo.domain.usecase.infusion.model.CarelevoPumpStopRequestModel
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchAdditionalPrimingUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchDiscardUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchNeedleInsertionCheckUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchSafetyCheckUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchTimeZoneUpdateUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.model.CarelevoPatchTimeZoneRequestModel
import app.aaps.pump.carelevo.domain.usecase.userSetting.CarelevoPatchBuzzModifyUseCase
import app.aaps.pump.carelevo.domain.usecase.userSetting.CarelevoPatchExpiredThresholdModifyUseCase
import app.aaps.pump.carelevo.domain.usecase.userSetting.CarelevoUpdateLowInsulinNoticeAmountUseCase
import app.aaps.pump.carelevo.domain.usecase.userSetting.CarelevoUpdateMaxBolusDoseUseCase
import app.aaps.pump.carelevo.domain.usecase.userSetting.model.CarelevoPatchBuzzRequestModel
import app.aaps.pump.carelevo.domain.usecase.userSetting.model.CarelevoPatchExpiredThresholdModifyRequestModel
import app.aaps.pump.carelevo.domain.usecase.userSetting.model.CarelevoUserSettingInfoRequestModel
import io.reactivex.rxjava3.core.Single
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.runBlocking
import org.joda.time.DateTime
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton
import kotlin.jvm.optionals.getOrNull

/**
 * Runs activation operations that are routed through the AAPS [app.aaps.core.interfaces.queue.CommandQueue]
 * as custom commands. The plugin's `executeCustomCommand` delegates here; it is invoked on the queue
 * worker thread AFTER the queue has guaranteed the pump is connected (connect-before-execute + managed
 * reconnect). Each op therefore runs BLOCKING and returns a [PumpEnactResult].
 *
 * The safety check streams progress (`Progress` → `Success`/`Error`); since a custom command returns a
 * single result, that progress is republished on [safetyProgress] so the wizard can drive its countdown.
 */
@Singleton
class CarelevoActivationExecutor @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val aapsSchedulers: AapsSchedulers,
    private val pumpEnactResultProvider: Provider<PumpEnactResult>,
    private val carelevoPatch: CarelevoPatch,
    private val safetyCheckUseCase: CarelevoPatchSafetyCheckUseCase,
    private val needleCheckUseCase: CarelevoPatchNeedleInsertionCheckUseCase,
    private val setBasalUseCase: CarelevoSetBasalProgramUseCase,
    private val additionalPrimingUseCase: CarelevoPatchAdditionalPrimingUseCase,
    private val discardUseCase: CarelevoPatchDiscardUseCase,
    private val pumpStopUseCase: CarelevoPumpStopUseCase,
    private val pumpResumeUseCase: CarelevoPumpResumeUseCase,
    private val timeZoneUpdateUseCase: CarelevoPatchTimeZoneUpdateUseCase,
    private val updateMaxBolusDoseUseCase: CarelevoUpdateMaxBolusDoseUseCase,
    private val updateLowInsulinNoticeAmountUseCase: CarelevoUpdateLowInsulinNoticeAmountUseCase,
    private val expiredThresholdModifyUseCase: CarelevoPatchExpiredThresholdModifyUseCase,
    private val buzzModifyUseCase: CarelevoPatchBuzzModifyUseCase,
    private val alarmClearRequestUseCase: AlarmClearRequestUseCase,
    private val alarmClearPatchDiscardUseCase: AlarmClearPatchDiscardUseCase,
    // Phase-2 new BLE stack (flag-gated). See `_docs/carelevo-new-ble-stack.md`.
    private val preferences: Preferences,
    private val bleSession: CarelevoBleSession,
    private val connectionCoordinator: CarelevoConnectionCoordinator
) {

    private companion object {

        // Upper bounds so a hung use case can never block the queue worker thread forever. These
        // mirror the timeouts the ViewModels used before the ops were routed through the queue.
        private const val NEEDLE_CHECK_TIMEOUT_SEC = 30L
        private const val SET_BASAL_TIMEOUT_SEC = 15L
        private const val ADDITIONAL_PRIMING_TIMEOUT_SEC = 60L
        private const val DISCARD_TIMEOUT_SEC = 30L
        private const val PUMP_STOP_TIMEOUT_SEC = 15L
        private const val PUMP_RESUME_TIMEOUT_SEC = 15L
        private const val TIMEZONE_UPDATE_TIMEOUT_SEC = 15L
        private const val SETTINGS_TIMEOUT_SEC = 15L

        // The alarm use cases already wait up to 10s internally for the patch's clear/discard event.
        private const val ALARM_TIMEOUT_SEC = 20L

        // Settle window after dropping the legacy GATT before the new transport re-dials the same
        // device (mirrors CarelevoPumpPlugin). Phase-2 new-stack path only.
        private const val NEW_BLE_SETTLE_MS = 1000L
        private const val RESULT_SUCCESS = 0 // pump result byte 0 = SUCCESS / BY_REQ (legacy Result/StopPumpResult taxonomy)
        private const val STOP_RESUME_SUB_ID = 0 // legacy StopPumpRequest/ResumePumpRequest use subId/causeId = 0
        private const val RESUME_MODE = 1 // legacy ResumePumpRequest(mode = 1)
        private const val TIMEZONE_SUB_ID = 1 // legacy SetTimeRequest(subId = 1) for the timezone/DST update path
        private const val TIMEZONE_AID_MODE = 0 // legacy SetTimeRequest(aidMode = 0)
        private const val NEEDLE_CHECK_NEW_STACK_TIMEOUT_MS = 90_000L // physical cannula insertion may take a while
        private const val SAFETY_PROGRESS_HEADROOM_SEC = 30 // legacy: Progress timeout = firstFrame.durationSeconds + 30
    }

    private val _safetyProgress = MutableSharedFlow<SafetyProgress>(extraBufferCapacity = 16)
    val safetyProgress: SharedFlow<SafetyProgress> = _safetyProgress.asSharedFlow()

    // Run a use-case Single on IO — NOT inline on the queue worker thread — with a hard timeout. The patch
    // use cases are `Single.fromCallable { ...blockingFirst()... }` with NO subscribeOn, so without moving
    // the subscription off the worker the inline block would ignore .timeout() and hang the single
    // CommandQueue thread forever whenever a patch confirmation event is lost / the link drops mid-wait.
    private fun <T : Any> awaitOnIo(single: Single<T>, timeoutSec: Long): T =
        single.subscribeOn(aapsSchedulers.io).timeout(timeoutSec, TimeUnit.SECONDS).blockingGet()

    fun execute(command: CustomCommand): PumpEnactResult? = when (command) {
        is CmdSafetyCheck            -> runSafetyCheck()
        is CmdNeedleCheck            -> runNeedleCheck()
        is CmdSetBasal               -> runSetBasal()
        is CmdAdditionalPriming      -> runAdditionalPriming()
        is CmdDiscard                -> runDiscard()
        is CmdPumpStop               -> runPumpStop(command)
        is CmdPumpResume             -> runPumpResume()
        is CmdTimeZoneUpdate         -> runTimeZoneUpdate(command)
        is CmdUpdateMaxBolus         -> runUpdateMaxBolus(command)
        is CmdUpdateLowInsulinNotice -> runUpdateLowInsulinNotice(command)
        is CmdUpdateExpiredThreshold -> runUpdateExpiredThreshold(command)
        is CmdUpdateBuzzer           -> runUpdateBuzzer(command)
        is CmdAlarmClear             -> runAlarmClear(command)
        is CmdAlarmClearPatchDiscard -> runAlarmClearPatchDiscard(command)
        else                         -> null
    }

    private fun runSafetyCheck(): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runSafetyCheckViaNewStack()
        }
        val result = pumpEnactResultProvider.get()
        var success = false
        try {
            // Blocks the queue worker thread until the check reaches a terminal state; the use case
            // has its own internal timeouts so it always completes. Progress is mirrored to the UI.
            safetyCheckUseCase.execute()
                .blockingSubscribe(
                    { progress ->
                        _safetyProgress.tryEmit(progress)
                        if (progress is SafetyProgress.Success) success = true
                    },
                    { error ->
                        aapsLogger.error(LTag.PUMPCOMM, "CmdSafetyCheck error", error)
                        _safetyProgress.tryEmit(SafetyProgress.Error(error))
                    }
                )
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdSafetyCheck exception", e)
            _safetyProgress.tryEmit(SafetyProgress.Error(e))
            return result.success(false).enacted(false).comment(e.message ?: "error")
        }
        return result.success(success).enacted(success)
    }

    private fun runNeedleCheck(): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runNeedleCheckViaNewStack()
        }
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(needleCheckUseCase.execute(), NEEDLE_CHECK_TIMEOUT_SEC)
            val success = response is ResponseResult.Success && response.data is NeedleCheckSuccess
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdNeedleCheck exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runSetBasal(): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runSetBasalViaNewStack()
        }
        val result = pumpEnactResultProvider.get()
        val profile = carelevoPatch.profile.value?.getOrNull()
            ?: return result.success(false).enacted(false).comment("profile not set")
        return try {
            val response = awaitOnIo(setBasalUseCase.execute(SetBasalProgramRequestModel(profile)), SET_BASAL_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdSetBasal exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 initial basal program over the new stack (flag-gated, **delivery-critical**). Mirrors
     * [runSetBasal]: build the 3-program plan with the use case's exact mapping, send all three
     * [app.aaps.pump.carelevo.ble.commands.BasalProgramCommand]s over ONE session (via
     * [CarelevoBleSession.runBasalProgram]), and only on all-success reuse the use case's `mode=1` +
     * infusion-info persist. Activation-only op (validated on a physical patch change).
     */
    private fun runSetBasalViaNewStack(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val profile = carelevoPatch.profile.value?.getOrNull()
            ?: return result.success(false).enacted(false).comment("profile not set")
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val plan = setBasalUseCase.buildBasalProgramPlan(profile)
            val programmed = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runBasalProgram(address, plan.programs)
            }
            val persisted = if (programmed) setBasalUseCase.persistBasalProgram(plan.segments) else false
            aapsLogger.info(LTag.PUMPCOMM, "newBle.setBasal programmed=$programmed persisted=$persisted")
            val ok = programmed && persisted
            result.success(ok).enacted(ok)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.setBasal FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runAdditionalPriming(): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runSingleWriteViaNewStack("additionalPriming") { AdditionalPrimingCommand() }
        }
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(additionalPrimingUseCase.execute(), ADDITIONAL_PRIMING_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdAdditionalPriming exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * BLE discard (tell the patch to stop delivering) + teardown. Routed through the queue so a
     * dropped link is reconnected before the stop is sent, and so the teardown runs HERE on the queue
     * worker thread: the worker is busy inside this command while it runs, so it cannot fire a
     * reconnect at the patch we are discarding (no zombie-reconnect race), and the ~300ms BLE teardown
     * stays off the Main thread. The DB-only force-discard is left in the ViewModels as the fallback
     * for when the queue cannot reach the patch at all.
     */
    private fun runDiscard(): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runDiscardViaNewStack()
        }
        val result = pumpEnactResultProvider.get()
        // The STOP alone decides success: a teardown failure only leaves a stale bond and must NOT
        // flip an already-successful stop to failure. Teardown runs on this (queue worker) thread so
        // the worker cannot fire a reconnect during it, and is single-flight inside discardTeardown().
        val stopped = try {
            awaitOnIo(discardUseCase.execute(), DISCARD_TIMEOUT_SEC) is ResponseResult.Success
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdDiscard stop exception", e)
            false
        }
        if (stopped) {
            carelevoPatch.discardTeardown()
        }
        return result.success(stopped).enacted(stopped)
    }

    private fun runPumpStop(command: CmdPumpStop): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runPumpStopViaNewStack(command.durationMin)
        }
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(pumpStopUseCase.execute(CarelevoPumpStopRequestModel(durationMin = command.durationMin)), PUMP_STOP_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdPumpStop exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runPumpResume(): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runPumpResumeViaNewStack()
        }
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(pumpResumeUseCase.execute(), PUMP_RESUME_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdPumpResume exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 pump-stop over the new [app.aaps.pump.carelevo.ble.BleClient] stack (flag-gated). Same
     * connection-ownership handling as the buzzer/status paths: drop the legacy link + settle, then write
     * [PumpStopCommand] on the new transport's own session. On success (`resultCode == 0`) persist the
     * suspended state through the SAME seam as legacy (`pumpStopUseCase.persistStopped`). SAFETY: this
     * suspends delivery; the pump can be resumed via [runPumpResumeViaNewStack] (or the legacy path with
     * the flag off). See `_docs/carelevo-new-ble-stack.md`.
     */
    private fun runPumpStopViaNewStack(durationMin: Int): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, PumpStopCommand(durationMinutes = durationMin, subId = STOP_RESUME_SUB_ID))
            }
            if (response.resultCode != RESULT_SUCCESS) {
                aapsLogger.error(LTag.PUMPCOMM, "newBle.pumpStop rejected result=${response.resultCode}")
                return result.success(false).enacted(false).comment("stop result ${response.resultCode}")
            }
            val persisted = pumpStopUseCase.persistStopped(durationMin)
            aapsLogger.info(LTag.PUMPCOMM, "newBle.pumpStop OK durationMin=$durationMin persisted=$persisted")
            result.success(persisted).enacted(persisted)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.pumpStop FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runPumpResumeViaNewStack(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, PumpResumeCommand(mode = RESUME_MODE, subId = STOP_RESUME_SUB_ID))
            }
            if (response.resultCode != RESULT_SUCCESS) { // 0 = BY_REQ
                aapsLogger.error(LTag.PUMPCOMM, "newBle.pumpResume rejected result=${response.resultCode}")
                return result.success(false).enacted(false).comment("resume result ${response.resultCode}")
            }
            val persisted = pumpResumeUseCase.persistResumed()
            aapsLogger.info(LTag.PUMPCOMM, "newBle.pumpResume OK persisted=$persisted")
            result.success(persisted).enacted(persisted)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.pumpResume FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runTimeZoneUpdate(command: CmdTimeZoneUpdate): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runSingleWriteViaNewStack("timeZoneUpdate") {
                SetTimeCommand(subId = TIMEZONE_SUB_ID, volume = command.insulinAmount, aidMode = TIMEZONE_AID_MODE, dateTime = DateTime.now())
            }
        }
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(timeZoneUpdateUseCase.execute(CarelevoPatchTimeZoneRequestModel(insulinAmount = command.insulinAmount)), TIMEZONE_UPDATE_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdTimeZoneUpdate exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    // patchState is read HERE (queue worker thread, after the reconnect) so the setting is pushed with the
    // patch's current state, not whatever it was when the preference-change enqueued the command.
    private fun runUpdateMaxBolus(command: CmdUpdateMaxBolus): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runUpdateMaxBolusViaNewStack(command.maxBolusDose)
        }
        val result = pumpEnactResultProvider.get()
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        return try {
            val response = awaitOnIo(
                updateMaxBolusDoseUseCase.execute(
                    CarelevoUserSettingInfoRequestModel(patchState = patchState, maxBolusDose = command.maxBolusDose)
                ),
                SETTINGS_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdUpdateMaxBolus exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runUpdateLowInsulinNotice(command: CmdUpdateLowInsulinNotice): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runUpdateLowInsulinNoticeViaNewStack(command.hours)
        }
        val result = pumpEnactResultProvider.get()
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        return try {
            val response = awaitOnIo(
                updateLowInsulinNoticeAmountUseCase.execute(
                    CarelevoUserSettingInfoRequestModel(patchState = patchState, lowInsulinNoticeAmount = command.hours)
                ),
                SETTINGS_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdUpdateLowInsulinNotice exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runUpdateExpiredThreshold(command: CmdUpdateExpiredThreshold): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runUpdateExpiredThresholdViaNewStack(command.hours)
        }
        val result = pumpEnactResultProvider.get()
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        return try {
            val response = awaitOnIo(
                expiredThresholdModifyUseCase.execute(
                    CarelevoPatchExpiredThresholdModifyRequestModel(patchState = patchState, patchExpiredThreshold = command.hours)
                ),
                SETTINGS_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdUpdateExpiredThreshold exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runUpdateBuzzer(command: CmdUpdateBuzzer): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runBuzzerViaNewStack(command.on)
        }
        val result = pumpEnactResultProvider.get()
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        return try {
            val response = awaitOnIo(
                buzzModifyUseCase.execute(
                    CarelevoPatchBuzzRequestModel(patchState = patchState, settingsAlarmBuzz = command.on)
                ),
                SETTINGS_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdUpdateBuzzer exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 first WRITE over the new [app.aaps.pump.carelevo.ble.BleClient] stack (flag-gated,
     * engineering-only): set the patch buzzer via [BuzzModeCommand] (0x18 → 0x78). Chosen as the first
     * write to device-validate because it is non-therapy and audibly verifiable (the patch buzzes). Same
     * connection-ownership handling as the plugin's status read: drop the legacy link + settle, then run
     * the command on the new transport's own session (this runs on the queue worker thread, blocked
     * inside the command, so the worker cannot concurrently re-dial legacy). See `_docs/carelevo-new-ble-stack.md`.
     */
    private fun runBuzzerViaNewStack(on: Boolean): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, BuzzModeCommand(use = on))
            }
            val success = response.resultCode == RESULT_SUCCESS
            aapsLogger.info(LTag.PUMPCOMM, "newBle.buzzer OK on=$on result=${response.resultCode}")
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.buzzer FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 max-bolus setting over the new [app.aaps.pump.carelevo.ble.BleClient] stack (flag-gated).
     * Preserves the legacy semantics: never push a threshold mid-bolus (persist locally + defer via
     * `needMaxBolusDoseSyncPatch`); otherwise drop the legacy link, write [InfusionThresholdCommand]
     * (max-volume) on the new transport, then persist with `synced` = the patch confirmed. On any failure
     * the value is still persisted with the sync flag set so the deferred-sync re-pushes on reconnect.
     */
    private fun runUpdateMaxBolusViaNewStack(value: Double): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        if (updateMaxBolusDoseUseCase.isBolusRunning()) {
            val persisted = updateMaxBolusDoseUseCase.persistMaxBolusDose(value, synced = false)
            aapsLogger.info(LTag.PUMPCOMM, "newBle.maxBolus bolus-running → deferred persisted=$persisted")
            return result.success(persisted).enacted(persisted)
        }
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, InfusionThresholdCommand(isMaxVolume = true, value = value))
            }
            val pushed = response.resultCode == RESULT_SUCCESS
            val persisted = updateMaxBolusDoseUseCase.persistMaxBolusDose(value, synced = pushed)
            aapsLogger.info(LTag.PUMPCOMM, "newBle.maxBolus OK value=$value result=${response.resultCode} persisted=$persisted")
            val success = pushed && persisted
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.maxBolus FAILED", e)
            updateMaxBolusDoseUseCase.persistMaxBolusDose(value, synced = false) // keep desired value + defer re-sync
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 low-insulin-notice setting over the new stack (flag-gated). The 0x75 response fabricates a
     * result of 0, so arrival = success (mirrors legacy). Persists via the extracted use-case method with
     * `synced` = arrived; on failure the value is persisted deferred for the next reconnect.
     */
    private fun runUpdateLowInsulinNoticeViaNewStack(hours: Int): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, NoticeThresholdCommand(thresholdType = NoticeThresholdCommand.TYPE_LOW_INSULIN, value = hours))
            }
            val pushed = response.resultCode == RESULT_SUCCESS
            val persisted = updateLowInsulinNoticeAmountUseCase.persistLowInsulinNoticeAmount(hours, synced = pushed)
            aapsLogger.info(LTag.PUMPCOMM, "newBle.lowInsulinNotice OK hours=$hours result=${response.resultCode} persisted=$persisted")
            val success = pushed && persisted
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.lowInsulinNotice FAILED", e)
            updateLowInsulinNoticeAmountUseCase.persistLowInsulinNoticeAmount(hours, synced = false)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 patch-expiry-reminder setting over the new stack (flag-gated). The legacy use case has NO
     * userSettingInfo persist for this op (the preference is the source of truth), so this is a pure BLE
     * write like the buzzer; arrival of the 0x75 frame (fabricated result 0) = success.
     */
    private fun runUpdateExpiredThresholdViaNewStack(hours: Int): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, NoticeThresholdCommand(thresholdType = NoticeThresholdCommand.TYPE_EXPIRY, value = hours))
            }
            val success = response.resultCode == RESULT_SUCCESS
            aapsLogger.info(LTag.PUMPCOMM, "newBle.expiryThreshold OK hours=$hours result=${response.resultCode}")
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.expiryThreshold FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Shared Phase-2 path for a pure single-response WRITE with no persistence (buzzer-style ops:
     * additional-priming, timezone, …): drop the legacy link + settle, run [command] on the new
     * transport's own session, success = `resultCode == 0`. Runs on the queue worker (blocked inside
     * the command) so the worker cannot re-dial legacy concurrently.
     */
    private fun runSingleWriteViaNewStack(label: String, command: () -> BleCommand<SimpleResultResponse>): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, command())
            }
            val success = response.resultCode == RESULT_SUCCESS
            aapsLogger.info(LTag.PUMPCOMM, "newBle.$label OK result=${response.resultCode}")
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.$label FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 discard over the new stack (flag-gated). Mirrors [runDiscard]: the STOP write alone
     * ([PatchDiscardCommand] 0x36) decides success; a teardown failure must not flip an already-stopped
     * patch to failure. Teardown runs here on the queue worker thread (no reconnect race).
     */
    private fun runDiscardViaNewStack(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        val stopped = try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, PatchDiscardCommand())
            }
            response.resultCode == RESULT_SUCCESS
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.discard stop exception", e)
            false
        }
        if (stopped) carelevoPatch.discardTeardown()
        aapsLogger.info(LTag.PUMPCOMM, "newBle.discard stopped=$stopped")
        return result.success(stopped).enacted(stopped)
    }

    /**
     * Phase-2 cannula-insertion (needle) check over the new stack (flag-gated). Long timeout — the pump
     * reports 0x79 only after the physical insertion. `resultCode == 0` = SUCCESS; the [checkNeedle] /
     * failure-count persist is reused from the use case. Activation-only op (validated on a patch change).
     */
    private fun runNeedleCheckViaNewStack(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, NeedleStatusCommand(), timeoutMs = NEEDLE_CHECK_NEW_STACK_TIMEOUT_MS)
            }
            val inserted = response.resultCode == RESULT_SUCCESS
            val persisted = needleCheckUseCase.persistNeedleResult(inserted)
            aapsLogger.info(LTag.PUMPCOMM, "newBle.needleCheck inserted=$inserted result=${response.resultCode} persisted=$persisted")
            val ok = inserted && persisted
            result.success(ok).enacted(ok)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.needleCheck FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 Safety Check over the new stack (flag-gated) — the first hardware use of `requestStream`.
     * Streams each 0x72 frame; progress frames (REP_REQUEST/REP_REQUEST1) drive the wizard countdown via
     * [_safetyProgress] (one Progress emit, matching legacy), the terminal SUCCESS frame persists
     * `checkSafety` + emits Success, any other terminal is an Error. Activation-only (validated on a patch change).
     */
    private fun runSafetyCheckViaNewStack(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        var success = false
        var progressEmitted = false
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSafetyCheck(address) { frame ->
                    when (frame.resultCode) {
                        SafetyCheckCommand.REP_REQUEST, SafetyCheckCommand.REP_REQUEST1 -> {
                            if (!progressEmitted) {
                                progressEmitted = true
                                _safetyProgress.tryEmit(SafetyProgress.Progress((frame.durationSeconds + SAFETY_PROGRESS_HEADROOM_SEC).toLong()))
                            }
                        }

                        SafetyCheckCommand.RESULT_SUCCESS                              -> {
                            if (safetyCheckUseCase.persistSafetyChecked()) {
                                success = true
                                _safetyProgress.tryEmit(SafetyProgress.Success(SafetyCheckResultModel(SafetyCheckResult.SUCCESS, frame.insulinVolume, frame.durationSeconds)))
                            } else {
                                _safetyProgress.tryEmit(SafetyProgress.Error(IllegalStateException("update patch info is failed")))
                            }
                        }

                        else                                                          ->
                            _safetyProgress.tryEmit(SafetyProgress.Error(IllegalStateException("safety check failed: result ${frame.resultCode}")))
                    }
                }
            }
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.safetyCheck FAILED", e)
            _safetyProgress.tryEmit(SafetyProgress.Error(e))
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 alarm clear over the new stack (flag-gated): map the cause to the wire alarm-type byte, send
     * [AlarmClearCommand] (0x47 → 0xA7), and on `resultCode == 0` reuse the use case's `markAcknowledged`
     * persist. Mirrors [runAlarmClear].
     */
    private fun runAlarmClearViaNewStack(command: CmdAlarmClear): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val alarmTypeByte = alarmClearRequestUseCase.commandAlarmType(command.alarmCause)
            val cause = command.alarmCause.code ?: 0
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, AlarmClearCommand(alarmTypeByte, cause))
            }
            val cleared = response.resultCode == RESULT_SUCCESS
            val persisted = if (cleared) alarmClearRequestUseCase.persistAlarmCleared(command.alarmId) else false
            aapsLogger.info(LTag.PUMPCOMM, "newBle.alarmClear cleared=$cleared result=${response.resultCode} persisted=$persisted")
            val ok = cleared && persisted
            result.success(ok).enacted(ok)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.alarmClear FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 alarm-triggered patch discard over the new stack (flag-gated): send [PatchDiscardCommand]
     * (0x36) then reuse the use case's DB cleanup (ack + reset sync flags + delete infusion/patch). Mirrors
     * [runAlarmClearPatchDiscard], which — unlike the plain [runDiscard] — does NOT unbond (no
     * `discardTeardown`); kept faithful to the legacy path.
     */
    private fun runAlarmClearPatchDiscardViaNewStack(command: CmdAlarmClearPatchDiscard): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, PatchDiscardCommand())
            }
            val discarded = response.resultCode == RESULT_SUCCESS
            val persisted = if (discarded) alarmClearPatchDiscardUseCase.persistAlarmDiscarded(command.alarmId) else false
            aapsLogger.info(LTag.PUMPCOMM, "newBle.alarmClearPatchDiscard discarded=$discarded result=${response.resultCode} persisted=$persisted")
            val ok = discarded && persisted
            result.success(ok).enacted(ok)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.alarmClearPatchDiscard FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runAlarmClear(command: CmdAlarmClear): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runAlarmClearViaNewStack(command)
        }
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(
                alarmClearRequestUseCase.execute(
                    AlarmClearUseCaseRequest(alarmId = command.alarmId, alarmType = command.alarmType, alarmCause = command.alarmCause)
                ),
                ALARM_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdAlarmClear exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runAlarmClearPatchDiscard(command: CmdAlarmClearPatchDiscard): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runAlarmClearPatchDiscardViaNewStack(command)
        }
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(
                alarmClearPatchDiscardUseCase.execute(
                    AlarmClearUseCaseRequest(alarmId = command.alarmId, alarmType = command.alarmType, alarmCause = command.alarmCause)
                ),
                ALARM_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdAlarmClearPatchDiscard exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }
}
