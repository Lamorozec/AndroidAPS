package app.aaps.pump.carelevo.common

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.pump.carelevo.coordinator.CarelevoAlarmClearCoordinator
import app.aaps.pump.carelevo.domain.model.alarm.CarelevoAlarmInfo
import app.aaps.pump.carelevo.domain.type.AlarmCause
import app.aaps.pump.carelevo.domain.type.AlarmType
import app.aaps.pump.carelevo.domain.usecase.alarm.CarelevoAlarmInfoUseCase
import app.aaps.pump.carelevo.presentation.model.AlarmEvent
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class CarelevoAlarmActionHandler @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val aapsSchedulers: AapsSchedulers,
    private val alarmUseCase: CarelevoAlarmInfoUseCase,
    private val alarmClearCoordinator: CarelevoAlarmClearCoordinator
) {

    // App-lifetime scope (this is a @Singleton): drives the suspend queue-routed alarm-clear ops.
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val _alarmQueue = MutableStateFlow<List<CarelevoAlarmInfo>>(emptyList())
    val alarmQueue = _alarmQueue.asStateFlow()

    private val _alarmQueueEmptyEvent = MutableSharedFlow<Unit>(
        replay = 0,
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )
    val alarmQueueEmptyEvent = _alarmQueueEmptyEvent.asSharedFlow()

    var alarmInfo: CarelevoAlarmInfo? = null

    private val compositeDisposable = CompositeDisposable()

    fun observeAlarms() =
        alarmUseCase.observeAlarms()
            .map { it.orElse(emptyList()) }

    fun getAlarmsOnce(includeUnacknowledged: Boolean = true): Single<List<CarelevoAlarmInfo>> =
        alarmUseCase.getAlarmsOnce(includeUnacknowledged)
            .map { it.orElse(emptyList()) }

    fun triggerEvent(event: AlarmEvent) {
        when (event) {
            is AlarmEvent.ClearAlarm -> startAlarmClearProcess(event.info)
            else                     -> Unit
        }
    }

    private fun startAlarmClearProcess(info: CarelevoAlarmInfo) {
        alarmInfo = info
        val alarmType = info.alarmType
        val alarmCause = info.cause

        aapsLogger.debug(LTag.PUMPCOMM, "startAlarmClearProcess alarmType=$alarmType, alarmCause=$alarmCause")

        // The BLE ops now go through the CommandQueue (it reconnects a resting pump first), so there is no
        // isPatchConnected() pre-gate; the old "disconnected" fallback becomes the on-failure path.
        when (alarmCause) {
            AlarmCause.ALARM_ALERT_RESUME_INSULIN_DELIVERY_TIMEOUT -> {
                scope.launch {
                    // Resume only if the alarm was actually cleared on the patch — do not resume delivery
                    // into an unresolved alarm condition (consistent with the in-app screen).
                    if (alarmClearCoordinator.clearAlarmOnPatch(info)) {
                        acknowledgeAndRemoveAlarm(info.alarmId)
                        alarmClearCoordinator.resumeInfusion()
                    }
                }
            }

            AlarmCause.ALARM_ALERT_OUT_OF_INSULIN,
            AlarmCause.ALARM_ALERT_PATCH_EXPIRED_PHASE_1,
            AlarmCause.ALARM_ALERT_PATCH_EXPIRED_PHASE_2,
            AlarmCause.ALARM_ALERT_APP_NO_USE,
            AlarmCause.ALARM_ALERT_PATCH_APPLICATION_INCOMPLETE,
            AlarmCause.ALARM_ALERT_LOW_BATTERY,
            AlarmCause.ALARM_ALERT_INVALID_TEMPERATURE,
            AlarmCause.ALARM_NOTICE_LOW_INSULIN,
            AlarmCause.ALARM_NOTICE_PATCH_EXPIRED,
            AlarmCause.ALARM_NOTICE_ATTACH_PATCH_CHECK             -> {
                scope.launch {
                    if (alarmClearCoordinator.clearAlarmOnPatch(info)) acknowledgeAndRemoveAlarm(info.alarmId)
                    else startAlarmAlertAbnormalClearProcess(info, alarmCause)
                }
            }

            AlarmCause.ALARM_ALERT_BLE_NOT_CONNECTED               -> {
                startAlarmAlertAbnormalClearProcess(info, alarmCause)
            }

            AlarmCause.ALARM_NOTICE_BG_CHECK,
            AlarmCause.ALARM_NOTICE_TIME_ZONE_CHANGED,
            AlarmCause.ALARM_NOTICE_LGS_START,
            AlarmCause.ALARM_NOTICE_LGS_FINISHED_DISCONNECTED_PATCH_OR_CGM,
            AlarmCause.ALARM_NOTICE_LGS_FINISHED_PAUSE_LGS,
            AlarmCause.ALARM_NOTICE_LGS_FINISHED_TIME_OVER,
            AlarmCause.ALARM_NOTICE_LGS_FINISHED_OFF_LGS,
            AlarmCause.ALARM_NOTICE_LGS_FINISHED_HIGH_BG,
            AlarmCause.ALARM_NOTICE_LGS_FINISHED_UNKNOWN,
            AlarmCause.ALARM_NOTICE_LGS_NOT_WORKING                -> {
                //startAlarmUpdateProcess()
            }

            AlarmCause.ALARM_UNKNOWN                               -> {
                if (alarmType == AlarmType.WARNING) {
                    if (alarmClearCoordinator.isPatchReachable()) {
                        // Reachable: tell the patch to discard (via the queue), then abandon it locally.
                        scope.launch {
                            alarmClearCoordinator.discardOnAlarm(info)
                            startAlarmClearPatchForceQuitProcess()
                        }
                    } else {
                        // Unreachable/faulty patch: abandon locally NOW — do not wait on the queue's
                        // connect-loop (up to ~119s) while a critical warning keeps sounding.
                        startAlarmClearPatchForceQuitProcess()
                    }
                }
            }

            else                                                   -> Unit
        }
    }

    private fun startAlarmAlertAbnormalClearProcess(info: CarelevoAlarmInfo, alarmCause: AlarmCause) {
        compositeDisposable += alarmUseCase.acknowledgeAlarm(info.alarmId)
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .subscribe(
                {
                    when (alarmCause) {
                        AlarmCause.ALARM_ALERT_BLUETOOTH_OFF -> {

                        }

                        else                                 -> acknowledgeAndRemoveAlarm(info.alarmId)
                    }

                }, { error ->

                })

    }

    private fun startAlarmClearPatchForceQuitProcess() {
        alarmClearCoordinator.forceQuitTeardown { clearAllAlarms() }
    }

    fun acknowledgeAndRemoveAlarm(alarmId: String) {
        _alarmQueue.value = alarmQueue.value.toMutableList().apply {
            removeAll { it.alarmId == alarmId }
        }
        if (alarmQueue.value.isEmpty()) {
            //_alarmQueueEmptyEvent.emit(Unit)
        }
    }

    fun clearAllAlarms() {
        compositeDisposable += alarmUseCase.clearAlarms()
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .subscribe(
                {
                    _alarmQueue.value = emptyList()
                    val ok = _alarmQueueEmptyEvent.tryEmit(Unit)
                    aapsLogger.debug(LTag.PUMPCOMM, "clearAllAlarms emitEmptyEvent=$ok")
                },
                { e ->
                    aapsLogger.error(LTag.PUMPCOMM, "clearAllAlarms.error error=$e")
                })
    }
}
