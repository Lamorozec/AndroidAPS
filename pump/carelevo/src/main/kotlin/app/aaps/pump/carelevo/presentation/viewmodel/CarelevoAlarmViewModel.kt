package app.aaps.pump.carelevo.presentation.viewmodel

import android.os.Handler
import android.os.HandlerThread
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import app.aaps.core.data.time.T
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.queue.CommandQueue
import app.aaps.core.interfaces.resources.ResourceHelper
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.interfaces.ui.UiInteraction
import app.aaps.pump.carelevo.R
import app.aaps.pump.carelevo.common.MutableEventFlow
import app.aaps.pump.carelevo.common.asEventFlow
import app.aaps.pump.carelevo.coordinator.CarelevoAlarmClearCoordinator
import app.aaps.pump.carelevo.domain.model.alarm.CarelevoAlarmInfo
import app.aaps.pump.carelevo.domain.type.AlarmCause
import app.aaps.pump.carelevo.domain.usecase.alarm.CarelevoAlarmInfoUseCase
import app.aaps.pump.carelevo.ext.transformNotificationStringResources
import app.aaps.pump.carelevo.presentation.model.AlarmEvent
import dagger.hilt.android.lifecycle.HiltViewModel
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import javax.inject.Inject

@HiltViewModel
class CarelevoAlarmViewModel @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val uiInteraction: UiInteraction,
    private val rh: ResourceHelper,
    private val aapsSchedulers: AapsSchedulers,
    private val commandQueue: CommandQueue,
    private val alarmUseCase: CarelevoAlarmInfoUseCase,
    private val alarmClearCoordinator: CarelevoAlarmClearCoordinator
) : ViewModel() {

    private val _alarmQueue = MutableStateFlow<List<CarelevoAlarmInfo>>(emptyList())
    val alarmQueue = _alarmQueue.asStateFlow()

    private val _alarmQueueEmptyEvent = MutableSharedFlow<Unit>(
        replay = 0,
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )
    val alarmQueueEmptyEvent = _alarmQueueEmptyEvent.asSharedFlow()

    private val _event = MutableEventFlow<AlarmEvent>()
    val event = _event.asEventFlow()

    var alarmInfo: CarelevoAlarmInfo? = null

    private val compositeDisposable = CompositeDisposable()

    private val sound = app.aaps.core.ui.R.raw.error
    private var handler = Handler(HandlerThread(this::class.simpleName + "Handler").also { it.start() }.looper)

    private fun startAlarm(reason: String) {
        if (sound == 0) return
        val title = rh.gs(R.string.carelevo)
        val status = alarmInfo?.let { rh.gs(it.cause.transformNotificationStringResources().first) } ?: title
        aapsLogger.debug(LTag.PUMPCOMM, "startAlarm reason=$reason status=$status")
        uiInteraction.runAlarm(status, title, sound)
    }

    private fun stopAlarm(reason: String) {
        uiInteraction.stopAlarm(reason)
    }

    fun triggerEvent(event: AlarmEvent) {
        when (event) {
            is AlarmEvent.ClearAlarm -> {
                stopAlarm("Confirm Click")
                startAlarmClearProcess(event.info)
            }

            is AlarmEvent.Mute       -> stopAlarm("Mute Click")

            is AlarmEvent.Mute5min   -> {
                stopAlarm("Mute5min Click")
                handler.postDelayed({ startAlarm("post") }, T.mins(5).msecs())
            }

            is AlarmEvent.StartAlarm -> startAlarm("start")
            else                     -> Unit
        }
    }

    fun loadUnacknowledgedAlarms() {
        compositeDisposable += alarmUseCase.getAlarmsOnce()
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.main)
            .subscribe(
                { optionalList ->
                    val alarms = optionalList.orElse(emptyList())
                        .filter { !it.isAcknowledged }
                        .sortedWith(
                            compareBy<CarelevoAlarmInfo> { it.alarmType.code }
                                .thenBy { it.createdAt }
                        )

                    _alarmQueue.value = alarms.also {
                        if (it.isEmpty()) {
                            _alarmQueueEmptyEvent.tryEmit(Unit)
                        }
                    }

                }, { e ->
                    aapsLogger.error(LTag.PUMPCOMM, "getAlarmsOnce.error error=$e")
                })
    }

    // The patch-facing ops now go through the CommandQueue (it reconnects a resting pump first), so there
    // is no isPatchConnected() pre-gate; the old "disconnected" branch becomes the on-failure path.
    private fun startAlarmClearProcess(info: CarelevoAlarmInfo) {
        alarmInfo = info
        val alarmCause = info.cause

        aapsLogger.debug(LTag.PUMPCOMM, "startAlarmClearProcess alarmType=${info.alarmType}, alarmCause=$alarmCause")

        when (alarmCause) {
            AlarmCause.ALARM_WARNING_LOW_INSULIN,
            AlarmCause.ALARM_WARNING_PATCH_EXPIRED_PHASE_1,
            AlarmCause.ALARM_WARNING_INVALID_TEMPERATURE,
            AlarmCause.ALARM_WARNING_BLE_NOT_CONNECTED,
            AlarmCause.ALARM_WARNING_INCOMPLETE_PATCH_SETTING,
            AlarmCause.ALARM_WARNING_SELF_DIAGNOSIS_FAILED,
            AlarmCause.ALARM_WARNING_PATCH_EXPIRED,
            AlarmCause.ALARM_WARNING_PATCH_ERROR,
            AlarmCause.ALARM_WARNING_PUMP_CLOGGED,
            AlarmCause.ALARM_WARNING_NEEDLE_INSERTION_ERROR,
            AlarmCause.ALARM_WARNING_LOW_BATTERY -> {
                if (alarmClearCoordinator.isPatchReachable()) {
                    // Reachable: tell the patch to discard (via the queue), then abandon it locally.
                    viewModelScope.launch {
                        alarmClearCoordinator.discardOnAlarm(info)
                        startAlarmClearPatchForceQuitProcess()
                    }
                } else {
                    // Unreachable/faulty patch: abandon locally NOW — do not wait on the queue's
                    // connect-loop (up to ~119s) while a critical warning keeps sounding.
                    startAlarmClearPatchForceQuitProcess()
                }
            }

            AlarmCause.ALARM_WARNING_NOT_USED_APP_AUTO_OFF -> {
                viewModelScope.launch {
                    if (alarmClearCoordinator.clearAlarmOnPatch(info)) {
                        acknowledgeAndRemoveAlarm(info.alarmId)
                        alarmClearCoordinator.resumeInfusion()
                    } else {
                        triggerEvent(AlarmEvent.ShowToastMessage(R.string.alarm_feat_msg_check_patch_connect))
                    }
                }
            }

            AlarmCause.ALARM_ALERT_BLUETOOTH_OFF -> {
                startAlarmAlertAbnormalClearProcess(info, alarmCause)
            }

            else -> Unit
        }
    }

    private fun startAlarmAlertAbnormalClearProcess(info: CarelevoAlarmInfo, alarmCause: AlarmCause) {
        viewModelScope.launch {
            compositeDisposable += alarmUseCase.acknowledgeAlarm(info.alarmId)
                .subscribeOn(aapsSchedulers.io)
                .observeOn(aapsSchedulers.main)
                .subscribe(
                    {
                        when (alarmCause) {
                            AlarmCause.ALARM_ALERT_BLUETOOTH_OFF -> {
                                startReconnect(info.alarmId)
                                viewModelScope.launch {
                                    _event.emit(AlarmEvent.RequestBluetoothEnable)
                                }
                            }

                            else                                 -> acknowledgeAndRemoveAlarm(info.alarmId)
                        }

                    }, { error ->

                    })
        }
    }

    private fun startAlarmClearPatchForceQuitProcess() {
        alarmClearCoordinator.forceQuitTeardown { clearAllAlarms() }
    }

    fun acknowledgeAndRemoveAlarm(alarmId: String) {
        _alarmQueue.value = alarmQueue.value.toMutableList().apply {
            removeAll { it.alarmId == alarmId }
        }
        if (alarmQueue.value.isEmpty()) {
            viewModelScope.launch {
                _alarmQueueEmptyEvent.emit(Unit)
            }
        }
    }

    private fun startReconnect(alarmId: String) {
        // Reconnect + refresh through the queue (managed connect-before-execute) rather than a raw BLE
        // connect that would fight the queue-owned lifecycle. Ack the alarm once the link is confirmed.
        viewModelScope.launch {
            if (commandQueue.readStatus("Bluetooth re-enabled after alarm").success) {
                aapsLogger.debug(LTag.PUMPCOMM, "reconnect readStatus success")
                acknowledgeAndRemoveAlarm(alarmId)
            } else {
                aapsLogger.debug(LTag.PUMPCOMM, "reconnect readStatus failed")
            }
        }
    }

    fun clearAllAlarms() {
        compositeDisposable += alarmUseCase.clearAlarms()
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.main)
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
