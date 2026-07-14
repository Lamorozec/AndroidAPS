package app.aaps.pump.carelevo.coordinator

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.pump.carelevo.ble.core.CarelevoBleController
import app.aaps.pump.carelevo.ble.core.Connect
import app.aaps.pump.carelevo.ble.core.Disconnect
import app.aaps.pump.carelevo.ble.core.DiscoveryService
import app.aaps.pump.carelevo.ble.core.EnableNotifications
import app.aaps.pump.carelevo.ble.data.CommandResult
import app.aaps.pump.carelevo.ble.data.isAbnormalBondingFailed
import app.aaps.pump.carelevo.ble.data.isConnected
import app.aaps.pump.carelevo.ble.data.isDiscoverCleared
import app.aaps.pump.carelevo.ble.data.isReInitialized
import app.aaps.pump.carelevo.ble.data.shouldBeConnected
import app.aaps.pump.carelevo.ble.data.shouldBeDiscovered
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.model.PatchState
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoRequestPatchInfusionInfoUseCase
import io.reactivex.rxjava3.disposables.CompositeDisposable
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.jvm.optionals.getOrNull

@Singleton
class CarelevoConnectionCoordinator @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val aapsSchedulers: AapsSchedulers,
    private val carelevoPatch: CarelevoPatch,
    private val bleController: CarelevoBleController,
    private val requestPatchInfusionInfoUseCase: CarelevoRequestPatchInfusionInfoUseCase
) {

    private var reconnectDisposable = CompositeDisposable()
    private val reconnecting = AtomicBoolean(false)

    fun onStop() {
        aapsLogger.debug(LTag.PUMPCOMM, "onStop.clearReconnectDisposable")
        stopReconnection()
    }

    fun isInitialized(): Boolean {
        // Activation-based, NOT connection-based (matches Omnipod Dash / Medtrum). The CommandQueue
        // idle-disconnects between commands, so "initialized" must stay true while the link is down —
        // otherwise the loop's applyTBRRequest / applySMBRequest gate aborts with "pump not initialized"
        // during the normal resting state, before a command can be queued to reconnect. True once the
        // patch is paired and its operational status has been read at least once.
        val patchInfo = carelevoPatch.patchInfo.value?.getOrNull() ?: return false
        return patchInfo.mode != null ||
            patchInfo.runningMinutes != null ||
            patchInfo.pumpState != null
    }

    fun isConnected(): Boolean {
        // No patch yet → report connected so the queue's connect-loop doesn't spin dialing a missing device.
        carelevoPatch.patchInfo.value?.getOrNull()?.address ?: return true
        // FULLY-ready link (bonded + service discovered + notifications enabled), NOT just ACL-connected,
        // so the QueueWorker never executes a command mid-reconnect against a half-open GATT.
        return carelevoPatch.btState.value?.getOrNull()?.isConnected() == true
    }

    fun connect(reason: String, txUuid: UUID, onLastDataUpdated: () -> Unit) {
        aapsLogger.debug(LTag.PUMPCOMM, "connect.start reason=$reason")

        val patchState = carelevoPatch.resolvePatchState()
        aapsLogger.debug(LTag.PUMPCOMM, "connect.state reason=$reason patchState=$patchState")

        if (reason == "Connection needed" && patchState == PatchState.NotConnectedBooted) {
            onLastDataUpdated()
            startReconnection(txUuid)
        }
    }

    fun disconnect(reason: String) {
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        aapsLogger.debug(LTag.PUMPCOMM, "disconnect.start reason=$reason patchState=$patchState")
        // Actually drop the BLE link when the queue is done (matches other patch pumps): cancel any
        // in-flight reconnection and close the GATT. The next queued command reconnects via connect().
        stopReconnection()
        val address = carelevoPatch.getPatchInfoAddress()?.uppercase() ?: return
        try {
            val result = bleController.execute(Disconnect(address))
                .timeout(3, TimeUnit.SECONDS)
                .blockingGet()
            aapsLogger.debug(LTag.PUMPCOMM, "disconnect.result reason=$reason result=$result")
        } catch (e: Throwable) {
            aapsLogger.error(LTag.PUMPCOMM, "disconnect.error reason=$reason", e)
        }
    }

    fun stopConnecting() {
        aapsLogger.debug(LTag.PUMPCOMM, "stopConnecting.called")
    }

    fun refreshPumpStatus(
        onLastDataUpdated: () -> Unit
    ) {
        if (!carelevoPatch.isBluetoothEnabled()) return
        if (!carelevoPatch.isCarelevoConnected()) return

        // Block until the read completes (or times out). The suspend CommandReadStatus reads
        // lastDataTime and reports the status read immediately after getPumpStatus() returns, so a
        // fire-and-forget subscribe would let the queue treat a stale/failed read as complete.
        try {
            val response = requestPatchInfusionInfoUseCase.execute()
                .subscribeOn(aapsSchedulers.io)
                .timeout(3000L, TimeUnit.MILLISECONDS)
                .blockingGet()
            when (response) {
                is ResponseResult.Success -> {
                    onLastDataUpdated()
                    aapsLogger.debug(LTag.PUMPCOMM, "getPumpStatus.success")
                }

                is ResponseResult.Error   -> {
                    aapsLogger.debug(LTag.PUMPCOMM, "getPumpStatus.responseError error=${response.e}")
                }

                else                      -> {
                    aapsLogger.debug(LTag.PUMPCOMM, "getPumpStatus.failure")
                }
            }
        } catch (e: Throwable) {
            aapsLogger.error(LTag.PUMPCOMM, "getPumpStatus.error", e)
        }
    }

    fun startReconnection(txUuid: UUID) {
        // Single-flight: the QueueWorker calls connect() every ~1s while waiting for isConnected(); don't
        // restart an in-flight reconnection (and don't let a mid-reconnect btState flicker reset it).
        if (!reconnecting.compareAndSet(false, true)) {
            aapsLogger.debug(LTag.PUMPCOMM, "reconnect.skip already in progress")
            return
        }
        reconnectDisposable.clear()
        aapsLogger.debug(LTag.PUMPCOMM, "reconnect.start")

        if (!carelevoPatch.isBluetoothEnabled()) {
            aapsLogger.debug(LTag.PUMPCOMM, "reconnect.skip reason=bluetoothDisabled")
            reconnecting.set(false)
            return
        }

        val address = carelevoPatch.patchInfo.value?.getOrNull()?.address?.uppercase() ?: run {
            reconnecting.set(false)
            return
        }
        aapsLogger.debug(LTag.PUMPCOMM, "reconnect.target address=$address")

        reconnectDisposable.add(
            bleController.execute(Connect(address))
                .subscribeOn(aapsSchedulers.io)
                .observeOn(aapsSchedulers.io)
                .subscribe(
                    { result ->
                        when (result) {
                            is CommandResult.Success -> {
                                aapsLogger.debug(LTag.PUMPCOMM, "reconnect.connect.success")
                            }

                            else                     -> {
                                aapsLogger.error(LTag.PUMPCOMM, "reconnect.connect.failure result=$result")
                                stopReconnection()
                            }
                        }
                    },
                    { e ->
                        aapsLogger.error(LTag.PUMPCOMM, "reconnect.connect.error error=$e")
                        stopReconnection()
                    }
                )
        )

        reconnectDisposable.add(
            carelevoPatch.btState
                .subscribeOn(aapsSchedulers.io)
                .observeOn(aapsSchedulers.io)
                .distinctUntilChanged()
                .timeout(10, TimeUnit.SECONDS)
                .subscribe(
                    { btState ->
                        btState.getOrNull()?.let { state ->
                            when {
                                state.shouldBeConnected()   -> {
                                    aapsLogger.debug(LTag.PUMPCOMM, "reconnect.state connected")

                                    reconnectDisposable.add(
                                        bleController.execute(DiscoveryService(address))
                                            .subscribeOn(aapsSchedulers.io)
                                            .observeOn(aapsSchedulers.io)
                                            .subscribe { result ->
                                                if (result !is CommandResult.Success) {
                                                    aapsLogger.error(LTag.PUMPCOMM, "reconnect.discovery.failure result=$result")
                                                    stopReconnection()
                                                }
                                            }
                                    )
                                }

                                state.shouldBeDiscovered()  -> {
                                    aapsLogger.debug(LTag.PUMPCOMM, "reconnect.state discovered")
                                    reconnectDisposable.add(
                                        bleController.execute(EnableNotifications(address, txUuid))
                                            .subscribeOn(aapsSchedulers.io)
                                            .observeOn(aapsSchedulers.io)
                                            .subscribe { result ->
                                                aapsLogger.debug(LTag.PUMPCOMM, "reconnect.enableNotifications result=$result")
                                                if (result !is CommandResult.Success) {
                                                    stopReconnection()
                                                } else {
                                                    aapsLogger.debug(LTag.PUMPCOMM, "reconnect.finished")
                                                    stopReconnection()
                                                }
                                            }
                                    )
                                }

                                state.isDiscoverCleared() ||
                                    state.isAbnormalBondingFailed() ||
                                    state.isReInitialized() -> {
                                    aapsLogger.error(LTag.PUMPCOMM, "reconnect.abnormalState state=$state")
                                    stopReconnection()
                                }
                            }
                        }
                    },
                    { e ->
                        aapsLogger.error(LTag.PUMPCOMM, "reconnect.observe.error error=$e")
                        stopReconnection()
                    }
                )
        )
    }

    private fun stopReconnection() {
        aapsLogger.debug(LTag.PUMPCOMM, "reconnect.stop")
        reconnectDisposable.clear()
        reconnecting.set(false)
    }
}
