package app.aaps.pump.carelevo.presentation.viewmodel

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.queue.CommandQueue
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.interfaces.sharedPreferences.SP
import app.aaps.core.interfaces.pump.ble.ScannedDevice as TransportScannedDevice
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.pump.carelevo.ble.CarelevoBleSession
import app.aaps.pump.carelevo.ble.CarelevoBleSource
import app.aaps.pump.carelevo.ble.CarelevoBleTransport
import app.aaps.pump.carelevo.ble.core.CarelevoBleController
import app.aaps.pump.carelevo.ble.core.Connect
import app.aaps.pump.carelevo.ble.core.DiscoveryService
import app.aaps.pump.carelevo.ble.core.EnableNotifications
import app.aaps.pump.carelevo.ble.core.StartScan
import app.aaps.pump.carelevo.ble.core.StopScan
import app.aaps.pump.carelevo.ble.data.CommandResult
import app.aaps.pump.carelevo.ble.data.PeripheralScanResult
import app.aaps.pump.carelevo.ble.data.ScannedDevice
import app.aaps.pump.carelevo.ble.data.isAbnormalBondingFailed
import app.aaps.pump.carelevo.ble.data.isAbnormalFailed
import app.aaps.pump.carelevo.ble.data.isDiscoverCleared
import app.aaps.pump.carelevo.ble.data.isPairingFailed
import app.aaps.pump.carelevo.ble.data.isReInitialized
import app.aaps.pump.carelevo.ble.data.shouldBeConnected
import app.aaps.pump.carelevo.ble.data.shouldBeDiscovered
import app.aaps.pump.carelevo.ble.data.shouldBeNotificationEnabled
import app.aaps.pump.carelevo.command.CmdDiscard
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.MutableEventFlow
import app.aaps.pump.carelevo.common.asEventFlow
import app.aaps.pump.carelevo.common.keys.CarelevoBooleanPreferenceKey
import app.aaps.pump.carelevo.common.keys.CarelevoIntPreferenceKey
import app.aaps.pump.carelevo.common.model.Event
import app.aaps.pump.carelevo.common.model.PatchState
import app.aaps.pump.carelevo.common.model.State
import app.aaps.pump.carelevo.common.model.UiState
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoConnectNewPatchUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchForceDiscardUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.model.CarelevoConnectNewPatchRequestModel
import app.aaps.pump.carelevo.presentation.model.CarelevoConnectPrepareEvent
import dagger.hilt.android.lifecycle.HiltViewModel
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Named
import kotlin.jvm.optionals.getOrNull

@HiltViewModel
class CarelevoPatchConnectViewModel @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val aapsSchedulers: AapsSchedulers,
    private val carelevoPatch: CarelevoPatch,
    private val bleController: CarelevoBleController,
    private val commandQueue: CommandQueue,
    private val sp: SP,
    private val preferences: Preferences,
    private val bleSession: CarelevoBleSession,
    private val transport: CarelevoBleTransport,
    private val connectNewPatchUseCase: CarelevoConnectNewPatchUseCase,
    private val patchForceDiscardUseCase: CarelevoPatchForceDiscardUseCase,
    @Named("characterTx") private val txUuid: UUID
) : ViewModel() {

    private var _selectedDevice: ScannedDevice? = null
    val selectedDevice get() = _selectedDevice

    // Device found by the new-stack discovery scan (flag-on path); the legacy path keeps _selectedDevice.
    private var _selectedNewStackDevice: TransportScannedDevice? = null

    private var _isScanWorking = false
    val isScanWorking get() = _isScanWorking

    private val commandDelay = 300L

    private val _event = MutableEventFlow<Event>()
    val event = _event.asEventFlow()

    private val _uiState: MutableStateFlow<State> = MutableStateFlow(UiState.Idle)
    val uiState = _uiState.asStateFlow()

    private val compositeDisposable = CompositeDisposable()

    private val connectDisposable = CompositeDisposable()

    init {
        observeScannedDevice()
    }

    private fun setUiState(state: State) {
        _uiState.tryEmit(state)
    }

    fun triggerEvent(event: Event) {
        viewModelScope.launch {
            when (event) {
                is CarelevoConnectPrepareEvent -> generateEventType(event).run { _event.emit(this) }
            }
        }
    }

    private fun generateEventType(event: Event): Event {
        return when (event) {
            is CarelevoConnectPrepareEvent.ShowConnectDialog                 -> event
            is CarelevoConnectPrepareEvent.ShowMessageScanFailed             -> event
            is CarelevoConnectPrepareEvent.ShowMessageScanIsWorking          -> event
            is CarelevoConnectPrepareEvent.ShowMessageBluetoothNotEnabled    -> event
            is CarelevoConnectPrepareEvent.ShowMessageSelectedDeviceIseEmpty -> event
            is CarelevoConnectPrepareEvent.ConnectComplete                   -> event
            is CarelevoConnectPrepareEvent.ConnectFailed                     -> event
            is CarelevoConnectPrepareEvent.DiscardComplete                   -> event
            is CarelevoConnectPrepareEvent.DiscardFailed                     -> event
            is CarelevoConnectPrepareEvent.ShowMessageNotSetUserSettingInfo  -> event
            else                                                             -> CarelevoConnectPrepareEvent.NoAction
        }
    }

    fun observeScannedDevice() {
        compositeDisposable += CarelevoBleSource.scanDevices
            .observeOn(aapsSchedulers.io)
            .subscribeOn(aapsSchedulers.io)
            .subscribe {
                aapsLogger.debug(LTag.PUMPCOMM, "device : $it")
                if (it is PeripheralScanResult.Success) {
                    val result = it.value
                    if (result.isNotEmpty()) {
                        _selectedDevice = result[0]
                    }
                }
            }
    }

    fun startScan() {
        if (!bleController.isBluetoothEnabled()) {
            triggerEvent(CarelevoConnectPrepareEvent.ShowMessageBluetoothNotEnabled)
            return
        }
        if (isScanWorking) {
            triggerEvent(CarelevoConnectPrepareEvent.ShowMessageScanIsWorking)
            return
        }
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            startScanViaNewStack()
            return
        }

        setUiState(UiState.Loading)
        compositeDisposable += bleController.execute(StartScan())
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .subscribe { result ->
                _isScanWorking = true
                Thread.sleep(10000)
                setUiState(UiState.Idle)
                stopScan()
            }
    }

    /**
     * Phase-2 discovery scan over the new transport (flag-gated). `scanAddress = null` puts
     * `CarelevoBleTransportImpl`'s scanner in service-UUID discovery mode (built for this wizard); the
     * first advertising patch wins — the legacy scan likewise auto-picks its top result. Stops early on a
     * hit instead of always burning the full window.
     */
    private fun startScanViaNewStack() {
        setUiState(UiState.Loading)
        _isScanWorking = true
        viewModelScope.launch(Dispatchers.IO) {
            val found = try {
                transport.scanAddress = null
                withTimeoutOrNull(NEW_STACK_SCAN_TIMEOUT_MS) {
                    transport.scanner.scannedDevices
                        // Subscribe BEFORE starting the scan so an immediate advertisement cannot be missed.
                        .onSubscription { transport.scanner.startScan() }
                        .first()
                }
            } finally {
                transport.scanner.stopScan()
                _isScanWorking = false
            }
            aapsLogger.info(LTag.PUMPCOMM, "newBle.scan found=${found?.name} address=${found?.address}")
            _selectedNewStackDevice = found
            setUiState(UiState.Idle)
            if (found != null) {
                triggerEvent(CarelevoConnectPrepareEvent.ShowConnectDialog)
            } else {
                triggerEvent(CarelevoConnectPrepareEvent.ShowMessageScanFailed)
            }
        }
    }

    private fun stopScan() {
        compositeDisposable += bleController.execute(StopScan())
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .subscribe { result ->
                _isScanWorking = false
                if (selectedDevice != null) {
                    triggerEvent(CarelevoConnectPrepareEvent.ShowConnectDialog)
                } else {
                    triggerEvent(CarelevoConnectPrepareEvent.ShowMessageScanFailed)
                }
            }
    }

    fun startPatchDiscardProcess() {
        when (carelevoPatch.patchState.value?.getOrNull()) {
            is PatchState.NotConnectedNotBooting, null -> {
                triggerEvent(CarelevoConnectPrepareEvent.DiscardComplete)
            }

            else                                       -> {
                // Route the BLE stop through the queue (reconnect-before-execute); if the patch can't
                // be reached at all, fall back to the DB-only force-discard.
                setUiState(UiState.Loading)
                viewModelScope.launch {
                    val result = commandQueue.customCommand(CmdDiscard())
                    if (result.success) {
                        // unBond + releasePatch now run inside CmdDiscard on the queue thread
                        setUiState(UiState.Idle)
                        triggerEvent(CarelevoConnectPrepareEvent.DiscardComplete)
                    } else {
                        aapsLogger.error(LTag.PUMPCOMM, "discard failed, falling back to force-discard")
                        startPatchForceDiscard()
                    }
                }
            }
        }
    }

    private fun startPatchForceDiscard() {
        setUiState(UiState.Loading)
        compositeDisposable += patchForceDiscardUseCase.execute()
            .timeout(3000L, TimeUnit.MILLISECONDS)
            .observeOn(aapsSchedulers.io)
            .subscribeOn(aapsSchedulers.io)
            .doOnError {
                aapsLogger.debug(LTag.PUMPCOMM, "doOnError called : $it")
                setUiState(UiState.Idle)
                triggerEvent(CarelevoConnectPrepareEvent.DiscardFailed)
            }.subscribe { response ->
                when (response) {
                    is ResponseResult.Success -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "response success")
                        carelevoPatch.discardTeardown()
                        setUiState(UiState.Idle)
                        triggerEvent(CarelevoConnectPrepareEvent.DiscardComplete)
                    }

                    is ResponseResult.Error   -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "response error : ${response.e}")
                        setUiState(UiState.Idle)
                        triggerEvent(CarelevoConnectPrepareEvent.DiscardFailed)
                    }

                    else                      -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "[CarelevoConnectPrepareViewMode;::startPatchForceDiscard] response failed")
                        setUiState(UiState.Idle)
                        triggerEvent(CarelevoConnectPrepareEvent.DiscardFailed)
                    }
                }
            }
    }

    fun startConnect(inputInsulin: Int) {
        aapsLogger.debug(LTag.PUMPCOMM, "startConnectTest called")
        if (!bleController.isBluetoothEnabled()) {
            triggerEvent(CarelevoConnectPrepareEvent.ShowMessageBluetoothNotEnabled)
            return
        }
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            startConnectViaNewStack(inputInsulin)
            return
        }
        if (selectedDevice == null) {
            triggerEvent(CarelevoConnectPrepareEvent.ShowMessageSelectedDeviceIseEmpty)
            return
        }

        val address = selectedDevice?.device?.address ?: ""
        connectDisposable += Completable.fromAction {
            bleController.clearBond(address).also {
                aapsLogger.debug(LTag.PUMPCOMM, "bondRemoveResult : $it")
            }
        }
            .andThen(Completable.timer(commandDelay, TimeUnit.MILLISECONDS))
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .subscribe({
                       }, { e ->
                           aapsLogger.error(LTag.PUMPCOMM, "bond remove + delay error")
                           stopConnect()
                       })

        connectDisposable += carelevoPatch.btState
            .observeOn(aapsSchedulers.io)
            .subscribeOn(aapsSchedulers.io)
            .subscribe { btState ->
                setUiState(UiState.Loading)
                aapsLogger.debug(LTag.PUMPCOMM, "bt state : $btState")
                btState?.getOrNull()?.let { state ->
                    if (state.shouldBeConnected()) {
                        aapsLogger.debug(LTag.PUMPCOMM, "should be connected called")
                        Thread.sleep(commandDelay)
                        bleController.execute(DiscoveryService(address))
                            .blockingGet()
                            .takeIf { it !is CommandResult.Success }
                            ?.let { stopConnect() }
                    }

                    if (state.shouldBeDiscovered()) {
                        aapsLogger.debug(LTag.PUMPCOMM, "should be discovered called")
                        Thread.sleep(commandDelay)
                        bleController.execute(EnableNotifications(address, txUuid))
                            .blockingGet()
                            .takeIf { it !is CommandResult.Success }
                            ?.let { stopConnect() }
                    }

                    if (state.shouldBeNotificationEnabled()) {
                        aapsLogger.debug(LTag.PUMPCOMM, "should be notification enabled called")
                        Thread.sleep(commandDelay)
                        connectNewPatch(inputInsulin)
                    }
                    if (state.isDiscoverCleared()) {
                        aapsLogger.debug(LTag.PUMPCOMM, "is discover cleared called")
                        Thread.sleep(commandDelay)
                        bleController.clearGatt()
                        stopConnect()
                    }
                    if (state.isAbnormalFailed()) {
                        aapsLogger.debug(LTag.PUMPCOMM, "is abnormal failed called")
                        Thread.sleep(commandDelay)
                        bleController.clearGatt()
                        stopConnect()
                    }
                    if (state.isAbnormalBondingFailed()) {
                        aapsLogger.debug(LTag.PUMPCOMM, "is abnormal bonding failed called")
                        Thread.sleep(commandDelay)
                        bleController.clearGatt()
                        stopConnect()
                    }
                    if (state.isReInitialized()) {
                        aapsLogger.debug(LTag.PUMPCOMM, "is reinitialized called")
                        Thread.sleep(commandDelay)
                        bleController.clearGatt()
                        stopConnect()
                    }
                    if (state.isPairingFailed()) {
                        aapsLogger.debug(LTag.PUMPCOMM, "is pairing failed called")
                        Thread.sleep(commandDelay)
                        bleController.clearGatt()
                        stopConnect()
                    }
                }
            }

        connectDisposable += bleController.execute(Connect(address))
            .observeOn(aapsSchedulers.io)
            .subscribeOn(aapsSchedulers.io)
            .subscribe { result ->
                when (result) {
                    is CommandResult.Success -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "connect result success")
                    }

                    else                     -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "connect result failed")
                        stopConnect()
                    }
                }
            }
    }

    private fun stopConnect() {
        connectDisposable.clear()
        triggerEvent(CarelevoConnectPrepareEvent.ConnectFailed)
        setUiState(UiState.Idle)
    }

    /**
     * Phase-2 pairing over the new stack (flag-gated): clear any stale bond (legacy `clearBond` parity) →
     * one `CarelevoBleSession.runPairing` session (connect → bond → MAC/auth/set-time→patch-info/alarm/
     * threshold) → persist via the use case's extracted `persistNewPatch`. No btState observer needed —
     * the session owns its connect handshake and either returns or throws. After success the CommandQueue
     * (activation customCommands) takes over exactly as on the legacy path.
     */
    private fun startConnectViaNewStack(inputInsulin: Int) {
        val device = _selectedNewStackDevice
        if (device == null) {
            triggerEvent(CarelevoConnectPrepareEvent.ShowMessageSelectedDeviceIseEmpty)
            return
        }
        val userSettingInfo = carelevoPatch.userSettingInfo.value?.getOrNull()
        if (userSettingInfo == null) {
            triggerEvent(CarelevoConnectPrepareEvent.ShowMessageNotSetUserSettingInfo)
            return
        }
        val request = CarelevoConnectNewPatchRequestModel(
            volume = inputInsulin,
            expiry = sp.getInt(CarelevoIntPreferenceKey.CARELEVO_PATCH_EXPIRATION_REMINDER_HOURS.key, 116),
            remains = userSettingInfo.lowInsulinNoticeAmount!!,
            maxBasalSpeed = userSettingInfo.maxBasalSpeed!!,
            maxVolume = userSettingInfo.maxBolusDose!!,
            isBuzzOn = sp.getBoolean(CarelevoBooleanPreferenceKey.CARELEVO_BUZZER_REMINDER.key, false)
        )

        setUiState(UiState.Loading)
        viewModelScope.launch(Dispatchers.IO) {
            val outcome = runCatching {
                transport.adapter.removeBond(device.address)
                delay(commandDelay)
                val pairing = bleSession.runPairing(
                    device.address,
                    CarelevoBleSession.PairingSpec(
                        volume = request.volume,
                        remains = request.remains,
                        expiry = request.expiry,
                        maxBasalSpeed = request.maxBasalSpeed,
                        maxBolusDose = request.maxVolume,
                        buzzUse = request.isBuzzOn
                    )
                )
                check(
                    connectNewPatchUseCase.persistNewPatch(
                        address = pairing.address,
                        serialNumber = pairing.serialNumber,
                        firmwareVersion = pairing.firmwareVersion,
                        modelName = pairing.modelName,
                        request = request
                    )
                ) { "persist new patch failed" }
                pairing
            }
            setUiState(UiState.Idle)
            outcome.fold(
                onSuccess = { pairing ->
                    aapsLogger.info(LTag.PUMPCOMM, "newBle.pairing OK serial=${pairing.serialNumber} address=${pairing.address}")
                    triggerEvent(CarelevoConnectPrepareEvent.ConnectComplete)
                },
                onFailure = { e ->
                    if (e is CancellationException) throw e
                    aapsLogger.error(LTag.PUMPCOMM, "newBle.pairing FAILED", e)
                    triggerEvent(CarelevoConnectPrepareEvent.ConnectFailed)
                }
            )
        }
    }

    private fun connectNewPatch(inputInsulin: Int) {
        aapsLogger.debug(LTag.PUMPCOMM, "connectNewPatch called")

        if (!bleController.isBluetoothEnabled()) {
            aapsLogger.debug(LTag.PUMPCOMM, "bluetooth is not enabled")
            setUiState(UiState.Idle)
            triggerEvent(CarelevoConnectPrepareEvent.ShowMessageBluetoothNotEnabled)
            return
        }

        val userSettingInfo = carelevoPatch.userSettingInfo.value?.getOrNull()
        if (userSettingInfo == null) {
            aapsLogger.debug(LTag.PUMPCOMM, "userSettingInfo is null")
            setUiState(UiState.Idle)
            triggerEvent(CarelevoConnectPrepareEvent.ShowMessageNotSetUserSettingInfo)
            return
        }

        val expiry = sp.getInt(CarelevoIntPreferenceKey.CARELEVO_PATCH_EXPIRATION_REMINDER_HOURS.key, 116)
        val isBuzzOn = sp.getBoolean(CarelevoBooleanPreferenceKey.CARELEVO_BUZZER_REMINDER.key, false)

        compositeDisposable += connectNewPatchUseCase.execute(
            CarelevoConnectNewPatchRequestModel(
                volume = inputInsulin,
                expiry = expiry,
                remains = userSettingInfo.lowInsulinNoticeAmount!!,
                maxBasalSpeed = userSettingInfo.maxBasalSpeed!!,
                maxVolume = userSettingInfo.maxBolusDose!!,
                isBuzzOn = isBuzzOn
            )
        )
            .timeout(30000, TimeUnit.MILLISECONDS)
            .observeOn(aapsSchedulers.io)
            .subscribeOn(aapsSchedulers.io)
            .onErrorReturn {
                ResponseResult.Error(it)
            }
            .subscribe { response ->
                when (response) {
                    is ResponseResult.Success -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "response success")
                        triggerEvent(CarelevoConnectPrepareEvent.ConnectComplete)
                        setUiState(UiState.Idle)
                        // Pairing is complete. From the first CustomCommand onward the CommandQueue
                        // (CarelevoConnectionCoordinator) is the sole connect/reconnect manager, so
                        // tear down the wizard's off-queue btState connect observer here. Otherwise it
                        // would re-fire connectNewPatch (a full re-pair) on every queue-driven reconnect
                        // during later activation steps and contend with the queue.
                        connectDisposable.clear()
                    }

                    is ResponseResult.Error   -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "response error : ${response.e}")
                        triggerEvent(CarelevoConnectPrepareEvent.ConnectFailed)
                        setUiState(UiState.Idle)
                    }

                    else                      -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "response failed")
                        triggerEvent(CarelevoConnectPrepareEvent.ConnectFailed)
                        setUiState(UiState.Idle)
                    }
                }
            }
    }

    override fun onCleared() {
        aapsLogger.debug(LTag.PUMPCOMM, "onCleared")
        connectDisposable.clear()
        super.onCleared()
    }

    fun resetForEnterStep() {
        _selectedDevice = null
        _selectedNewStackDevice = null
        _isScanWorking = false
        bleController.clearScan()
        setUiState(UiState.Idle)
        connectDisposable.clear()
    }

    private companion object {

        // Legacy scans a fixed 10 s window (Thread.sleep in startScan); same budget, but the new-stack
        // scan completes early on the first hit.
        private const val NEW_STACK_SCAN_TIMEOUT_MS = 10_000L
    }
}
