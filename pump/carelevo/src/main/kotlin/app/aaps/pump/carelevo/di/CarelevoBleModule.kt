package app.aaps.pump.carelevo.di

import app.aaps.pump.carelevo.ble.CarelevoBleTransport
import app.aaps.pump.carelevo.ble.CarelevoBleTransportImpl
import app.aaps.pump.carelevo.config.BleEnvConfig
import dagger.Module
import dagger.Provides
import dagger.hilt.InstallIn
import dagger.hilt.components.SingletonComponent
import java.util.UUID
import javax.inject.Named
import javax.inject.Singleton

@Module
@InstallIn(SingletonComponent::class)
class CarelevoBleModule {

    @Provides
    @Named("cccDescriptor")
    internal fun provideCccDescriptor(): UUID = UUID.fromString(BleEnvConfig.BLE_CCC_DESCRIPTOR)

    @Provides
    @Named("serviceUuid")
    internal fun provideServiceUuid(): UUID = UUID.fromString(BleEnvConfig.BLE_SERVICE_UUID)

    @Provides
    @Named("characterTx")
    internal fun provideTxCharacteristicUuid(): UUID = UUID.fromString(BleEnvConfig.BLE_TX_CHAR_UUID)

    @Provides
    @Named("characterRx")
    internal fun provideRxCharacteristicUuid(): UUID = UUID.fromString(BleEnvConfig.BLE_RX_CHAR_UUID)

    /**
     * Shared-fleet [app.aaps.core.interfaces.pump.ble.BleTransport] for CareLevo.
     * The coroutine stack ([app.aaps.pump.carelevo.ble.BleClient] via
     * [app.aaps.pump.carelevo.ble.gatt.BleTransportGattConnection]) runs on this; a future
     * emulator impl can be swapped in here. See `_docs/carelevo-new-ble-stack.md`.
     */
    @Provides
    @Singleton
    internal fun provideCarelevoBleTransport(impl: CarelevoBleTransportImpl): CarelevoBleTransport = impl
}
