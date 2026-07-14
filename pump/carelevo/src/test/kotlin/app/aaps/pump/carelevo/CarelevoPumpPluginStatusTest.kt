package app.aaps.pump.carelevo

import app.aaps.core.data.pump.defs.TimeChangeType
import app.aaps.pump.carelevo.command.CmdTimeZoneUpdate
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.result.ResultSuccess
import com.google.common.truth.Truth.assertThat
import io.reactivex.rxjava3.core.Single
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class CarelevoPumpPluginStatusTest : CarelevoPumpPluginTestBase() {

    @Test
    fun `connect should not throw and should keep plugin usable`() {
        plugin.connect("test")

        assertThat(plugin).isNotNull()
    }

    @Test
    fun `disconnect should not throw`() {
        plugin.disconnect("test")

        assertThat(plugin).isNotNull()
    }

    @Test
    fun `stopConnecting should not throw`() {
        plugin.stopConnecting()

        assertThat(plugin).isNotNull()
    }

    @Test
    fun `getPumpStatus should skip request when bluetooth is disabled`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(false)

        runBlocking { plugin.getPumpStatus("test") }

        verify(requestPatchInfusionInfoUseCase, never()).execute()
    }

    @Test
    fun `getPumpStatus should skip request when pump is disconnected`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)
        whenever(carelevoPatch.isCarelevoConnected()).thenReturn(false)

        runBlocking { plugin.getPumpStatus("test") }

        verify(requestPatchInfusionInfoUseCase, never()).execute()
    }

    @Test
    fun `getPumpStatus should request infusion info when connected`() {
        whenever(requestPatchInfusionInfoUseCase.execute()).thenReturn(
            Single.just(ResponseResult.Success(ResultSuccess))
        )

        runBlocking { plugin.getPumpStatus("test") }

        verify(requestPatchInfusionInfoUseCase).execute()
    }

    @Test
    fun `timezoneOrDSTChanged should route a CmdTimeZoneUpdate through the command queue`() {
        // Now managed by the queue (connect-before-execute) instead of a direct BLE write, so a resting
        // pump reconnects first. The executor runs the timezone use case on the queue worker thread.
        runBlocking {
            whenever(commandQueue.customCommand(any())).thenReturn(fakePumpEnactResult())

            plugin.timezoneOrDSTChanged(TimeChangeType.TimezoneChanged)

            verify(commandQueue).customCommand(any<CmdTimeZoneUpdate>())
        }
    }
}
