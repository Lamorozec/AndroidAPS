package app.aaps.pump.carelevo

import app.aaps.core.interfaces.pump.DetailedBolusInfo
import app.aaps.pump.carelevo.ble.commands.ExtendBolusCancelCommand
import app.aaps.pump.carelevo.ble.commands.ExtendBolusCommand
import app.aaps.pump.carelevo.ble.commands.ExtendBolusResponse
import app.aaps.pump.carelevo.domain.model.infusion.CarelevoImmeBolusInfusionInfoDomainModel
import app.aaps.pump.carelevo.domain.model.infusion.CarelevoInfusionInfoDomainModel
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.isA
import org.mockito.kotlin.whenever
import java.util.Optional

class CarelevoPumpPluginBolusTest : CarelevoPumpPluginTestBase() {

    @Test
    fun `deliverTreatment should require insulin greater than zero`() {
        val bolusInfo = DetailedBolusInfo().apply {
            insulin = 0.0
            carbs = 0.0
        }

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking { plugin.deliverTreatment(bolusInfo) }
        }
    }

    @Test
    fun `deliverTreatment should require carbs equal to zero`() {
        val bolusInfo = DetailedBolusInfo().apply {
            insulin = 1.0
            carbs = 10.0
        }

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking { plugin.deliverTreatment(bolusInfo) }
        }
    }

    @Test
    fun `deliverTreatment should return not enacted when bluetooth is disabled`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(false)

        val result = runBlocking {
            plugin.deliverTreatment(DetailedBolusInfo().apply {
                insulin = 1.0
                carbs = 0.0
            })
        }

        assertThat(result.enacted).isFalse()
    }

    @Test
    fun `deliverTreatment should return not enacted when no patch address is stored`() {
        whenever(carelevoPatch.getPatchInfoAddress()).thenReturn(null)

        val result = runBlocking {
            plugin.deliverTreatment(DetailedBolusInfo().apply {
                insulin = 1.0
                carbs = 0.0
            })
        }

        assertThat(result.enacted).isFalse()
        assertThat(result.bolusDelivered).isWithin(0.001).of(0.0)
    }

    @Test
    fun `deliverTreatment should reject when immediate bolus is already running`() {
        infusionInfoSubject.onNext(
            Optional.of(
                CarelevoInfusionInfoDomainModel(
                    immeBolusInfusionInfo = CarelevoImmeBolusInfusionInfoDomainModel(
                        infusionId = "imme-1",
                        address = "AA:BB:CC:DD:EE:FF",
                        mode = 3,
                        volume = 1.0,
                        infusionDurationSeconds = 30
                    )
                )
            )
        )

        val result = runBlocking {
            plugin.deliverTreatment(DetailedBolusInfo().apply {
                insulin = 1.0
                carbs = 0.0
            })
        }

        assertThat(result.success).isFalse()
        assertThat(result.enacted).isFalse()
        assertThat(result.bolusDelivered).isWithin(0.001).of(0.0)
    }

    @Test
    fun `setExtendedBolus should succeed when the pump accepts the command`() {
        val result = runBlocking { plugin.setExtendedBolus(insulin = 1.2, durationInMinutes = 30) }

        assertThat(result.success).isTrue()
        assertThat(result.enacted).isTrue()
    }

    @Test
    fun `setExtendedBolus should fail when the pump rejects the command`() {
        whenever { bleSession.runSingle(any(), isA<ExtendBolusCommand>(), any()) }
            .thenReturn(ExtendBolusResponse(resultCode = 1, expectedTimeSeconds = 0))

        val result = runBlocking { plugin.setExtendedBolus(insulin = 1.2, durationInMinutes = 30) }

        assertThat(result.success).isFalse()
        assertThat(result.enacted).isFalse()
    }

    @Test
    fun `cancelExtendedBolus should succeed when the pump accepts the command`() {
        val result = runBlocking { plugin.cancelExtendedBolus() }

        assertThat(result.success).isTrue()
        assertThat(result.enacted).isTrue()
        assertThat(result.isTempCancel).isTrue()
    }

    @Test
    fun `cancelExtendedBolus should fail when the session throws`() {
        whenever { bleSession.runSingle(any(), isA<ExtendBolusCancelCommand>(), any()) }
            .thenAnswer { throw IllegalStateException("failed") }

        val result = runBlocking { plugin.cancelExtendedBolus() }

        assertThat(result.success).isFalse()
        assertThat(result.enacted).isFalse()
    }
}
