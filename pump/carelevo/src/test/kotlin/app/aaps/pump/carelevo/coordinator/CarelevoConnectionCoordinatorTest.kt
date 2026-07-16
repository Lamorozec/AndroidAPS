package app.aaps.pump.carelevo.coordinator

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.domain.model.patch.CarelevoPatchInfoDomainModel
import com.google.common.truth.Truth.assertThat
import io.reactivex.rxjava3.subjects.BehaviorSubject
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import java.util.Optional

@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
internal class CarelevoConnectionCoordinatorTest {

    @Mock lateinit var aapsLogger: AAPSLogger
    @Mock lateinit var carelevoPatch: CarelevoPatch

    private lateinit var sut: CarelevoConnectionCoordinator

    private fun patchInfo(
        mode: Int? = null,
        runningMinutes: Int? = null,
        pumpState: Int? = null
    ): CarelevoPatchInfoDomainModel =
        CarelevoPatchInfoDomainModel(
            address = "AA:BB:CC:DD:EE:FF",
            mode = mode,
            runningMinutes = runningMinutes,
            pumpState = pumpState
        )

    private fun stubPatchInfo(subject: BehaviorSubject<Optional<CarelevoPatchInfoDomainModel>>) {
        whenever(carelevoPatch.patchInfo).thenReturn(subject)
    }

    @BeforeEach
    fun setUp() {
        sut = CarelevoConnectionCoordinator(aapsLogger, carelevoPatch)
    }

    // ---- isInitialized ----

    @Test
    fun `isInitialized false when the patch subject has no value yet`() {
        // BehaviorSubject with no default → patchInfo.value is null → early return false.
        stubPatchInfo(BehaviorSubject.create<Optional<CarelevoPatchInfoDomainModel>>())
        assertThat(sut.isInitialized()).isFalse()
    }

    @Test
    fun `isInitialized false when the patch info is empty`() {
        stubPatchInfo(BehaviorSubject.createDefault(Optional.empty<CarelevoPatchInfoDomainModel>()))
        assertThat(sut.isInitialized()).isFalse()
    }

    @Test
    fun `isInitialized false when mode, runningMinutes and pumpState are all null`() {
        stubPatchInfo(BehaviorSubject.createDefault(Optional.of(patchInfo())))
        assertThat(sut.isInitialized()).isFalse()
    }

    @Test
    fun `isInitialized true when mode is set`() {
        stubPatchInfo(BehaviorSubject.createDefault(Optional.of(patchInfo(mode = 1))))
        assertThat(sut.isInitialized()).isTrue()
    }

    @Test
    fun `isInitialized true when only runningMinutes is set`() {
        stubPatchInfo(BehaviorSubject.createDefault(Optional.of(patchInfo(runningMinutes = 100))))
        assertThat(sut.isInitialized()).isTrue()
    }

    @Test
    fun `isInitialized true when only pumpState is set`() {
        stubPatchInfo(BehaviorSubject.createDefault(Optional.of(patchInfo(pumpState = 0))))
        assertThat(sut.isInitialized()).isTrue()
    }

    // ---- isConnected ----

    @Test
    fun `isConnected is always true (per-op sessions)`() {
        assertThat(sut.isConnected()).isTrue()
    }

    // ---- connect / disconnect / stopConnecting no-ops ----

    @Test
    fun `connect is a no-op that only logs`() {
        sut.connect("bootstrap")
        verify(aapsLogger).debug(LTag.PUMPCOMM, "connect.noop reason=bootstrap (per-op sessions)")
    }

    @Test
    fun `disconnect is a no-op that only logs`() {
        sut.disconnect("shutdown")
        verify(aapsLogger).debug(LTag.PUMPCOMM, "disconnect.noop reason=shutdown (per-op sessions)")
    }

    @Test
    fun `stopConnecting is a no-op that only logs`() {
        sut.stopConnecting()
        verify(aapsLogger).debug(LTag.PUMPCOMM, "stopConnecting.called")
    }
}
