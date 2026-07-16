package app.aaps.pump.carelevo.presentation.viewmodel

import android.os.Handler
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.resources.ResourceHelper
import app.aaps.core.interfaces.ui.UiInteraction
import app.aaps.core.ui.R as CoreUiR
import app.aaps.pump.carelevo.R
import app.aaps.pump.carelevo.common.CarelevoAlarmActionHandler
import app.aaps.pump.carelevo.domain.model.alarm.CarelevoAlarmInfo
import app.aaps.pump.carelevo.domain.type.AlarmCause
import app.aaps.pump.carelevo.ext.transformNotificationStringResources
import app.aaps.pump.carelevo.presentation.model.AlarmEvent
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import org.robolectric.RobolectricTestRunner
import org.robolectric.Shadows
import org.robolectric.annotation.Config
import java.time.Duration

/**
 * Unit tests for [CarelevoAlarmViewModel].
 *
 * The ViewModel is a thin UI shell that owns ONLY the alarm-sound lifecycle and forwards clear
 * requests to [CarelevoAlarmActionHandler]. Its constructor eagerly instantiates a real Android
 * [android.os.HandlerThread] + [Handler] (a dedicated re-arm thread for the "mute 5 min" flow), so
 * a plain-JVM Mockito test cannot construct it — the framework classes return `Stub!`. This suite
 * therefore runs under [RobolectricTestRunner] (matching `CarelevoAlarmNotifierTest` in the same
 * module) so the HandlerThread/Handler/Looper all execute for real, while every non-Android
 * collaborator ([aapsLogger], [uiInteraction], [rh], [alarmActionHandler]) is mocked.
 *
 * The action handler is mocked, so the three delegated flows are stubbed and asserted to be the
 * exact same instances the shell re-exposes. Sound side effects are asserted on the mocked
 * [UiInteraction] (`runAlarm` / `stopAlarm`); the private re-arm [Handler] is driven through
 * Robolectric's paused-looper shadow to fire the 5-minute delayed re-alarm deterministically.
 */
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35])
class CarelevoAlarmViewModelTest {

    private lateinit var aapsLogger: AAPSLogger
    private lateinit var uiInteraction: UiInteraction
    private lateinit var rh: ResourceHelper
    private lateinit var alarmActionHandler: CarelevoAlarmActionHandler

    // Backing flows the mocked handler hands out; the shell must re-expose these very instances.
    private val alarmQueueFlow: MutableStateFlow<List<CarelevoAlarmInfo>> = MutableStateFlow(emptyList())
    private val emptyEventFlow: MutableSharedFlow<Unit> = MutableSharedFlow()
    private val uiRequestsFlow: MutableSharedFlow<AlarmEvent> = MutableSharedFlow()

    private lateinit var sut: CarelevoAlarmViewModel

    private fun alarm(
        cause: AlarmCause = AlarmCause.ALARM_NOTICE_LGS_START,
        id: String = "alarm-1"
    ): CarelevoAlarmInfo =
        CarelevoAlarmInfo(
            alarmId = id,
            alarmType = cause.alarmType,
            cause = cause,
            value = null,
            createdAt = "2026-07-16T10:00:00",
            updatedAt = "2026-07-16T10:00:00",
            isAcknowledged = false
        )

    @Before
    fun setUp() {
        aapsLogger = mock()
        uiInteraction = mock()
        rh = mock()
        alarmActionHandler = mock()

        // Read once at construction time (they become `val`s on the shell).
        whenever(alarmActionHandler.alarmQueue).thenReturn(alarmQueueFlow)
        whenever(alarmActionHandler.alarmQueueEmptyEvent).thenReturn(emptyEventFlow)
        whenever(alarmActionHandler.uiRequests).thenReturn(uiRequestsFlow)

        sut = CarelevoAlarmViewModel(aapsLogger, uiInteraction, rh, alarmActionHandler)
    }

    @After
    fun tearDown() {
        // Stop the dedicated HandlerThread each test spun up so its looper thread does not leak.
        runCatching { handlerOf(sut).looper.quitSafely() }
    }

    // ---- reflection helpers (private members the shell owns) -----------------------------------

    private fun handlerOf(vm: CarelevoAlarmViewModel): Handler {
        val field = CarelevoAlarmViewModel::class.java.getDeclaredField("handler")
        field.isAccessible = true
        return field.get(vm) as Handler
    }

    private fun invokeOnCleared(vm: CarelevoAlarmViewModel) {
        val method = CarelevoAlarmViewModel::class.java.getDeclaredMethod("onCleared")
        method.isAccessible = true
        method.invoke(vm)
    }

    // ---- delegation / exposed state -----------------------------------------------------------

    @Test
    fun `re-exposes the handler queue empty-event and ui-request flows unchanged`() {
        assertThat(sut.alarmQueue).isSameInstanceAs(alarmQueueFlow)
        assertThat(sut.alarmQueueEmptyEvent).isSameInstanceAs(emptyEventFlow)
        assertThat(sut.event).isSameInstanceAs(uiRequestsFlow)
    }

    @Test
    fun `alarmInfo defaults to null`() {
        assertThat(sut.alarmInfo).isNull()
    }

    @Test
    fun `alarmInfo is settable`() {
        val info = alarm()
        sut.alarmInfo = info
        assertThat(sut.alarmInfo).isSameInstanceAs(info)
    }

    @Test
    fun `loadActiveAlarms delegates to the action handler`() {
        sut.loadActiveAlarms()

        verify(alarmActionHandler).loadActiveAlarms()
    }

    // ---- triggerEvent branches ----------------------------------------------------------------

    @Test
    fun `triggerEvent ClearAlarm stops the alarm stores the info and forwards to the handler`() {
        val info = alarm()
        val event = AlarmEvent.ClearAlarm(info)

        sut.triggerEvent(event)

        verify(uiInteraction).stopAlarm("Confirm Click")
        assertThat(sut.alarmInfo).isSameInstanceAs(info)
        verify(alarmActionHandler).triggerEvent(event)
    }

    @Test
    fun `triggerEvent Mute only stops the alarm and does not touch the handler`() {
        sut.triggerEvent(AlarmEvent.Mute)

        verify(uiInteraction).stopAlarm("Mute Click")
        verify(uiInteraction, never()).runAlarm(any<String>(), any<String>(), any<Int>())
        verify(alarmActionHandler, never()).triggerEvent(any())
    }

    @Test
    fun `triggerEvent Mute5min stops the alarm and does not re-arm immediately`() {
        sut.triggerEvent(AlarmEvent.Mute5min)

        verify(uiInteraction).stopAlarm("Mute5min Click")
        // The re-arm is a delayed post on the dedicated thread; without advancing time it must not fire.
        verify(uiInteraction, never()).runAlarm(any<String>(), any<String>(), any<Int>())
    }

    @Test
    fun `triggerEvent Mute5min re-arms the alarm after five minutes`() {
        whenever(rh.gs(R.string.carelevo)).thenReturn("Carelevo")

        sut.triggerEvent(AlarmEvent.Mute5min)
        // Drive the dedicated HandlerThread's paused looper past the 5-minute delay so the re-arm runs.
        Shadows.shadowOf(handlerOf(sut).looper).idleFor(Duration.ofMinutes(6))

        // alarmInfo is null → status falls back to the app title.
        verify(uiInteraction).runAlarm(eq("Carelevo"), eq("Carelevo"), eq(CoreUiR.raw.error))
    }

    @Test
    fun `triggerEvent StartAlarm with no alarm info runs the alarm using the app title as status`() {
        whenever(rh.gs(R.string.carelevo)).thenReturn("Carelevo")

        sut.triggerEvent(AlarmEvent.StartAlarm)

        verify(uiInteraction).runAlarm(eq("Carelevo"), eq("Carelevo"), eq(CoreUiR.raw.error))
    }

    @Test
    fun `triggerEvent StartAlarm with alarm info runs the alarm using the cause title as status`() {
        val causeTitleRes = AlarmCause.ALARM_NOTICE_LGS_START.transformNotificationStringResources().first
        whenever(rh.gs(R.string.carelevo)).thenReturn("Carelevo")
        whenever(rh.gs(causeTitleRes)).thenReturn("LGS Started")
        sut.alarmInfo = alarm(AlarmCause.ALARM_NOTICE_LGS_START)

        sut.triggerEvent(AlarmEvent.StartAlarm)

        verify(uiInteraction).runAlarm(eq("LGS Started"), eq("Carelevo"), eq(CoreUiR.raw.error))
    }

    @Test
    fun `triggerEvent with a non-handled event does nothing`() {
        sut.triggerEvent(AlarmEvent.NoAction)

        verifyNoInteractions(uiInteraction)
        verify(alarmActionHandler, never()).triggerEvent(any())
        assertThat(sut.alarmInfo).isNull()
    }

    // ---- onCleared ----------------------------------------------------------------------------

    @Test
    fun `onCleared removes callbacks and quits the handler thread without throwing`() {
        // Executes handler.removeCallbacksAndMessages(null) + handler.looper.quitSafely().
        invokeOnCleared(sut)
        // Idempotent: a second teardown (e.g. the @After) must also be safe.
        invokeOnCleared(sut)
    }

    @Test
    fun `onCleared drops a pending Mute5min re-arm so the alarm never fires`() {
        whenever(rh.gs(R.string.carelevo)).thenReturn("Carelevo")
        sut.triggerEvent(AlarmEvent.Mute5min)

        invokeOnCleared(sut)

        // The pending delayed re-arm was cleared and the looper is quitting; advancing time must not re-arm.
        runCatching { Shadows.shadowOf(handlerOf(sut).looper).idleFor(Duration.ofMinutes(6)) }
        verify(uiInteraction, never()).runAlarm(any<String>(), any<String>(), any<Int>())
    }
}
