package app.aaps.pump.carelevo.domain.usecase.alarm

import app.aaps.pump.carelevo.domain.CarelevoPatchObserver
import app.aaps.pump.carelevo.domain.model.RequestResult
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.bt.Result
import app.aaps.pump.carelevo.domain.model.bt.SetAlarmClearRequest
import app.aaps.pump.carelevo.domain.model.bt.SetAlarmClearResultModel
import app.aaps.pump.carelevo.domain.model.result.ResultFailed
import app.aaps.pump.carelevo.domain.model.result.ResultSuccess
import app.aaps.pump.carelevo.domain.repository.CarelevoAlarmInfoRepository
import app.aaps.pump.carelevo.domain.repository.CarelevoPatchRepository
import app.aaps.pump.carelevo.domain.type.AlarmCause
import app.aaps.pump.carelevo.domain.type.AlarmType
import app.aaps.pump.carelevo.domain.usecase.CarelevoUseCaseRequest
import app.aaps.pump.carelevo.domain.usecase.CarelevoUseCaseResponse
import app.aaps.pump.carelevo.domain.usecase.alarm.model.AlarmClearUseCaseRequest
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.kotlin.ofType
import io.reactivex.rxjava3.schedulers.Schedulers
import java.time.LocalDateTime

class AlarmClearRequestUseCase(
    private val patchObserver: CarelevoPatchObserver,
    private val patchRepository: CarelevoPatchRepository,
    private val alarmRepository: CarelevoAlarmInfoRepository
) {

    fun execute(request: CarelevoUseCaseRequest): Single<ResponseResult<CarelevoUseCaseResponse>> {
        return Single.fromCallable {
            runCatching {
                val req = request as? AlarmClearUseCaseRequest
                    ?: throw IllegalArgumentException("request is not AlarmClearUseCaseRequest")

                val now = LocalDateTime.now().toString()
                val alarmTypeCmd = when (req.alarmCause.alarmType) {
                    AlarmType.ALERT -> 162
                    AlarmType.NOTICE -> 163
                    else -> throw IllegalArgumentException("alarmType is not supported")
                }

                val clearEventSingle = patchObserver.patchEvent
                    .ofType<SetAlarmClearResultModel>()
                    .firstOrError()
                    .timeout(10, java.util.concurrent.TimeUnit.SECONDS)

                when (val result = patchRepository.requestSetAlarmClear(
                    SetAlarmClearRequest(
                        alarmType = alarmTypeCmd,
                        causeId = req.alarmCause.code ?: 0
                    )
                ).blockingGet()) {
                    is RequestResult.Pending<*> -> Unit
                    is RequestResult.Success<*> -> throw IllegalStateException("request set alarm clear returned Success (expected Pending)")
                    is RequestResult.Failure    -> throw IllegalStateException("request set alarm clear failed: ${result.message}")
                    is RequestResult.Error      -> throw result.e
                }

                val clearResult = clearEventSingle.blockingGet()

                if (clearResult.result == Result.SUCCESS) {
                    alarmRepository.markAcknowledged(
                        alarmId = req.alarmId,
                        acknowledged = true,
                        updatedAt = now
                    ).blockingAwait()

                    ResultSuccess
                } else {
                    ResultFailed
                }
            }.fold(
                onSuccess = {
                    ResponseResult.Success(it)
                },
                onFailure = {
                    ResponseResult.Error(it)
                }
            )
        }.observeOn(Schedulers.io())
    }

    /**
     * Map an [AlarmCause]'s [AlarmType] to the wire alarm-type byte (ALERT=162, NOTICE=163) — the exact
     * mapping [execute] uses, extracted so the Phase-2 new-BLE-stack path can build the `AlarmClearCommand`.
     */
    fun commandAlarmType(alarmCause: AlarmCause): Int = when (alarmCause.alarmType) {
        AlarmType.ALERT  -> ALARM_TYPE_ALERT
        AlarmType.NOTICE -> ALARM_TYPE_NOTICE
        else             -> throw IllegalArgumentException("alarmType is not supported")
    }

    /**
     * Persist the alarm acknowledgement (`markAcknowledged`) — extracted from [execute]'s SUCCESS branch so
     * the Phase-2 new-BLE-stack path reuses the exact same write. Returns false if the write throws.
     */
    fun persistAlarmCleared(alarmId: String): Boolean = runCatching {
        alarmRepository.markAcknowledged(alarmId = alarmId, acknowledged = true, updatedAt = LocalDateTime.now().toString()).blockingAwait()
    }.isSuccess

    companion object {

        private const val ALARM_TYPE_ALERT = 162
        private const val ALARM_TYPE_NOTICE = 163
    }
}
