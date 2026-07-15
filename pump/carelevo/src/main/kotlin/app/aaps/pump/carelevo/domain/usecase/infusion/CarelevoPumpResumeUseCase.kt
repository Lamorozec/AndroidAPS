package app.aaps.pump.carelevo.domain.usecase.infusion

import app.aaps.pump.carelevo.domain.CarelevoPatchObserver
import app.aaps.pump.carelevo.domain.model.RequestResult
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.bt.ResumePumpRequest
import app.aaps.pump.carelevo.domain.model.bt.ResumePumpResultModel
import app.aaps.pump.carelevo.domain.model.bt.StopPumpResult
import app.aaps.pump.carelevo.domain.model.result.ResultSuccess
import app.aaps.pump.carelevo.domain.repository.CarelevoInfusionInfoRepository
import app.aaps.pump.carelevo.domain.repository.CarelevoPatchInfoRepository
import app.aaps.pump.carelevo.domain.repository.CarelevoPatchRepository
import app.aaps.pump.carelevo.domain.usecase.CarelevoUseCaseResponse
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.kotlin.ofType
import io.reactivex.rxjava3.schedulers.Schedulers
import org.joda.time.DateTime
import javax.inject.Inject

class CarelevoPumpResumeUseCase @Inject constructor(
    private val patchObserver: CarelevoPatchObserver,
    private val patchRepository: CarelevoPatchRepository,
    private val patchInfoRepository: CarelevoPatchInfoRepository,
    private val infusionInfoRepository: CarelevoInfusionInfoRepository
) {

    fun execute(): Single<ResponseResult<CarelevoUseCaseResponse>> {
        return Single.fromCallable {
            runCatching {
                patchRepository.requestResumePump(ResumePumpRequest(mode = 1, causeId = 0))
                    .blockingGet()
                    .takeIf { it is RequestResult.Pending }
                    ?: throw IllegalStateException("request pump resume is not pending")

                val requestResumePumpResult = patchObserver.patchEvent
                    .ofType<ResumePumpResultModel>()
                    .blockingFirst()

                if (requestResumePumpResult.result != StopPumpResult.BY_REQ) {
                    throw IllegalStateException("request pump resume result is failed: ${requestResumePumpResult.result}")
                }

                if (!persistResumed()) {
                    throw IllegalStateException("update resume state is failed")
                }
                ResultSuccess
            }.fold(
                onSuccess = {
                    ResponseResult.Success(it as CarelevoUseCaseResponse)
                },
                onFailure = {
                    ResponseResult.Error(it)
                }
            )
        }.observeOn(Schedulers.io())
    }

    /**
     * Persist the resumed state (basalInfusionInfo `mode=1,isStop=false` + patchInfo `isStopped=false, …`)
     * after a successful pump-resume. Extracted from [execute] so the Phase-2 new-BLE-stack path (the
     * executor's `bleClient` resume write) reuses the exact same persistence. Returns `false` on any
     * missing record or failed write.
     */
    fun persistResumed(): Boolean {
        val basalInfusionInfo = infusionInfoRepository.getInfusionInfoBySync()?.basalInfusionInfo ?: return false
        if (!infusionInfoRepository.updateBasalInfusionInfo(basalInfusionInfo.copy(updatedAt = DateTime.now(), mode = 1, isStop = false))) return false
        val patchInfo = patchInfoRepository.getPatchInfoBySync() ?: return false
        return patchInfoRepository.updatePatchInfo(
            patchInfo.copy(isStopped = false, stopMinutes = null, stopMode = null, isForceStopped = null, mode = 1)
        )
    }
}