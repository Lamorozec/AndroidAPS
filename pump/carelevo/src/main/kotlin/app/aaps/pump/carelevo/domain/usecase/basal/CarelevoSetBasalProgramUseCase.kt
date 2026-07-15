package app.aaps.pump.carelevo.domain.usecase.basal

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.profile.Profile
import app.aaps.pump.carelevo.domain.CarelevoPatchObserver
import app.aaps.pump.carelevo.domain.ext.generateUUID
import app.aaps.pump.carelevo.domain.ext.splitSegment
import app.aaps.pump.carelevo.domain.model.RequestResult
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.basal.CarelevoBasalSegment
import app.aaps.pump.carelevo.domain.model.basal.CarelevoBasalSegmentDomainModel
import app.aaps.pump.carelevo.domain.model.bt.SetBasalProgramRequestV2
import app.aaps.pump.carelevo.domain.model.bt.SetBasalProgramResult
import app.aaps.pump.carelevo.domain.model.bt.SetBasalProgramResultModel
import app.aaps.pump.carelevo.domain.model.infusion.CarelevoBasalInfusionInfoDomainModel
import app.aaps.pump.carelevo.domain.model.infusion.CarelevoBasalSegmentInfusionInfoDomainModel
import app.aaps.pump.carelevo.domain.model.result.ResultSuccess
import app.aaps.pump.carelevo.domain.repository.CarelevoBasalRepository
import app.aaps.pump.carelevo.domain.repository.CarelevoInfusionInfoRepository
import app.aaps.pump.carelevo.domain.repository.CarelevoPatchInfoRepository
import app.aaps.pump.carelevo.domain.usecase.CarelevoUseCaseRequest
import app.aaps.pump.carelevo.domain.usecase.CarelevoUseCaseResponse
import app.aaps.pump.carelevo.domain.usecase.basal.model.SetBasalProgramRequestModel
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.kotlin.ofType
import io.reactivex.rxjava3.schedulers.Schedulers
import org.joda.time.DateTime
import java.util.concurrent.TimeUnit
import javax.inject.Inject

class CarelevoSetBasalProgramUseCase @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val patchObserver: CarelevoPatchObserver,
    private val basalRepository: CarelevoBasalRepository,
    private val patchInfoRepository: CarelevoPatchInfoRepository,
    private val infusionInfoRepository: CarelevoInfusionInfoRepository
) {

    fun execute(request: CarelevoUseCaseRequest): Single<ResponseResult<CarelevoUseCaseResponse>> {
        return Single.fromCallable {
            runCatching {
                if (request !is SetBasalProgramRequestModel) {
                    throw IllegalArgumentException("request is not SetBasalProgramRequestModel")
                }

                val profileBasalSegment = request.profile.getBasalValues()
                val basalSegment = profileBasalSegment.mapIndexed { index, value ->
                    val nextIndex = if (profileBasalSegment.size == index + 1) {
                        0
                    } else {
                        index + 1
                    }
                    val startTimeMinutes = TimeUnit.SECONDS.toMinutes(value.timeAsSeconds.toLong())
                    val endTimeMinutes = if (nextIndex == 0) {
                        1440
                    } else {
                        TimeUnit.SECONDS.toMinutes(profileBasalSegment[nextIndex].timeAsSeconds.toLong())
                    }
                    CarelevoBasalSegmentDomainModel(
                        startTime = startTimeMinutes.toInt(),
                        endTime = endTimeMinutes.toInt(),
                        speed = value.value
                    )
                }.splitSegment()

                aapsLogger.debug(LTag.PUMPCOMM, "splitSegment result=$basalSegment")

                val requestBasalList = basalSegment
                    .chunked(8)
                    .mapIndexed { index, group ->
                        val segmentGroup = group.map {
                            CarelevoBasalSegment(
                                injectStartHour = 1,
                                injectStartMin = 0,
                                injectSpeed = it.speed
                            )
                        }
                        SetBasalProgramRequestV2(
                            seqNo = index,
                            segmentList = segmentGroup
                        )
                    }

                aapsLogger.debug(LTag.PUMPCOMM, "buildRequestList result=$requestBasalList")

                val programRequest1 = requestBasalList[0]
                basalRepository.requestSetBasalProgramV2(programRequest1)
                    .blockingGet()
                    .takeIf { it is RequestResult.Pending }
                    ?: throw IllegalStateException("request program1 is not pending")

                aapsLogger.debug(LTag.PUMPCOMM, "requestProgram1.start")

                val requestProgram1Result = patchObserver.basalEvent
                    .ofType<SetBasalProgramResultModel>()
                    .blockingFirst()

                aapsLogger.debug(LTag.PUMPCOMM, "requestProgram1.result result=$requestProgram1Result")

                if (requestProgram1Result.result != SetBasalProgramResult.SUCCESS) {
                    throw IllegalStateException("request program1 result is failed")
                }

                val programRequest2 = requestBasalList[1]
                basalRepository.requestSetBasalProgramV2(programRequest2)
                    .blockingGet()
                    .takeIf { it is RequestResult.Pending }
                    ?: throw IllegalStateException("request program2 is not pending")

                aapsLogger.debug(LTag.PUMPCOMM, "requestProgram2.start")

                val requestProgram2Result = patchObserver.basalEvent
                    .ofType<SetBasalProgramResultModel>()
                    .blockingFirst()

                aapsLogger.debug(LTag.PUMPCOMM, "requestProgram2.result result=$requestProgram2Result")

                if (requestProgram2Result.result != SetBasalProgramResult.SUCCESS) {
                    throw IllegalStateException("request program2 result is failed")
                }

                val programRequest3 = requestBasalList[2]
                basalRepository.requestSetBasalProgramV2(programRequest3)
                    .blockingGet()
                    .takeIf { it is RequestResult.Pending }
                    ?: throw IllegalStateException("request program3 is not pending")

                aapsLogger.debug(LTag.PUMPCOMM, "requestProgram3.start")

                val requestProgram3Result = patchObserver.basalEvent
                    .ofType<SetBasalProgramResultModel>()
                    .blockingFirst()

                aapsLogger.debug(LTag.PUMPCOMM, "requestProgram3.result result=$requestProgram3Result")

                if (requestProgram3Result.result != SetBasalProgramResult.SUCCESS) {
                    throw IllegalStateException("request program3 result is failed")
                }

                val patchInfo = patchInfoRepository.getPatchInfoBySync()
                    ?: throw NullPointerException("patch info must be not null")

                val updatePatchInfoResult = patchInfoRepository.updatePatchInfo(patchInfo.copy(updatedAt = DateTime.now(), mode = 1))

                aapsLogger.debug(LTag.PUMPCOMM, "updatePatchInfo result=$updatePatchInfoResult")

                if (!updatePatchInfoResult) {
                    throw IllegalStateException("update patch info is failed")
                }

                val updateInfusionInfoResult = infusionInfoRepository.updateBasalInfusionInfo(
                    CarelevoBasalInfusionInfoDomainModel(
                        infusionId = generateUUID(),
                        address = patchInfo.address,
                        mode = 1,
                        segments = basalSegment.map {
                            CarelevoBasalSegmentInfusionInfoDomainModel(
                                startTime = it.startTime,
                                endTime = it.endTime,
                                speed = it.speed
                            )
                        },
                        isStop = false
                    )
                )

                aapsLogger.debug(LTag.PUMPCOMM, "updateInfusionInfo result=$updateInfusionInfoResult")

                if (!updateInfusionInfoResult) {
                    throw IllegalStateException("update infusion info is failed")
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
     * Compute the basal program from [profile] the exact way [execute] does (`getBasalValues` →
     * `splitSegment` → 24 hourly segments → three `chunked(8)` speed lists, seqNo 0/1/2). Extracted for the
     * Phase-2 new-BLE-stack path: [BasalProgramPlan.programs] are the wire payloads (v2 sends speed only,
     * not hour/min), [BasalProgramPlan.segments] is the full 24-segment list the infusion-info persist needs.
     */
    fun buildBasalProgramPlan(profile: Profile): BasalProgramPlan {
        val profileBasalSegment = profile.getBasalValues()
        val basalSegment = profileBasalSegment.mapIndexed { index, value ->
            val nextIndex = if (profileBasalSegment.size == index + 1) 0 else index + 1
            val startTimeMinutes = TimeUnit.SECONDS.toMinutes(value.timeAsSeconds.toLong())
            val endTimeMinutes = if (nextIndex == 0) MINUTES_PER_DAY else TimeUnit.SECONDS.toMinutes(profileBasalSegment[nextIndex].timeAsSeconds.toLong())
            CarelevoBasalSegmentDomainModel(
                startTime = startTimeMinutes.toInt(),
                endTime = endTimeMinutes.toInt(),
                speed = value.value
            )
        }.splitSegment()
        val programs = basalSegment.chunked(SEGMENTS_PER_PROGRAM).map { group -> group.map { it.speed } }
        return BasalProgramPlan(programs = programs, segments = basalSegment)
    }

    /**
     * Persist the basal program (`mode=1` + basal infusion-info) — extracted from [execute]'s post-write
     * branch so the Phase-2 new-BLE-stack path reuses the exact same writes. Returns false with no patch
     * record or if any write fails.
     */
    fun persistBasalProgram(segments: List<CarelevoBasalSegmentDomainModel>): Boolean = runCatching {
        val patchInfo = patchInfoRepository.getPatchInfoBySync()
            ?: throw NullPointerException("patch info must be not null")
        require(patchInfoRepository.updatePatchInfo(patchInfo.copy(updatedAt = DateTime.now(), mode = 1))) { "update patch info is failed" }
        require(
            infusionInfoRepository.updateBasalInfusionInfo(
                CarelevoBasalInfusionInfoDomainModel(
                    infusionId = generateUUID(),
                    address = patchInfo.address,
                    mode = 1,
                    segments = segments.map {
                        CarelevoBasalSegmentInfusionInfoDomainModel(startTime = it.startTime, endTime = it.endTime, speed = it.speed)
                    },
                    isStop = false
                )
            )
        ) { "update infusion info is failed" }
    }.isSuccess

    /** The three sequential basal-program payloads ([programs], seqNo 0/1/2) plus the full [segments] list. */
    data class BasalProgramPlan(
        val programs: List<List<Double>>,
        val segments: List<CarelevoBasalSegmentDomainModel>
    )

    companion object {

        private const val MINUTES_PER_DAY = 1440L
        private const val SEGMENTS_PER_PROGRAM = 8
    }
}
