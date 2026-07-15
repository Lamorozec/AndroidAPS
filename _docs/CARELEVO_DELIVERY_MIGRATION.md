# CareLevo — Delivery-Path Migration (new BLE stack)

Detailed plan for migrating the CareLevo **delivery path** (bolus / temp-basal / extended-bolus + cancels)
from the legacy Rx BLE stack onto the coroutine `BleClient` stack. This is the coordinator-rewire portion of
**Phase 2.C** in [`carelevo-new-ble-stack.md`](carelevo-new-ble-stack.md) (the master roadmap); read that first
for the stack architecture, the lost-event race, and the connection-ownership border.

Status: **planned, not started** (2026-07-15). Flag: `CARELEVO_USE_NEW_BLE_STACK` (engineering-only, default
off). Everything below is flag-gated with the legacy Rx path left as the untouched fallback.

---

## Scope

Already migrated (flag-gated, device-validated): status read (infusion info 0x31) + **all** executor
`customCommand` ops (buzzer, pump-stop/resume, thresholds, timezone, priming, discard, needle, safety-check,
alarm-clear ×2, activation set-basal).

**This doc covers the remaining, delivery-critical ops**, which flow *plugin `Pump` methods → coordinators →
use cases → legacy Rx BLE*, NOT through the executor `customCommand` path:

| Op | Plugin method | Coordinator | Command (already written, byte-verified) |
|---|---|---|---|
| Immediate bolus | `deliverTreatment` | `CarelevoBolusCoordinator` | `ImmediateBolusCommand` 0x24→0x84 |
| Bolus cancel (stop) | `stopBolusDelivering` | `CarelevoBolusCoordinator` | `BolusCancelCommand` 0x2C→0x8C |
| Extended bolus set | `setExtendedBolus` | `CarelevoBolusCoordinator` | `ExtendBolusCommand` 0x25→0x85 |
| Extended bolus cancel | `cancelExtendedBolus` | `CarelevoBolusCoordinator` | `ExtendBolusCancelCommand` 0x29→0x89 |
| Temp basal absolute | `setTempBasalAbsolute` | `CarelevoTempBasalCoordinator` | `TempBasalCommand.byUnit` 0x23→0x83 |
| Temp basal percent | `setTempBasalPercent` | `CarelevoTempBasalCoordinator` | `TempBasalCommand.byPercent` 0x23→0x83 |
| Temp basal cancel | `cancelTempBasal` | `CarelevoTempBasalCoordinator` | `TempBasalCancelCommand` 0x2D→0x8D |
| Basal profile *update* | `updateBasalProfile` | `CarelevoBasalProfileUpdateCoordinator` | `BasalProgramCommand(isUpdate=true)` 0x21→0x81 |

> **Note on set-basal:** the *activation* set-basal (0x13, initial program) is already migrated via the
> executor. The *mid-therapy* profile **update** (0x21) above is a separate delivery-path op and is in scope
> here (it also cancels a running TBR/extended bolus first via lambdas).
> **✅ DONE + DEVICE-VALIDATED (2026-07-15, 2.D-0a, profile switch):** `CarelevoBasalProfileUpdateCoordinator.executeBasalProgram`
> is flag-gated → `gateway.runBasalProgram(…, isUpdate = !shouldUseSetBasalProgram)`; persist reuses
> `CarelevoSetBasalProgramUseCase.buildBasalProgramPlan/persistBasalProgram` (identical to the update use
> case's); outer timeout 20 s → 60 s on the new branch.

---

## Key architectural finding — no persistent-connection rewire needed for delivery

The concern that a bolus "holds the link for minutes" is a **non-issue**, because:

1. **All six delivery commands are discrete `BleCommand`s** (single request → single response). None stream.
   They are already written, unit-tested, and byte-identical to the legacy request builders / response parsers.
2. **Bolus progress is *synthetic*.** The pump returns only `expectedCompletionSeconds` in the 0x84 start-ACK;
   `CarelevoBolusCoordinator.handleBolusSuccess` fabricates the progress bar on a local timer. There is **no**
   pump-sent progress frame and **no** completion frame — "finish" is a **DB-only** use case, no BLE. So the
   connection is **not needed during delivery**: start round-trip → close session → run timer with no link →
   finish (DB).
3. **Every delivery op already runs on the CommandQueue worker thread** (via `CommandBolus` /
   `CommandTempBasal*` / `CommandExtendedBolus` → the plugin `Pump` methods, blocking). That's the *same*
   invariant that makes the executor's connection-ownership border safe — while the worker is blocked in the
   op, it cannot re-dial legacy.

**Therefore the entire delivery path reuses the per-op fresh-session pattern already shipped for the executor
ops.** A genuinely persistent new-transport connection (rewriting `CarelevoConnectionCoordinator`) is a later
optimization (D4), **not a prerequisite**.

Do **not** re-route delivery through new `customCommand`s — it is already on the queue worker via the standard
`Command*` handlers; the correct change is a flag-gate *inside each coordinator method*.

---

## Shared infrastructure (build first, before D1)

1. **Global new-stack session mutex.** Executor ops never overlap because the queue serializes them. But bolus
   **cancel arrives out-of-band** — `CommandQueue.cancelAllBoluses()` spawns `Thread { stopBolusDelivering() }`,
   off the worker. That cancel session could collide with an in-flight delivery session (the transport is a
   `@Singleton` with **one** GATT + **one** listener slot → two concurrent sessions = status-133). A mutex that
   every `CarelevoBleSession` op acquires makes the out-of-band cancel wait for the start session to *release*
   (start closes before the sim loop), then open cleanly. Serializes delivery-session vs cancel-session vs any
   concurrent status read.
2. **Coordinator-side border helper**, mirroring the executor's `runSingleWriteViaNewStack`:
   `connectionCoordinator.disconnect("new-ble-session")` → `delay(NEW_BLE_SETTLE_MS = 1000)` → acquire session
   mutex → `bleSession.runSingle(cmd)` → `resultCode == 0` → extracted persist. Inject `bleSession`,
   `connectionCoordinator`, `preferences` into the coordinators (they currently have none of these).
3. **Extract-persist methods** on the delivery use cases (parity, exactly like `persistStopped` /
   `persistBasalProgram`); leave each legacy `execute()` intact (additive):
   - `CarelevoStartTempBasalInfusionUseCase.persistTempBasalStarted(...)` / `...Cancel...persistTempBasalCancelled()`
   - `CarelevoStartExtendBolusInfusionUseCase.persistExtendBolusStarted(...)` / `...Cancel...persistExtendBolusCancelled()`
   - `CarelevoStartImmeBolusInfusionUseCase.persistImmeBolusStarted(...)` + `CarelevoFinishImmeBolusInfusionUseCase.persistImmeBolusFinished()`
   - **`pumpSync.*` stays in the coordinators, unchanged** (it is the AAPS-main-DB write, distinct from the
     CareLevo-local Room writes done inside the use cases).

---

## Phased plan (flag-gated; each phase device-tested on a physical patch before the next)

### D1 — Temp basal (set absolute/percent + cancel)  ·  simplest, proves the coordinator border

Pure discrete request/response, no progress, no cancel-while-running. `TempBasalCommand.byUnit/byPercent`
already reproduce the byte asymmetry (**6-byte** unit with trailing `0x00` vs **5-byte** percent, value =
`percent/100.0`, range 0..200); `TempBasalCancelCommand` is opcode-only.

- Flag-gate inside `CarelevoTempBasalCoordinator.setTempBasalAbsolute/Percent/cancelTempBasal`: border helper →
  `runSingle(TempBasalCommand.byUnit(rate, hour, min) | .byPercent(percent, hour, min) | TempBasalCancelCommand())`
  → `resultCode==0` → extracted persist → **then** the same `pumpSync.syncTemporaryBasalWithPumpId /
  syncStopTemporaryBasalWithPumpId` the legacy coordinator does. Compute `hour = min/60`, `min = min%60`.
- Drop the cancel's legacy `delaySubscription(2000ms)` (the session's own 1 s settle + connect handshake covers
  the patch-settle). Re-check the tight **3 s** percent timeout against fresh-connect latency (connect alone is
  ~a few hundred ms + `CONNECT_TIMEOUT_MS`); adopt the 10 s/15 s bounds if needed.
- Behavior decision: today a disconnected patch → **silent no-op** (bare `PumpEnactResult`). The new path uses
  `getPatchInfoAddress()`; decide fail-explicitly vs no-op and keep it consistent.

### D2 — Extended bolus (set + cancel)  ·  same shape as D1

Set = one 0x25→0x85 (pump then delivers autonomously, no progress UI); cancel = 0x29→0x89 returning
`infusedAmount`.

- Flag-gate inside `CarelevoBolusCoordinator.setExtendedBolus/cancelExtendedBolus`; extracted persist +
  unchanged `pumpSync.syncExtendedBolusWithPumpId / syncStopExtendedBolusWithPumpId`.
- **Safety guard:** legacy **always** sends `immediateDose = 0.0` (pure extended). Pass `0.0` explicitly or an
  unintended upfront dose is injected. `extendedSpeed = volume/(minutes/60)`.
- Surface `ExtendBolusCancelResponse.infusedAmount` to keep `CancelBolusInfusionResponseModel` +
  `result.isTempCancel = true`. (Legacy does not reconcile that amount into the DB — keep it like-for-like.)

### D3 — Immediate bolus  ·  the hard one (progress + cancel)

- **Start:** border helper → `runSingle(ImmediateBolusCommand(actionId, volume))`. `correlationByte = actionId`
  is a *stricter* correlation guard than legacy's `blockingFirst()`-by-type. Preserve `actionId` sequencing
  (`(bolusActionSeq ?: 0)+1` normalized 1..255) and persist it (`persistImmeBolusStarted`, mode=3).
- **Close the session**, then run the **existing synthetic progress loop** with **no link**, moved from
  `SystemClock.sleep` to coroutine `delay()`, still polling `BolusProgressData.isStopPressed` each ~100 ms.
- **Finish** = DB-only (`persistImmeBolusFinished`), no BLE.
- **Cancel = Option A (chosen, see below).**
- **Do not touch** the `BolusProgressData.start / updateProgress / clear` call pattern — the Phase-3
  client-progress mirror (`ClientControlReceiver` observing `BolusProgressData.state`) depends on it and would
  silently break. Keep the generation-scoped `clear()` (prevents an SMB wiping a manual bolus's state).
- Preserve the `isStopPressed` (instant) vs `isImmeBolusStop` (set only on 0x8C confirm) mutual-exclusion so a
  completion (full) and a cancel (partial) never both write to the treatment DB (`pumpId = now`, not a real
  pump id, so there is no DB-level de-dup — the ordering is the only guard).

### D4 — Persistent-connection rewire  ·  DEFERRED (only if needed)

Only if per-op connect/disconnect churn or cancel latency proves unacceptable on-device. Large: coordinator
owns one long-lived `BleClient`; the queue's connect-before / idle-disconnect (5 s keep-alive) drives the new
transport directly; the RxJava `btState` reconnection state-machine ports to a coroutine supervisor; a
long-lived `unsolicitedEvents → CarelevoPatch` bridge handles async alarms/reports (see "Known limitation").
Keep this a separate effort.

---

## Decision (locked 2026-07-15): bolus cancel = Option A

**Option A — per-op cancel session (chosen).** On stop: break the loop, open a *fresh* session for
`BolusCancelCommand`. Simplest, fits the per-op model, no held connection.

- **Cost:** the cancel pays a ~1–2 s connect before the 0x2C reaches the pump (vs legacy writing over an
  already-open link) → marginally more insulin delivered before the pump halts.
- **Mitigations:** the session mutex serializes it against the (already-closed) start session; re-base the
  legacy `15 s × maxRetry` cancel retry on a **wall-clock deadline**; keep polling `isStopPressed` so the sim
  loop halts instantly while the cancel session spins up.
- **Gate:** stop-to-halt latency **must be measured on the Pixel with a real patch** before the flag is
  flipped for delivery.

**Option B — held session for the whole bolus** (cancel writes instantly on the open client, legacy-equivalent
latency, but holds an idle GATT for minutes + must signal the cancel into the delivery coroutine): **held in
reserve**, adopt only if A's measured latency is unacceptable.

---

## Known limitation (called out, not hidden)

With per-op sessions, async frames arriving **during** a bolus — `0x8A` pump-stop-rpt, `0x98` pulse-finish, and
**alarms** — are dropped (no session open to catch them). Parity mostly holds (legacy ignores 0x98;
`getPumpStatus` reconciles reservoir), but **alarms raised during a bolus** are the real gap. Closing it needs
the long-lived `unsolicitedEvents → CarelevoPatch.handleAlarm` bridge, which belongs to **D4**. Pull forward
only if required.

The extended-bolus `0x9C` delay report is likewise unsolicited and currently ignored by both stacks — out of
scope unless a consumer is ever added.

---

## Byte / persistence reference (parity contract)

Wire encodings (all verified byte-identical to legacy in the command unit tests):

- `0x24` bolus start: `[0x24, actionId, whole, centi]` → `0x84` `[.., result, expSec=[3]*60+[4], remains=[5]*100+[6]+[7]/100]`
- `0x2C` bolus cancel: `[0x2C]` → `0x8C` `[.., result, infused=[2]+[3]/100]` (legacy `insulinRemains=0.0` fabricated — dropped)
- `0x25` extended: `[0x25, immWhole, immCenti, spdWhole, spdCenti, hour, min]` → `0x85` `[.., result, expSec=[2]*60+[3]]`
- `0x29` extended cancel: `[0x29]` → `0x89` `[.., result, infused=[2]+[3]/100]`
- `0x23` temp basal byUnit: `[0x23, whole, centi, hour, min, 0x00]`; byPercent: `[0x23, whole, centi, hour, min]` (value=`percent/100`) → `0x83` `[.., result]`
- `0x2D` temp basal cancel: `[0x2D]` → `0x8D` `[.., result]`

Persistence per op (must stay like-for-like):

- **CareLevo-local Room** (inside use cases): temp basal → `updateTempBasalInfusionInfo`(mode=2)+`updatePatchInfo`(mode=2);
  extended → `updateExtendBolusInfusionInfo`(mode=5)+patch mode=5; imme start → `updateImmeBolusInfusionInfo`(mode=3,
  `infusionDurationSeconds=expSec`)+patch mode=3+`bolusActionSeq`; every cancel/finish → delete the row +
  **recompute** patch mode (extend=5 / imme=3 / tempBasal=2 / basal running=1 / stop=0).
- **AAPS main DB** (in coordinators, `runBlocking`): `syncTemporaryBasalWithPumpId` / `syncStopTemporaryBasalWithPumpId`;
  `syncExtendedBolusWithPumpId` / `syncStopExtendedBolusWithPumpId`; `syncBolusWithPumpId(insulin)` on complete /
  `syncBolusWithPumpId(infusedAmount, NORMAL)` on stop. `pumpId = timestamp = dateUtil.now()` (synthetic — keep).

---

## Risk register

- **Two-GATT / status-133:** any delivery session must first drop the legacy link + suppress reconnect
  (`stopReconnection` clears the single-flight `AtomicBoolean`) and never overlap another session → the mutex.
- **Cancel latency regression (Option A):** re-base retries on a wall-clock deadline; device-validate stop.
- **Double-record hazard (bolus):** preserve the `isStopPressed`/`isImmeBolusStop` ordering (no pump-id de-dup).
- **`immediateDose` must be 0.0 (extended):** else an unintended upfront dose.
- **Client-mirror fragility:** don't move/relocate `BolusProgressData.updateProgress` calls.
- **Silent-no-op behavior change** on a disconnected patch (temp basal) — decide fail-vs-no-op explicitly.
- **Byte asymmetry (temp basal unit vs percent):** already correct in `TempBasalCommand`; don't collapse the
  two shapes into one path.
- **BleClient single-in-flight mutex:** a delivery op must not re-enter `request()` while holding a slot; the
  per-op sessions + connectionless sim loop avoid this by construction.

---

## Device-validation checklist (physical patch, flag on)

- D1: set TBR absolute + percent + cancel → verify `newBle.*` logs `resultCode=0`, treatment-DB TBR rows, and
  the running-TBR chip.
- D2: set extended bolus + cancel → extended-bolus record + `infusedAmount` on cancel.
- D3: manual bolus → progress bar animates (synthetic), treatment recorded on completion; **stop mid-bolus** →
  measure stop-to-halt latency, partial `infusedAmount` recorded, no double record; SMB path unaffected.
- Regression: confirm the legacy path is untouched with the flag **off**.
