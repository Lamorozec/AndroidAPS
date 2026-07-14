# Carelevo → CommandQueue lifecycle conversion

Goal: make the AAPS `CommandQueue` (`QueueWorker`) the **sole** owner of the Carelevo
connect → execute → disconnect lifecycle, with every patch op either a plain `Pump` override
(therapy) or a `commandQueue.customCommand(...)` routed through `CarelevoActivationExecutor`.
No bespoke background reconnect/keep-alive at the pump layer.

## Current op inventory

**Category A — plain `Pump` overrides, already queue-driven** (`CarelevoPumpPlugin.kt`): bolus,
stopBolus, setTempBasalAbsolute/Percent, cancelTempBasal, setExtendedBolus/cancel,
setNewBasalProfile,
getPumpStatus. ✅ nothing to do.

**Category B — `customCommand` via `CarelevoActivationExecutor`, already migrated**: safety check,
needle check, set-basal, additional priming, discard. ✅ template to reuse.

**Category C — direct BLE, off-queue (the 17 conversion targets):**

- Overview: `startPumpStopProcess` (suspend), `startPumpResume`, `refreshPatchInfusionInfo` (manual
  refresh).
- Alarm handler: resume-on-alarm, clear-alarm, clear-discard, force-quit (disconnect+unbond).
- Settings writes (`CarelevoSettingsCoordinator`): max-bolus, low-insulin notice, expiry threshold,
  buzzer, timezone/DST.
- **KEEP off-queue** (pre-activation / reactive, no queue-managed pump yet): BLE scan, pairing
  connect
  sequence, connectNewPatch, comm-check reconnect, cannula-insertion confirm (reactive observer).

## Why the lifecycle doesn't work today (critical)

Even though `disconnect()` now really drops the link, three low-level paths in
`CarelevoBleMangerImpl`
re-open it behind the queue's back:

- `connectGatt(autoConnect = true)` (`:348-354`) — OS re-dials in the background.
- status-22 GATT auto-reconnect (`:830-848`) — status 22 (`GATT_CONN_TERMINATE_LOCAL_HOST`) is
  exactly
  what a deliberate `disconnect()` produces → schedules `connectTo(address)` after 2 s.
- write-while-disconnected reconnect (`:460-468`).
  Plus `onStart` eager reconnect (`CarelevoPumpPlugin.kt:264-272`) and dead code (commented
  CONNECTING
  ticker, `EventForceStopConnecting` no-op + its `CarelevoBolusCoordinator` sender,
  `forceBleResetAndReconnect`).
  (Note: on-device the pump *did* stay idle-disconnected after the new disconnect() — these didn't
  spin in
  practice — but they are latent and must be neutralized for the queue to reliably own the
  lifecycle.)

## Reference contract (Dash / Medtrum) — the target shape

The `QueueWorker` (`implementation/.../queue/QueueWorker.kt:56-216`) is the ONLY lifecycle driver:
it
re-calls `connect("Connection needed")` each ~1 s until `isConnected()`, runs the command, then
after
`waitForDisconnectionInSeconds` (5) calls `disconnect("Queue empty")`. Reference drivers are thin:
`connect()` fires ONE attempt and returns; `disconnect()`/`stopConnecting()` really drop the link;
NO
self-reconnect. `isConnected()` returns **true when no patch is activated** so the queue never dials
a
missing device. `isSuspended()` = real delivery-suspend (`patchInfo.isStopped`), not connection
state
(already fixed for Carelevo). Patch-specific ops go through `customCommand`.

## Phased plan (each independently buildable + device-testable)

- **P0 — dead-code deletion** (0 behavior change): commented CONNECTING ticker (
  `CarelevoPatch.kt:253-265`);
  `EventForceStopConnecting` handler (`CarelevoPatch.kt:205-211`) + sender (
  `CarelevoBolusCoordinator.kt:133`);
  `forceBleResetAndReconnect` (`CarelevoBleMangerImpl.kt:1011-1025`, zero callers).
- **P1 — manual refresh → queue**: `refreshPatchInfusionInfo:966` → `commandQueue.readStatus(...)`.
- **P2 — remove onStart eager reconnect** (`CarelevoPumpPlugin.initializeOnStart` step 4,
  `:264-272`).
- **P3 — [CRITICAL] neutralize OS/low-level reconnect**: `autoConnect=true`→`false` (`:351`); delete
  the
  status-22 `stateScope.launch{delay;connectTo}` (keep `close()`+state emit); delete the
  write-reconnect
  launch (keep `clearGatt()`+FAILURE return); harden
  `CarelevoConnectionCoordinator.startReconnection` to
  BLOCK until `EnableNotifications` succeeds (currently fire-and-forget). **Must be device-proven
  before
  delivery ops.** Conservative fallback: gate the status-22/write-reconnect behind a "queue wants
  connected" flag.
- **P4 — suspend/resume → `CmdPumpStop`/`CmdPumpResume`**. Keep the temp/extended pre-cancel in the
  VM
  (already `commandQueue.cancelTempBasal/cancelExtended`) BEFORE enqueue — do NOT fold into the
  executor
  (re-entrant queue deadlock). pumpSync bookkeeping in the callback.
- **P5 — timezone/DST → `CmdTimeZoneUpdate`** (`timezoneOrDSTChanged:612`).
- **P6 — settings writes → `Cmd*`** (max-bolus/low-insulin/expiry/buzzer). Preserve the UseCases'
  `patchState` self-gate + `needSync` deferred-sync-on-next-connect; fall back to off-queue if
  per-toggle
  forced connects are too heavy.
- **P7 — alarm ops → `Cmd*`** (resume/clear/clear-discard). Fold the force-quit disconnect+unbond
  into
  `CmdAlarmClearDiscard`'s post-success step (worker thread, like `discardTeardown`); rely on
  `autoConnect=false`.
- **P8 — review/cleanup**: keep scan/pairing/connectNewPatch/comm-check/cannula-confirm off-queue;
  gate the
  alarm-screen direct connect (`CarelevoAlarmViewModel:313`); verify all Pump booleans are
  queue-accurate.

## Top risks

1. **Pump unreachable** after P3 if `startReconnection` stays flaky/fire-and-forget → harden to
   block until
   notifications; land P3 in one revertible phase; device-prove idle-disconnect→bolus + keep-alive
   reconnect + BT off/on.
2. **Re-entrant queue deadlock** — executors run on the single worker thread and must NEVER call
   `commandQueue.*`.
3. **Resume/suspend silently failing** through the queue — only do pumpSync/DB work on real
   `Success`; after P3 only.
4. **Hung worker** — every new blocking executor op needs a bounded `TimeUnit.SECONDS` timeout.
5. **Blocking a reactive observer** — keep cannula-confirm non-blocking.
6. **Force-quit is anti-queue** (wants disconnect+unbond) — fold into a discard command, don't make
   it a reconnecting command.
7. **Settings deferred-sync lost** — preserve `needSync` semantics; do last.

Sequence: **P0 → P1 → P2 → P3 (device-prove) → P4/P5/P7 → P6 → P8.**
