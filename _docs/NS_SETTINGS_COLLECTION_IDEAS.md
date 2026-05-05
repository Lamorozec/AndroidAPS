# NS APIv3 `settings` collection — AAPS integration

The Nightscout APIv3 `settings` collection is a free-form per-app document
store that AAPS uses to sync running configuration and (planned) coordination
state between a master AAPS instance and its clients. This document is the
specification — wire format, modules, semantics, and the roadmap.

## Roadmap at a glance

| Phase | Scope                                                             | State    |
|-------|-------------------------------------------------------------------|----------|
| 1     | Running configuration master→client sync                          | shipped  |
| 2a    | Active scene state (master-published, with scoped record NS ids)  | shipped  |
| 2b    | Pairing infrastructure + client→master command channel + commands | planned  |
| 3     | Shared editable configs (scenes / automation / TT presets)        | planned  |
| 4     | Backup / restore                                                  | planned  |
| 5     | Insulin configuration editing (gated)                             | optional |

## Collection semantics

Per-app document keyed by an `identifier`. Schema is free-form JSON.

- **Authorization.** READ-by-id, CREATE, PATCH, UPDATE, DELETE follow the
  standard `:read` / `:create` / `:update` / `:delete` scopes. SEARCH, HISTORY,
  and the WS `settings` channel require `api:settings:admin`. Tokens without
  the admin role silently lose WS push but retain catch-up GET.
- **Validation.** `validateCommon` requires `date` (number > 946684800000),
  `utcOffset` (number in [-1440, 1440]), `app` (non-empty string) on
  UPDATE / CREATE. All three are immutable after first create — the publisher
  sends fixed constants because the doc is a snapshot, not a timestamped event.
- **Sync.** Only `srvModified` is honored. No `created_at` / `date` fallback.
- **Dedup.** On `identifier` only. Two CREATEs with the same identifier — the
  second becomes a replace. PATCH only updates existing docs (404 on first
  call); UPDATE (PUT) is the upsert primitive.
- **Auto-prune.** If the server enables `API3_AUTOPRUNE_SETTINGS`, docs expire
  by `srvCreated`. PATCH does not refresh `srvCreated`. UPDATE refreshes it.
  Active users get free refresh on every pref change; idle users need an
  explicit re-PUT (planned, not yet implemented).

---

## Phase 1: Running configuration sync (shipped)

Master writes a single `settings/aaps` document on every relevant pref change;
clients read it on connect and on WS push. Replaces the legacy piggyback on
`devicestatus.configuration`.

### Wire format

Identifier: `aaps` — single document per NS instance.

```
{
  identifier:    "aaps",                  // server-managed
  srvCreated, srvModified,                // server-managed
  date:          946684800001,            // fixed constant; immutable after create
  utcOffset:     0,                       //         "
  app:           "AAPS",                  //         "
  schemaVersion: 1,
  runningConfig: {
    insulin, insulinConfiguration,
    aps, apsConfiguration,
    sensitivity, sensitivityConfiguration,
    smoothing,
    safetyConfiguration,
    quickWizardConfiguration,
    scenesConfiguration,
    automationConfiguration,
    overviewConfiguration: {              // free-floating keys (no domain owner)
      units, low_mark, high_mark,
      statuslights_*,
      boluswizard_percentage,
      temp_target_presets,
      used_autosens_on_main_phone
    },
    pump, version
  }
}
```

### Block ownership

Each `*Configuration` block is owned by exactly one `ConfigExportImport`
implementer. The implementer declares the prefs it owns (`syncedKeys`) and how
to refresh its in-memory cache (`reloadInternalState`).

| Block                      | Owner                       | Cache refresh on apply                        |
|----------------------------|-----------------------------|-----------------------------------------------|
| `insulinConfiguration`     | `InsulinImpl`               | `loadSettings()`                              |
| `apsConfiguration`         | active `APS` plugin         | none                                          |
| `sensitivityConfiguration` | active `Sensitivity` plugin | none                                          |
| `safetyConfiguration`      | `SafetyPlugin`              | none                                          |
| `quickWizardConfiguration` | `QuickWizard`               | `setData(...)`                                |
| `scenesConfiguration`      | `SceneRepository`           | none — `StateFlow`-backed                     |
| `automationConfiguration`  | `AutomationPlugin`          | `loadFromSP()` + `EventAutomationDataChanged` |
| `overviewConfiguration`    | none (free-floating)        | none — values read fresh                      |

### Architecture

- `core/interfaces/configuration/ConfigExportImport.kt` — interface declaring
  `val syncedKeys: List<NonPreferenceKey>` and `fun reloadInternalState()`.
  Each plugin that contributes to the running config implements it.
- `core/interfaces/configuration/RunningConfigurationKeys.kt` —
  `fun observableKeys(): List<NonPreferenceKey>`. Returns the union across all
  installed Insulin / APS / Sensitivity / Safety / QuickWizard / Scenes /
  Automation owners plus free-floating overview keys. Implemented by
  `RunningConfigurationImpl`.
- `core/nssdk/localmodel/configuration/NSRunningConfiguration.kt` — typed
  payload of the `runningConfig` block.
- `plugins/configuration/configBuilder/RunningConfigurationImpl.kt` — single
  source of truth for serialization and dispatch.

### Master flow (`config.APS`)

1. `RunningConfigurationPublisher.start(scope)` from `NSClientV3Plugin.onStart()`.
2. Initial publish on start. Then merges per-key
   `preferences.observeChange(...)` for every key in
   `RunningConfigurationKeys.observableKeys()` plus
   `rxBus.toFlow(EventConfigBuilderChange)` (catches plugin-selection changes
   that aren't pref-driven).
3. Debounces 5 s, builds
   `{ schemaVersion, runningConfig, date, utcOffset, app }` and PUTs via
   `nsAndroidClient.updateSettings("aaps", body)`.
4. Logs `► SETTINGS aaps srvModified=<human-readable>` on success or
   `✕ SETTINGS aaps HTTP <code> <body>` on failure.

### Client flow (`config.AAPSCLIENT`)

- **Catch-up GET.** `LoadSettingsWorker` runs in the standard `executeLoop`
  chain on every reconnect. GETs `settings/aaps`, parses, calls
  `runningConfiguration.apply(NSRunningConfiguration)`, advances
  `lastLoadedSrvModified.collections.settings`. `srvModified` is read from the
  doc body (`doc.optLong("srvModified")`); ETag header is used as fallback
  because NS doesn't always set it for settings.
- **Real-time WS.** `NSClientV3Service` settings branch parses incoming
  `create` / `update` events and applies through the same path. Subscription
  requires `api:settings:admin`; without it only catch-up GET runs.

---

## Phase 2: Active scene + command channel

State propagation remains one-way (master → client) via `settings/aaps`. A new
client → master direction is added for **command intent only** — clients never
write state directly. Master is the single authoritative writer for state;
client commands are processed by master, applied locally, and propagated back
through the existing settings doc.

### 2a — Active scene state (master-published) — shipped

Master publishes the currently-running scene as a new block of `runningConfig`,
carrying scene timing and the **NS identifiers** of records the scene created
(but not the master's local Room ids or the master-only `priorSmb` revert flag):

```
runningConfig.activeScene: {
  sceneId:     "<scene definition id>",
  activatedAt: <epoch ms>,
  durationMs:  <number>,
  ttNsId:      <string|null>,    // NS id of the scene's TempTarget
  psNsId:      <string|null>,    // NS id of the scene's ProfileSwitch
  rmNsId:      <string|null>,    // NS id of the scene's RunningMode
  teNsId:      <string|null>     // NS id of the scene's TherapyEvent
} | null
```

- Owner: `ActiveSceneManager` (in `ui/`) implements `ActiveSceneSync` and
  exposes a wire snapshot. The runtime state model is split so the misnomer is
  gone: `ActiveSceneState.priorSmb` (the only truly prior value, master-only)
  is separate from `ActiveSceneState.ScopedRecords` (records the scene
  created, carried as Room id + NS id pairs).
- NS-id ↔ Room-id reconciliation is symmetric: both master and client subscribe
  to `persistenceLayer.observeChanges<TT|PS|RM|TE>()` and fill in whichever
  half of the pair they don't yet have (master gains the NS id once the
  record is uploaded; client gains the Room id once the matching record
  syncs in). Either side may briefly show the scene name without chips
  while the other half is in flight.
- Client UI renders "Scene X running, expires HH:MM" plus the existing
  scene-managed chips (TT/profile/loop-mode), driven by `ScopedRecords.*Id`.
- A `null` block means no scene is active; client clears.

### 2b — Command channel — planned

New identifier class: `aaps-cmd-<uuid>` — **one doc per command**, generated
client-side per request. Master observes, processes, then DELETEs.

#### Wire format

```
{
  identifier:    "aaps-cmd-<uuid>",       // server-managed
  date, utcOffset, app: "AAPS",
  schemaVersion: 1,

  command:       "scene.stop" | "scene.start" | ...,
  payload:       { sceneId?: "..." },     // command-specific
  clientId:      "<paired-client uuid>",
  counter:       <number>,                // monotonically increasing per client
  requestedAt:   <epoch ms>,
  sig:           "<hex HMAC-SHA256>"
}
```

#### Lifecycle

1. Client builds command, signs, PUTs `aaps-cmd-<uuid>`.
2. Master observes via WS settings branch (filter on identifier prefix
   `aaps-cmd-`).
3. Master verifies (see §"Pairing & signing"); if valid, dispatches to the
   command handler, then DELETEs the doc.
4. Invalid commands are DELETEd silently — no leakage of which check failed.
5. Master applies state change locally; the existing `settings/aaps` publisher
   broadcasts the updated state.
6. Client UI updates from the `settings/aaps` push, not from a direct response
   to the command. Optimistic UI on the issuing client with a 10 s timeout
   fallback.

#### Master subscription

Phase 1 master only **writes** to settings. For 2b master must also subscribe
to the WS settings channel and route incoming `create` events whose identifier
matches `aaps-cmd-*` to the command pipeline. `aaps`-identifier writes are
ignored on master.

### 2c — Pairing & per-command authentication — planned

NS auth alone provides a token-gated channel but no client identity, no replay
protection, and no tamper detection beyond TLS. Per-client signing is layered
on top: pair devices once, sign every command. Secrets never touch NS — only
signatures do.

#### Master gate

NSCv3 settings, under "Use websockets":

- New toggle **"Allow client control"** (default OFF).
- When OFF: incoming `aaps-cmd-*` docs are DELETEd on receipt without
  execution. A single user-facing notification surfaces the rejection with a
  link to the toggle.
- When ON: command pipeline is active; signed commands from paired clients
  are processed.

#### Pairing flow

One-time per client.

1. Master: Authorized clients screen → FAB **+** → enter client name.
2. Master generates 32-byte random `secret` and a `clientId` (UUID); stores
   `{ clientId, secret, label, pairedAt: now, lastCounter: 0,
   state: pending, capabilities: [...all currently supported commands] }`.
3. Master displays QR carrying
   `{ masterInstallId, clientId, secret, capabilities, qrExpiresAt }`.
    - QR validity: 2–3 minutes with countdown.
    - Screen uses `FLAG_SECURE`; QR is blurred until "Show QR" is tapped and
      auto-blurs after 30 s.
    - Cancel button drops the pending entry.
4. Client: "Pair with master" → camera scanner. After scan, client shows a
   confirmation screen with master id, label, allowed capabilities; user taps
   **Pair** to confirm (no silent acceptance of scanned data).
5. Client stores `{ masterInstallId, clientId, secret, capabilities }` in
   encrypted prefs.
6. Client immediately sends a signed `hello` command (counter = 1).
7. Master verifies; on success flips entry to `state: active` and shows
   "Connected ✓".
8. If no `hello` is received within `qrExpiresAt`, master drops the pending
   entry.

#### Per-command signing

Canonical signing input (exact order):

```
"$clientId|$counter|$requestedAt|$command|<canonical JSON of payload>"
```

`sig = HMAC-SHA256(secret, signingInput)` (hex-encoded).

Master accepts iff:

- `clientId` exists in the paired-clients table and entry is `active`.
- `counter > lastCounter` for that client → after acceptance bump `lastCounter`.
- `|now − requestedAt| ≤ 5 min` (clock skew tolerance).
- `command ∈ capabilities` for that client.
- Recomputed `sig` matches.

All checks fail closed: invalid commands are DELETEd silently.

#### Revocation

Master Authorized clients screen → delete entry → all future commands from
that `clientId` fail signature verification. No CRL needed because master is
the only verifier.

#### Storage

- **Master:** paired-clients table in encrypted prefs. **Excluded** from local
  AAPS backup (contains secrets) and from NS settings sync (master-private).
- **Client:** master pairing record in encrypted prefs. Excluded from backup.

#### Failure modes

- **Master reinstall** wipes the paired-clients table → all clients become
  orphans, every command is rejected. Client tracks consecutive rejections;
  after N (suggest 3) it surfaces "Pairing seems broken — re-pair?".
- **Backup restore on either side** does not restore secrets. User re-pairs.
- **Already-paired client scans a different QR** — confirm replace, not silent
  overwrite. v1 = one paired master per client.

### UI

**Master:**

- NSCv3 settings → "Use websockets" → **Allow client control** (default OFF).
- Management bottom sheet → new entry **Authorized clients** (NS icon).
- Authorized clients screen:
    - Header reflects toggle state (e.g., "Disabled — commands ignored" when OFF).
    - List rows: name • state badge (`pending` / `active` / `disabled` / `stale`)
      • paired on • last seen • last action.
    - Per-row delete with confirmation.
    - FAB **+** → name prompt → QR display.
    - Empty-state copy: "No paired clients yet — tap + to pair one."

**Client:**

- Settings entry **Pair with master** → camera scanner → confirmation →
  paired status (which master, when, **Unpair** button).

### Implementation order

1. **2a — display only.** No new client → master path. Validates round-trip on
   active-scene fields.
2. **Pairing infrastructure** (master toggle, Authorized clients screen, QR,
   client scanner, encrypted-prefs storage, `hello` verification).
3. **2b — command channel** atop pairing (master WS subscription, generic
   dispatch, signature verification, DELETE-on-ack).
4. **First commands:** `scene.stop`, then `scene.start`.

### Out of scope for v1

- Per-client capability scoping (all paired clients have the full command set
  the master version supports).
- Multi-master per client.
- ECDSA keypairs (HMAC is sufficient for the threat model).

---

## Phase 3: Shared editable configs (planned)

Multi-writer surface for editable lists: scene definitions, automations, TT
presets. Insulin configurations are explicitly excluded (see phase 5).

### Concurrency model

NS APIv3 has no server-side If-Match or version-checked PATCH; concurrency is
app-level optimistic.

1. Client reads doc, captures `srvModified = X`.
2. User edits.
3. Client re-reads `srvModified` immediately before PATCHing. If it has
   advanced, prompt "this was changed on another device — reload?" rather
   than silently overwriting.
4. PATCH body includes `__expectedSrvModified: X` plus the actual changes.
5. Client captures the new `srvModified` from the response.

Audit trail: every cross-device edit also writes a `Note` treatment with
`{ enteredBy: "AAPSClient/<installId>", notes: "Edited scene 'Exercise'" }`.

### Trust model

Most clients can read everything; only some can edit. Phase 2 pairing is the
foundation: paired clients are the candidates for the edit allowlist. Server
does not enforce — the gate is in-app on the client (untrusted clients refuse
to expose edit UI). Threat model is "wrong family member opens AAPSClient",
not adversarial.

---

## Phase 4: Backup / restore (planned)

Independent of phases 1–3.

- Identifier class: `aaps-backup-<installId>` — one doc per install,
  exclusive owner.
- Reuses the existing `ImportExportPrefs` serializer with secrets filtered out
  (master password, paired pump MACs, NS tokens, paired-clients table).
- Daily upload + manual "Backup now" button.
- Restore is always user-triggered. SEARCH lists existing backups, user picks,
  diff against current prefs, confirm. Per-section opt-out during restore.

---

## Phase 5 (optional): Insulin configuration editing

Gated behind a stricter allowlist than phase 3 — `criticalConfigClients`,
opted into per-client by an explicit confirmation on the master phone. PATCH
path same as phase 3 with additional confirmation UI on commit.

---

## Identifier strategy

| Identifier                   | Owner                           | Phase | Purpose                                                  |
|------------------------------|---------------------------------|-------|----------------------------------------------------------|
| `aaps`                       | master                          | 1     | Running configuration                                    |
| `aaps-cmd-<uuid>`            | client (signed) → master DELETE | 2     | One-shot command from client to master                   |
| `aaps-cfg-<masterInstallId>` | master                          | 3     | Per-master config when single `aaps` is no longer enough |
| `aaps-backup-<installId>`    | install (exclusive)             | 4     | Per-install backup                                       |

`<installId>` / `<masterInstallId>` is a UUID generated on first launch and
persisted in prefs. Stable across app updates; not re-generated on restore.

---

## Out of scope (deliberately)

- **Insulin config edits without trust allowlist.** Phase 5 only.
- **Server-side enforcement of write permissions.** NS has no per-section
  ACLs; trust is in-app and assumes the client app respects its own rules.
- **Live loop telemetry on settings.** IOB / COB / current temp basal / loop
  status stay on devicestatus.
- **Profile (basal / ISF / ICR) sync.** Stays on the `profile` collection.
- **Per-section ACLs on the wire.** Doc is not partitioned on the server.
