## Context

`src/channels/signal.ts` is a native adapter (no Chat SDK bridge) that connects to a local signal-cli JSON-RPC daemon over TCP. It currently issues four outbound RPC methods: `send`, `updateProfile`, `updateConfiguration`, `sendTyping`. Inbound envelopes are consumed via signal-cli's `subscribeReceive` stream; the adapter handles `dataMessage` (text + attachments) and `editMessage`, but ignores `receiptMessage` and `typingMessage`.

Each session has two SQLite files. Container-owned `outbound.db` contains a `processing_ack` table (`message_id`, `status` in `{'processing','completed','failed'}`, `status_changed`). The host-side sweep loop (`src/host-sweep.ts:169`) calls `syncProcessingAcks(inDb, outDb)` once per tick to update `messages_in.status='completed'` in inbound.db for every `processing_ack` row with status in `('completed','failed')`. The function currently returns `void` — it does not surface which IDs transitioned in this tick.

The Signal `dataMessage.timestamp` (the protocol timestamp the receiver echoes back as `targetTimestamps[i]` in `sendReceipt`) is preserved as `messages_in.id` (set to `String(dataMessage.timestamp ?? Date.now())` in `signal.ts:887`). The id is recoverable without additional storage, with one caveat: when the inbound envelope had no `timestamp` field, the id is `Date.now()` — not a valid Signal protocol timestamp. The implementation must detect and skip receipt firing in that edge case.

Users expect read indicators in personal messaging. A bot that goes silent after the typing indicator stops is indistinguishable from one that crashed. Typing indicators (`signal.ts:1213`) bridge the active-processing window, but acknowledgment at the end of consumption is missing. Adding `read` receipts at the natural acknowledgment point — host observing the `processing_ack` transition — closes that gap.

signal-cli's `sendReceipt` JSON-RPC method shipped in 0.13.0. Wire format: `{ recipient, targetTimestamps: [number], type: 'read' | 'viewed' | 'delivery' }`. The `/add-signal` skill installs `latest` from GitHub releases — there is no version pin to update. The runtime version-detection path is therefore the only durable safety net.

## Goals / Non-Goals

**Goals:**
- Send a `read` receipt for every consumed 1:1 inbound text message, gated by per-group config (default on).
- Recognize peer `receiptMessage` envelopes so the delivery layer can be extended later without re-touching the dispatcher.
- Keep the change additive — existing message flow is unchanged on the happy path.
- Reuse the existing TCP RPC infrastructure (no new connections, no new daemon flags).
- Degrade gracefully when signal-cli is < 0.13.0 (warn + disable receipts; do not crash the adapter).
- Make the channel-completion dispatch hook generic enough that other adapters can later plug in their own ack semantics (WhatsApp blue-double-check, Telegram, etc.) without re-architecting.

**Non-Goals:**
- E2E privacy posture changes — sealed-sender behavior is whatever signal-cli's default is. Not touching it.
- Group-chat receipts — multi-recipient delivery state has different semantics; separate change.
- Outbound `viewed` receipts for media — requires a heuristic for when the agent surfaced the attachment. Deferred.
- Persisting peer receipts to `messages_out.read_at` — DB migration with no current consumer. Deferred.
- Cross-channel parity in this change — just Signal. Other adapters can adopt the new hook later.
- Pinning signal-cli to a specific version in the install skill — runtime detection is preferred.

## Decisions

### 1. Where to detect "container finished processing"

Candidates:
- (a) Container writes to a new column directly observable from the channel adapter.
- (b) Extend `syncProcessingAcks` to return the set of message IDs whose `messages_in.status` just transitioned to `'completed'`, then dispatch in the host-sweep tick.
- (c) Add a new sibling table or pub/sub mechanism.

**Decision: (b).** Smallest change to existing flow. `syncProcessingAcks` already iterates the right rows; today it discards the IDs. Capturing them and returning the slice that actually went from non-completed to completed (via the `UPDATE ... WHERE status != 'completed'` returning count) requires one extra prepared-statement check per row. Host-sweep gets a list it can dispatch on.

Failed-status messages are excluded from receipt firing — "read" should only fire when work succeeded; failed processing is closer to an undelivered state.

### 2. How to dispatch from host-sweep to channel adapters

**Decision: extend `channel-registry.ts` with an optional `onMessageCompleted` hook.** Adapters register a callback at startup; host-sweep iterates transitioned IDs, looks up `(channel_type, platform_id)` from `messages_in`, calls the registered hook for that channel with the row's id and platform-id.

Generic shape:
```ts
type CompletionHook = (msg: { id: string; platformId: string; threadId: string | null }) => void | Promise<void>;
```

Signal adapter registers its hook in `start()` and uses the row id (which equals the Signal protocol timestamp string) as `targetTimestamp`. Other adapters can implement different semantics; the hook is opt-in (channels that don't register a hook are a no-op).

Errors thrown by the hook are caught at the dispatch site and logged — a failed receipt RPC must not break the sweep loop or block other channels' receipts.

### 3. Where to persist the `signal.readReceipts` flag

Candidates:
- (a) New JSON column `channel_settings` on `container_configs`.
- (b) New normalized table `channel_settings (agent_group_id, channel_type, settings_json)`.
- (c) Reuse `additional_mounts` JSON.
- (d) Per-thread on `messaging_group_agents` wiring metadata.

**Decision: (a) — `channel_settings` JSON column on `container_configs`.** Single migration, single accessor, consistent with the existing JSON-column pattern (`skills`, `mcp_servers`, etc.). Schema (in code, not DB):

```ts
type ChannelSettings = {
  signal?: { readReceipts?: boolean; /* room for more */ };
  // future: discord?: {...}; telegram?: {...};
};
```

Defaults resolved at read time: missing key → `{ signal: { readReceipts: true } }`. Existing rows pre-migration get an empty `'{}'` from the column default and resolve correctly.

(d) was rejected for v1: a per-thread switch implies different receipt behavior in different chats run by the same agent group, which is unusual UX for a personal-assistant bot. If the demand surfaces later, add a wiring-level override that takes precedence over the agent-group default.

### 4. Default value

**Decision: `true` (receipts on).** A personal-assistant bot in a 1:1 DM is acting on the user's behalf. The user installed it; consent is implicit. Power users flip the flag explicitly. Matches the typing-indicator default.

### 5. signal-cli version detection

**Decision: runtime check at adapter `start()` via `version` RPC.** No install-skill pin. Adapter caches a boolean `receiptsSupported` after the RPC returns; the completion-hook checks it before firing.

Behaviors:
- `version >= 0.13.0`: receipts wiring enabled.
- `version < 0.13.0`: warn log naming detected version + disabled feature; `receiptsSupported = false`; rest of adapter normal.
- RPC fails or response unparseable: warn log; `receiptsSupported = false` (safe default); rest of adapter normal.

Manual override knob for testing: env var `SIGNAL_FORCE_RECEIPTS_DISABLED=1` flips the boolean off even on supported daemons.

## Risks / Trade-offs

- **`syncProcessingAcks` return-value change ripples.** It's called from `host-sweep.ts` only, but the function is exported and other callers may appear in the future. Mitigation: keep the function's primary effect identical (UPDATE statement); just additionally return the affected ids. Existing call sites that ignore the return value continue to work.
- **Hook ordering and reentrancy.** Host-sweep runs once per session per tick. If a hook is slow (synchronous RPC + I/O wait), it can delay the rest of the tick for that session. Mitigation: hook is awaited but the dispatch site wraps each call in `Promise.allSettled` so one slow channel doesn't block another, and per-channel hook implementations should be fast or async-fire-and-forget.
- **Online-status leak via receipt timing.** Same exposure as the existing typing indicator and signal-cli's automatic `delivery` receipt. Marginal additional risk.
- **Group misfire.** `messages_in.platform_id` is set to `group:${groupId}` for group inbounds. The Signal completion-hook detects the prefix and skips. Test pinned.
- **Self-loop.** signal-cli's inbound stream filters self-messages already (the adapter relies on this for echo suppression). Pin via test.
- **Version drift.** Future signal-cli versions may rename or remove `sendReceipt`. Runtime detection only checks the version number, not method presence. Mitigation: catch RPC errors in `sendReadReceipt` and log + flip `receiptsSupported = false` on first failure. Self-healing per session.
- **`channel_settings` migration on existing installs.** ALTER TABLE adding a column with a DEFAULT is non-destructive in SQLite. Migration is idempotent (already-applied marker in `schema_version`). Risk minimal.
- **Hook on every completed message.** For high-volume agent groups, this is one extra map lookup + (for Signal) one extra RPC per consumed message. RPC is async fire-and-forget. No measurable perf hit expected.
- **Deferred peer-receipt storage.** Re-opening the change later when a consumer surfaces is accepted. Better to ship visible behavior first.
