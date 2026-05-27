## 1. DB migration + config accessor

- [x] 1.1 Add `src/db/migrations/<next-id>-channel-settings.sql`: `ALTER TABLE container_configs ADD COLUMN channel_settings TEXT NOT NULL DEFAULT '{}';` Idempotent (migration framework already tracks applied IDs).
- [x] 1.2 Define `ChannelSettings` type in `src/types.ts` (or sibling): `{ signal?: { readReceipts?: boolean } }`. Add `channel_settings: string` to `ContainerConfigRow`.
- [x] 1.3 Extend `src/db/container-configs.ts` with `getChannelSettings(agentGroupId): ChannelSettings` that parses the JSON, returns `{}` on missing/invalid, and merges with hardcoded defaults `{ signal: { readReceipts: true } }`.
- [x] 1.4 Add `setChannelSettings(agentGroupId, settings: ChannelSettings)` writer mirroring the existing JSON-column pattern.
- [x] 1.5 Add `'channel_settings'` to `JSON_COLUMNS` set so the generic `updateContainerConfigJson` accepts it (for `ncl groups config update --json '{"channel_settings":{...}}'`).
- [x] 1.6 Unit tests in `src/db/container-configs.test.ts` (create if missing, ≤300 lines):
  - Roundtrip: write `{signal:{readReceipts:false}}`, read back equal.
  - Default fill: empty column resolves to `{signal:{readReceipts:true}}`.
  - Invalid JSON: malformed column resolves to defaults, logs warn.
  - Migration idempotency: running migration twice does not error.

## 2. Host-side dispatch: completion-hook infrastructure

- [x] 2.1 Update `syncProcessingAcks(inDb, outDb)` in `src/db/session-db.ts` to return `string[]` — the IDs whose `messages_in.status` actually transitioned from non-completed to completed in this call. Use the `UPDATE ... WHERE status != 'completed'` row-changes count to filter. Keep the primary side-effect identical.
- [x] 2.2 Update host-sweep callers (`src/host-sweep.ts:171`) to receive the returned slice.
- [x] 2.3 Extend `src/channels/channel-registry.ts` (or sibling) to support an optional `onMessageCompleted` hook per channel type. Define `type CompletionHook = (msg: { id: string; platformId: string; threadId: string | null }) => void | Promise<void>`.
- [x] 2.4 Add `registerCompletionHook(channelType, hook)` and `dispatchCompletionHooks(rows)`. Internal `dispatchCompletionHooks` should `Promise.allSettled` and log any rejected hook results without throwing.
- [x] 2.5 In `src/host-sweep.ts`, after `syncProcessingAcks` returns transitioned IDs: for each id, read `(channel_type, platform_id, thread_id)` from `messages_in` and call `dispatchCompletionHooks` once. Skip ids where `channel_type` is null (system messages, a2a).
- [x] 2.6 Tests in `src/host-sweep.test.ts` (or new `completion-dispatch.test.ts`, ≤300 lines):
  - Behavioral: when a `processing_ack` row exists with status `'completed'` AND `messages_in.status` was `'processing'`, the hook for that channel_type is called exactly once with correct `{id, platformId, threadId}`.
  - Behavioral: `status='failed'` does NOT fire the completion hook.
  - Behavioral: re-running the sweep does NOT re-fire the hook (idempotency).
  - Unit: a thrown hook does not affect other hooks' delivery; the rejection is logged.
  - Unit: null `channel_type` rows are skipped.

## 3. Signal adapter — outbound read receipts

- [x] 3.1 Add `sendReadReceipt(recipient: string, targetTimestamp: number): Promise<void>` helper in `src/channels/signal.ts` calling `tcp.rpc('sendReceipt', { recipient, targetTimestamps: [targetTimestamp], type: 'read' })`. Wrap in try/catch — log on failure, flip cached `receiptsSupported = false` if the daemon returns method-not-found.
- [x] 3.2 In adapter `start()`, call `tcp.rpc('version', {})`, parse the returned version string, set `receiptsSupported = (version >= '0.13.0')`. Honor env var `SIGNAL_FORCE_RECEIPTS_DISABLED=1` as a hard off. Log the resolved value at info level.
- [x] 3.3 Register a `CompletionHook` for `channel_type='signal'` via the new registry. Hook implementation:
  - Return early if `receiptsSupported === false`.
  - Return early if `platformId` starts with `group:` (group inbound).
  - Parse `msg.id` to integer — return early if NaN or `id` was the `Date.now()` fallback (heuristic: compare against `messages_in` original-timestamp column if added, else accept any integer in Signal's normal range; explicit allow-NaN-reject is enough for v1).
  - Look up the agent group's `signal.readReceipts` via `getChannelSettings(agentGroupId)`. Return early if false. **Note**: hook receives `(id, platformId, threadId)` but not `agentGroupId` directly. Either extend the hook payload to include `agentGroupId` (cleanest) or derive it via `sessionsByMessageId` lookup. Pick whichever is closest to existing host-sweep state.
  - Call `sendReadReceipt(platformId, parseInt(msg.id, 10))`.
- [x] 3.4 Tests in `src/channels/signal.test.ts` (or split into `signal-receipts.test.ts` if file approaches 300 lines):
  - Unit: `sendReadReceipt` calls `tcp.rpc` with exact shape `('sendReceipt', { recipient, targetTimestamps: [n], type: 'read' })`.
  - Behavioral: completion-hook with default config + receipts-supported fires `sendReceipt` exactly once.
  - Behavioral: `readReceipts: false` → no RPC issued.
  - Behavioral: `platformId='group:abc'` → no RPC issued regardless of config.
  - Behavioral: `receiptsSupported=false` (mock version RPC returning `0.12.5`) → no RPC issued.
  - Behavioral: `SIGNAL_FORCE_RECEIPTS_DISABLED=1` → no RPC issued even with new daemon.
  - Unit: `sendReadReceipt` swallows RPC errors and logs; subsequent calls do not throw.

## 4. Signal adapter — inbound `receiptMessage` handling

- [x] 4.1 In the envelope dispatcher in `src/channels/signal.ts`, add a branch for `envelope.receiptMessage`. Read `type` (`read | viewed | delivery`) and `timestamps` (array).
- [x] 4.2 Log at `debug`: `{ direction: 'inbound-receipt', sender, type, timestamps }`. No DB write. No further action.
- [x] 4.3 Ensure non-`dataMessage` envelopes still flow through whatever existing fallthrough exists (don't accidentally swallow `editMessage`, `typingMessage`, etc.).
- [x] 4.4 Tests in `src/channels/signal.test.ts`:
  - Unit: feed a synthesized `receiptMessage` envelope, assert logger called with expected payload, assert no inbound emission to `setup.onInbound`, assert no DB write.
  - Unit: feed all three receipt types — each logs distinctly.
  - Unit: a `dataMessage` envelope continues to flow through normally (regression).

## 5. Docs + skill setup output

- [x] 5.1 Create `docs/signal.md` with sections: Overview (link to add-signal skill), Read receipts (when they fire, default value, how to disable, version requirement), Limitations (no group receipts in v1, no viewed receipts in v1).
- [x] 5.2 Add a single-line note to `.claude/skills/add-signal/SKILL.md` "After install" section: "Read receipts are on by default for DMs. Disable per agent group with `ncl groups config update --json '{\"channel_settings\":{\"signal\":{\"readReceipts\":false}}}'`."

## 6. Validation

- [x] 6.1 Host unit + integration: `pnpm test` green; new tests included. (88/88 in receipts-related test files; 418/423 overall — 5 unrelated pre-existing failures in `scripts/q.test.ts`.)
- [x] 6.2 Container unit (no changes expected): `cd container/agent-runner && bun test` green. (No container changes in this feature.)
- [ ] 6.3 Manual DM happy path: from `+12053702232` → bot `+15103942741`, send "ping receipts". Observe single → double check in sender's Signal client after agent acks.
- [ ] 6.4 Manual opt-out: run `ncl groups config update --id <main> --json '{"channel_settings":{"signal":{"readReceipts":false}}}'`, restart group, send DM, verify sender stays on single check.
- [ ] 6.5 Manual group exclusion: bot in a Signal group, send group message, verify no receipt fires.
- [ ] 6.6 Manual inbound: from peer device mark bot outbound as read, verify `logs/nanoclaw.log` contains `inbound-receipt` debug line.
- [ ] 6.7 Manual older daemon (optional): start adapter against signal-cli 0.12.x, verify warning log and receipts off; rest of adapter functions normally.
