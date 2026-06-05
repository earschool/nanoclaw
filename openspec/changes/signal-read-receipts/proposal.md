## Why

Signal users see no read indicator when the bot consumes their message. The conversation feels one-sided — the sender does not know whether the message landed or was processed. signal-cli supports both `read` and `viewed` receipt types via the `sendReceipt` JSON-RPC method, but the NanoClaw adapter currently sends neither outbound nor handles inbound peer receipts.

Typing indicators (already wired) help bridge the gap during processing, but the final "the bot has seen this" acknowledgment is missing. Adding it brings Signal parity with the human-to-human Signal UX users expect, at zero protocol risk (signal-cli already speaks the wire format).

## What Changes

- **Outbound read receipts.** When the host detects that the consuming container has completed processing of an inbound Signal message (transition observed in `syncProcessingAcks` against the per-session `processing_ack` table in `outbound.db`), the host dispatches to the Signal adapter, which emits a `sendReceipt` JSON-RPC of type `read` back to the sender.
- **Inbound `receiptMessage` envelopes.** Peer read/viewed/delivery confirmations are recognized in the envelope dispatcher in `src/channels/signal.ts` and logged at `debug` level. DB-side storage (a future `messages_out.read_at` or peer-receipt log table) is deferred.
- **Per-group config flag** `signal.readReceipts` (boolean, default `true`) so users can opt out via `ncl groups config update`. Persisted in a **new `channel_settings` JSON column** on `container_configs` (DB migration required — see Impact).
- **Runtime signal-cli capability detection.** Adapter calls `version` RPC at startup; if the daemon is < 0.13.0 (or the call fails), outbound receipt wiring is disabled for that session and a warning is logged. No version pin in the install skill — the runtime gate is the source of truth.

Out of scope (deferred): outbound `viewed` receipts for media attachments, group-chat receipts, persistent storage of peer receipts on `messages_out`, retroactive receipts for messages received before the feature was enabled.

## Capabilities

### New Capabilities
- `signal-channel`: outbound + inbound read receipt semantics for the Signal native channel adapter, plus runtime daemon-capability detection

### Modified Capabilities
(None — no prior `signal-channel` spec exists. The Signal adapter was installed via `/add-signal` without an accompanying OpenSpec capability spec.)

## Impact

- **Code**:
  - `src/channels/signal.ts` — new `sendReadReceipt` helper, envelope dispatcher branch for `receiptMessage`, version detection at adapter `start()`, registration of an ack callback with the new dispatch hook
  - `src/db/session-db.ts` — `syncProcessingAcks` returns the set of message IDs that newly transitioned (currently returns nothing; we cannot dispatch what we cannot detect)
  - `src/host-sweep.ts` — after sync, look up `(channel_type, platform_id)` from `messages_in` for each transitioned ID and dispatch to the right channel adapter via the new ack hook
  - `src/channels/channel-registry.ts` (or sibling) — add an `onMessageCompleted(messageId, channelType, platformId, originalId)` callback registry so channels can react to host-observed completions without polling
  - `src/db/container-configs.ts` — accessor for `channel_settings` JSON column (read/write); type defs in `src/types.ts`
- **DB migration (required)**:
  - `src/db/migrations/<next-id>-channel-settings.sql`: `ALTER TABLE container_configs ADD COLUMN channel_settings TEXT NOT NULL DEFAULT '{}'`
  - Reading code resolves missing keys to defaults (`{ signal: { readReceipts: true } }`) at access time, so existing rows pre-migration behave correctly
- **No container-side DB changes**: the new dispatch hook fires host-side, after the container's `processing_ack` write has been observed by the sweep. Outbound receipt RPC goes directly to signal-cli, not through the per-session DBs.
- **Tests**: New unit cases in `src/channels/signal.test.ts` (outbound helper, gating, group exclusion, version detection, inbound envelope logging). New host-side cases in `src/host-sweep.test.ts` (sync returns transitions; dispatch fires once per completion). New DB cases in `src/db/container-configs.test.ts` (channel_settings roundtrip + defaults). Container-side: none — feature is host-side.
- **Docs**: New `docs/signal.md` (existing docs convention is flat under `docs/`, not under `docs/channels/`). One-line note in `/add-signal` skill setup output.
- **Dependencies**: No new packages. No version pin added to the `/add-signal` skill — runtime detection handles older daemons.
- **Breaking change**: None — additive feature with safe defaults. Default-on receipts are visible to users on upgrade; documented in release notes.
