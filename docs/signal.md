# Signal Channel

NanoClaw's Signal channel is a native adapter that connects to a local [signal-cli](https://github.com/AsamK/signal-cli) JSON-RPC daemon over TCP. No Chat SDK bridge, no bot API — NanoClaw registers as a full Signal account on a dedicated phone number (or links as a secondary device on an existing one). Install with `/add-signal`.

## Overview

The adapter speaks signal-cli's `subscribeReceive` stream for inbound envelopes and its outbound RPC methods (`send`, `updateProfile`, `updateConfiguration`, `sendTyping`, `sendReceipt`) for everything written back to Signal. Inbound `dataMessage` and `editMessage` envelopes are routed into NanoClaw's normal message flow; inbound `receiptMessage` and `typingMessage` envelopes are recognized but not propagated.

Source: `src/channels/signal.ts` (installed from the `channels` branch by `/add-signal`).

## Read receipts

The adapter sends `read` receipts back to senders after the consuming container reports that it has finished processing a message. This is the same "blue double-check" semantics Signal users expect from another human, scoped to the NanoClaw acknowledgment point.

### What triggers a receipt

The host runs a per-session sweep that watches each container's `processing_ack` table (in `outbound.db`) and copies completion state into `messages_in.status` on `inbound.db`. When that sweep observes the transition from non-`completed` to `completed`, the host dispatches a per-channel completion hook. The Signal adapter's hook looks up the original message's protocol timestamp (preserved as `messages_in.id`) and issues `sendReceipt` with `type: 'read'` to the original sender.

The timestamp echoed back is the sender-side `dataMessage.timestamp` from the original envelope. Signal's clients use that field to match the receipt to the right message in their own UI, which is why the host preserves it as the row id rather than minting a new one.

### Defaults

- **DMs**: on
- **Group messages**: off (multi-recipient delivery state has different semantics; deferred)
- **Failed processing**: no receipt (only `completed`-status transitions fire the hook; `failed` is treated as undelivered)
- **signal-cli < 0.13.0**: off, with a startup warning log naming the detected version
- **`SIGNAL_FORCE_RECEIPTS_DISABLED=1`** (environment override): off regardless of daemon version

### Disabling per agent group

Read receipts are gated by a per-agent-group flag `channel_settings.signal.readReceipts` stored on the agent group's container config. To turn them off for one agent group:

```bash
ncl groups config update --id <agent-group-id> \
  --channel-settings '{"signal":{"readReceipts":false}}'
```

The change takes effect immediately on the next consumed message — no container restart needed, since the gate is checked host-side at receipt-dispatch time.

To re-enable, set the same flag back to `true` or clear the override:

```bash
ncl groups config update --id <agent-group-id> \
  --channel-settings '{"signal":{"readReceipts":true}}'
```

Missing or invalid `channel_settings` values resolve to the default `{ signal: { readReceipts: true } }` at read time, so agent groups created before this feature shipped get the default behavior on upgrade.

## Inbound receipts from peers

When a peer's Signal client confirms delivery, marks the bot's outbound as read, or marks media as viewed, signal-cli forwards a `receiptMessage` envelope through `subscribeReceive`. The adapter recognizes all three subtypes (`read`, `viewed`, `delivery`) and logs them at `debug` level.

The log shape, written to `logs/nanoclaw.log` when the host's log level is `debug` or finer:

```
{ direction: 'inbound-receipt', sender: '<peer-uuid>', type: 'read' | 'viewed' | 'delivery', timestamps: [<number>, ...] }
```

No DB write happens. No further routing fires. Higher-level features (e.g. surfacing "the user read my reply" to the agent, or annotating `messages_out`) are deferred to a future change.

## Limitations (v1)

- **No outbound receipts in group chats.** Multi-recipient delivery state needs per-member tracking; out of scope for v1.
- **No `viewed` receipts for media.** Requires a heuristic for when the agent surfaced the attachment to the user. Deferred.
- **No persistent storage of peer receipts.** Inbound `receiptMessage` is logged only — there is no `messages_out.read_at` column or peer-receipt log table yet.
- **No retroactive receipts.** Messages received before this feature was enabled do not get a receipt when the next message comes in; the gate fires only on the live transition observed in the sweep loop.

## Version requirement

`sendReceipt` shipped in signal-cli `0.13.0`. The adapter calls the `version` RPC at startup and caches a boolean for the lifetime of the connection. The completion hook checks the cache before issuing the RPC.

- `version >= 0.13.0`: outbound receipts enabled (subject to the per-group flag).
- `version < 0.13.0`: outbound receipts disabled. A `warn` log entry names the detected version. The rest of the adapter (inbound routing, typing indicators, outbound `send`) functions normally.
- `version` RPC fails or returns an unparseable response: outbound receipts disabled as a safe default. A `warn` log entry records the failure.
- Daemon returns method-not-found on a later `sendReceipt` call: the adapter flips the cached flag to disabled for the rest of the session and logs the failure. Self-healing per session; no crash.

The `/add-signal` install skill installs the `latest` signal-cli release from GitHub, which is well past `0.13.0`. There is no version pin to update — the runtime check is the source of truth, so the adapter degrades gracefully on older or unusual daemon builds without any manual intervention.

## See also

- `/add-signal` — install skill (one-time setup, registration, credentials)
- `docs/architecture.md` — host-sweep loop, `processing_ack` semantics, and the per-session DB split that this feature plugs into
- `openspec/changes/signal-read-receipts/design.md` — design rationale, alternatives considered, and the runtime version-detection decision
