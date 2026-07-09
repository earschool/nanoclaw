## Context

NanoClaw's outbound delivery pipeline today: agent writes a row to `messages_out` (kind='chat' or 'system' or 'agent'); host's delivery poll picks it up, parses the JSON content, and calls `deliveryAdapter.deliver(channelType, platformId, threadId, kind, content, files)`. The adapter does whatever the platform requires for "send a chat message" and optionally returns the platform's message id.

Every channel adapter currently exposes exactly one outbound primitive (`deliver`). The Signal adapter, the Chat SDK bridge adapters, and the legacy native adapters all converge on this single method. As a result, agent expressivity is capped at "send a new chat message"; we cannot surface platform-native message-level actions even when the underlying platform fully supports them.

Two such actions matter enough to be worth a first-class primitive:

1. **Reactions** — Signal's `sendReaction` RPC, Discord's `addReaction`/`removeReaction` REST endpoints, Slack's `reactions.add`/`reactions.remove`, etc. Every Chat SDK channel exposes this. Quietly absent in our agent surface.

2. **Quote-replies** — Signal's `send` RPC takes optional `quoteTimestamp`/`quoteAuthor`/`quoteMessage`; Discord has message-references; Slack has thread_ts; iMessage has reply chains. All Chat SDK channels expose a `reply()` method.

Recent stream S3 receipts work proved the host-side primitives for cross-message addressing are workable: we already plumb Signal protocol timestamps through `messages_in.id` and the new `sender` field. This change generalizes that primitive into a full reverse direction.

## Goals / Non-Goals

**Goals**
- Agent has two new tools (`add_reaction`, `send_quote_reply`) that are platform-native everywhere they can be (Signal + every Chat SDK channel), and absent everywhere they can't.
- Capability gating is automatic: tools never appear in the agent's toolset on channels where the adapter doesn't implement them, so the agent never has to handle "tool exists but won't work" failure modes.
- Toggle semantics for reactions are uniform across platforms from the agent's perspective: same emoji on same target = remove. Different emoji = adapter decides (Signal replaces, Discord coexists).
- Inbound reactions surface as conversation context without waking the agent — passive awareness, zero risk of reaction-storm loops.
- Bot's own past message ids are addressable: surfaced in both formatter output and tool-call return values, so the agent can reliably point at "the message I just sent."
- Zero impact on existing channel adapters that don't implement the new methods. Existing wirings, existing tests, existing flows untouched.

**Non-Goals**
- No new approval / rate-limit / gating policy. The new tools ride the same authorization plane as `send_message`.
- No cross-conversation targeting. Tools reject targets that aren't in the active session's chat.
- No `send_message` overload with quote shortcut — the two new tools are the only way.
- No retroactive edits to historical prompts when inbound reactions arrive on old messages — the reaction shows up as a new context entry only.
- No persisted reaction *state* table (counts per message); we infer toggle-state by scanning `messages_out`. No analytics / dashboard / reaction counts.
- No emoji allowlist / sanitization; agents pass whatever Unicode they choose. Platform rejects = tool error returned to agent.
- No feature flag. Capability gating is the only enable/disable mechanism.

## Decisions

### D1. Tool surface: two new MCP tools, not extensions of `send_message`

`send_message` stays a plain "send a new chat message" verb. `add_reaction` and `send_quote_reply` are independent primitives.

**Why:** Cleaner discovery, simpler reasoning at the LLM. Each tool has one job. A user reading the agent's transcript can tell at a glance which action it took. Discoverability is the dominant cost at this LLM scale; tool sprawl is cheap, fat signatures are expensive.

**Alternative considered:** add `inReplyTo` param to `send_message`. Rejected — the call sites would mix the two intents, and reaction-only sends (no text body) become awkwardly modeled.

### D2. Adapter contract: two new optional methods on `ChannelAdapter`

```ts
interface ChannelAdapter {
  // existing
  deliver(...): Promise<string | undefined>;

  // new — both optional
  sendReaction?(platformId: string, threadId: string | null, payload: {
    targetMessageId: string;
    emoji: string;
    remove: boolean;
  }): Promise<string | undefined>;

  sendQuoteReply?(platformId: string, threadId: string | null, message: OutboundMessage, target: {
    messageId: string;
  }): Promise<string | undefined>;
}
```

**Why:** Adapters that don't implement = no compilation change, no behavior change. Capability detection is `typeof adapter.sendReaction === 'function'` — uniform across native adapters and the Chat SDK bridge.

**Alternative considered:** single `deliver()` with kind-dispatch inside the adapter. Rejected — every adapter would duplicate the kind-switch logic, and external introspection of "what does this adapter actually support?" gets harder.

### D3. Capability gating: written by host at session spawn, read by container at startup

On session spawn (`src/session-manager.ts` resolveSession path), host computes:

```ts
const capabilities = {
  reactions: typeof adapter.sendReaction === 'function',
  quoteReplies: typeof adapter.sendQuoteReply === 'function',
  attachments: /* already-existing check */,
};
```

…and writes `INSERT OR REPLACE INTO session_capabilities (id, capabilities) VALUES (1, ?)` into the session's `inbound.db`. Container's agent-runner reads this once at startup (in the same routine that already reads other session metadata), and the MCP tool registrar gates registration on the relevant boolean.

**Why:** Capability is fully derivable from the live adapter, but recomputing per-tool-call would mean the agent-runner needs to round-trip through the host for every check. Writing at spawn lets the container be ignorant of host state. Capability is a session-lifetime invariant (channel can't change mid-session), so caching at spawn is safe.

**Alternative considered:** new MCP tool `get_channel_capabilities` round-tripping to host. Rejected on latency + plumbing cost. Static config in agent-runner code rejected as the worst of both worlds.

### D4. Toggle semantics via `messages_out` scan

```ts
// In src/db/session-db.ts:
export function getPriorReactionState(outDb, args: {
  channelType: string;
  platformId: string;
  targetMessageId: string;
  emoji: string;
}): { hasPriorAdd: boolean };
```

Implementation: `SELECT 1 FROM messages_out WHERE kind='reaction' AND content LIKE '%targetMessageId%' AND content LIKE '%emoji%' AND deliver_status='delivered'`. Refine to a structured query in v2 if we add a `reactions` summary table; for now the JSON LIKE is fast enough at our scale (single-user, per-session DB).

When `add_reaction(target, e)` is invoked:
- Scan history. If found a prior add (and no later remove) for `(target, e)` → call `sendReaction(..., { remove: true })`.
- Otherwise → call `sendReaction(..., { remove: false })`.

**Why:** No new tables, no new migrations. Existing single-writer guarantees on `messages_out` still hold (container writes, host reads). The query is contained per-session.

**Alternative considered:** new `reactions` table in central DB. Rejected as over-engineering for the volume we expect.

### D5. Inbound reactions: persist as `messages_in` rows with `trigger=0`

When `handleEnvelope` sees `dataMessage.reaction`, write a `messages_in` row:

```ts
{
  id: String(reactionTimestamp),
  kind: 'chat',
  platformId,
  channelType: 'signal',
  threadId: null,
  trigger: 0,
  content: JSON.stringify({
    text: `[Reaction ${emoji} by ${senderName} on msg ${targetMessageId}${
      targetText ? ` "${targetText.slice(0, 40)}"` : ''
    }]`,
    sender,
    senderId: `signal:${sender}`,
    senderName,
    isReaction: true,
    reaction: { emoji, targetMessageId, targetAuthor },
  }),
  timestamp,
}
```

The formatter renders `isReaction: true` rows in a compact form (single line, no quoted-content block).

**Why:**
- Auditable in DB (matches every other inbound).
- Survives container restart — context isn't lost.
- `trigger=0` means it accumulates; agent picks it up on next wake but doesn't engage on the reaction alone (prevents reaction-storm loops).
- Same primitive across DM and group (group reactions follow the same envelope shape).

### D6. Bot's own platform ids surfaced via formatter + tool return

Two parallel paths:

1. **Formatter.** When rendering an agent turn in conversation history, the container's agent-runner joins `messages_out` (agent's own past sends) with `delivered.platform_message_id` (recorded after the host's adapter returns one). The rendered form looks like:
   ```
   <assistant_turn message_id="1779968354113">
   Read. Both.
   </assistant_turn>
   ```
2. **Tool return.** `send_message`, `add_reaction`, `send_quote_reply` all return `{ platformMessageId: string | null }` to the caller. The agent can stash this for immediate follow-ups within the same turn (e.g. "send X, then react 👍 to it").

**Why:** Path 1 covers the "across-turn" case (agent references its previous response on a later turn). Path 2 covers the "within-turn" case (no DB roundtrip needed). Both paths are cheap; together they cover all reasonable agent usage patterns.

### D7. Chat SDK bridge: runtime feature-detect, not version pin

```ts
const bridgeCapabilities = {
  reactions: typeof chatInstance.react === 'function',
  quoteReplies: typeof chatInstance.reply === 'function',
};
```

If the active Chat SDK version doesn't expose `react` or `reply` for a given channel, the capability blob written into `session_capabilities` reflects that, and the tools simply don't appear for that session. No error, no silent fallback, no warning at startup — capabilities are query-time only.

**Why:** Chat SDK is a moving target. Pinning a min version forces a coordinated upgrade. Feature-detect lights the feature up automatically across SDK upgrades and degrades gracefully when a channel doesn't support it.

### D8. No feature flag — capability is the gate

There is no `ENABLE_REACTIONS` env var, no per-agent-group toggle, no per-wiring override. Once the change merges, the feature is live everywhere it's supported. Owners who want to disable it can downgrade the Chat SDK or stop the channel — but there's no first-class kill switch.

**Why:** The cost of a flag is borne forever (every code path must check it; every doc must mention it). The cost of "operator wants to disable" is low — a custom adapter wrapper or a manual `delete adapter.sendReaction` would do it.

## Risks / Trade-offs

- **Toggle scan cost.** A session DB with a long reaction history makes the LIKE query slow. Cap: ~5ms at 10k reactions per session is well within the existing sweep budget. We add an index on `messages_out(kind)` to short-circuit.
- **Reaction race.** Two agent containers in the same session firing reactions on the same message at the same time would both see no-prior-add and both emit add. Not a realistic scenario (one container per session by invariant) but mentioning for completeness.
- **Inbound-reaction storm.** A user reacting 50 times in a row generates 50 inbound rows. They all land with trigger=0, so no extra wakes, but the next wake's prompt will carry all 50 lines. Acceptable; rate-limiting inbound reactions at the adapter could be a future optimization.
- **Bot's own msg id formatter coupling.** Reading `delivered.platform_message_id` from the inbound DB into the prompt couples the formatter to a previously host-only column. The container-side `delivered` mirror needs to be kept in sync; we already do this via the host's outbox-clear path.
- **Cross-adapter consistency.** The toggle vs add-multiple distinction is a deliberate platform-honest difference. The agent has to understand the difference (Signal is "one bot reaction at a time"; Discord can have many). Documented in CLAUDE.md.
- **Inbound reaction id collision.** Signal reaction envelopes use the reaction's own protocol timestamp as the id. We use that as `messages_in.id`. Reasonable risk of collision with chat message timestamps from the same sender if they happen at the same millisecond. We rely on the same `id` uniqueness invariant we already trust for chat messages.

## Migration Plan

1. **Schema migration (host).** New migration in `src/db/migrations/0NN-session-capabilities.ts`. Adds `session_capabilities` to the inbound-DB schema template. Existing session DBs get the table on next open via the schema-apply path that already handles incremental changes.

2. **Container-side adoption.** Agent-runner reads `session_capabilities` at startup. If the table is absent (older session DB), fall back to "no extra capabilities" — tools are simply not registered. This means existing sessions need no migration; they get the new tools on next spawn.

3. **Adapter implementations.** Land Signal native + Chat SDK bridge in the same PR. Each native adapter that wants to opt in can land its own `sendReaction`/`sendQuoteReply` in a follow-up; the absence of either method just means the feature won't appear for that channel.

4. **Rollout.** No phased rollout. The capability-driven gate is the rollout mechanism — channels light up as adapters implement them.

## Open Questions

- Should `send_quote_reply` return the bot's new platform message id (for chained quote-replies)? Yes — per D6, every send tool returns its platform id.
- Do we need a `remove_reaction` tool separate from `add_reaction`'s toggle? No — toggle semantics on `add_reaction` are sufficient. If the agent really wants to remove without knowing prior state, it can scan its own history (formatter exposes ids) or just call `add_reaction(target, sameEmoji)` and trust the toggle.
- Inbound reactions on bot's own messages: do they appear in the prompt as "Alice reacted 👍 to [your previous message]"? Yes — same format as any other inbound reaction, with the target text being the bot's own.
