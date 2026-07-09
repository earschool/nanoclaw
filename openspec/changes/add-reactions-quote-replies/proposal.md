## Why

Today the agent has one outbound primitive: send a new chat message. That hides two things humans do constantly on chat platforms — react to a specific message with an emoji, and quote-reply to a specific message. Without these, the agent's outbound expression is flatter than what every channel adapter's underlying platform already supports natively.

Concrete gap surfaced during live testing: a user quote-replied to one of NanoClaw's group messages, and the only way for the agent to acknowledge it was to send a fresh text message — losing the message-level threading the human's UI was rendering. The agent also has no way to say "got it" with a 👍 instead of a sentence.

This change adds two new agent MCP tools — `add_reaction` and `send_quote_reply` — and the host plumbing required to deliver them through the Signal adapter and every Chat SDK adapter, with capability-based gating so unsupported channels don't expose tools the agent can't actually use.

## What Changes

- **Agent MCP surface (container/agent-runner/src/mcp-tools/)**: two new tools.
  - `add_reaction(target_message_id, emoji)` — toggle semantics: same emoji on same target removes; different emoji either replaces (single-reaction platforms like Signal) or adds (multi-reaction platforms like Discord, decided by adapter).
  - `send_quote_reply(target_message_id, text, attachments?)` — sends a chat message that quotes the target. Body text + attachments + arbitrarily-deep platform-rendered chain (we don't constrain depth; we just point at one target).
  - Both tools surface platform errors (e.g. target deleted, emoji rejected) back to the agent as the tool result; the agent decides next step.

- **Outbound persistence (`messages_out`)**: two new `kind` values.
  - `kind='reaction'` with content `{ targetMessageId, emoji }`. Stored, delivered via the standard outbound polling path.
  - `kind='chat'` (unchanged) with content that may now include `quote: { targetMessageId }`. Adapter reads `quote` field and addresses platform-native quote at send time.

- **Adapter contract (`src/channels/adapter.ts`)**: two new optional methods.
  - `sendReaction(platformId, threadId, payload)` — payload `{ targetMessageId, emoji }`.
  - `sendQuoteReply(platformId, threadId, message, target)` — `target = { messageId }`.
  - Both optional so existing/legacy adapters compile unchanged.

- **Signal native adapter (`src/channels/signal.ts`)**: implement both.
  - `sendReaction` → `tcp.rpc('sendReaction', { emoji, targetTimestamp, targetAuthor, recipient or groupId, account, remove })`. Toggle state decided by host (see below).
  - `sendQuoteReply` → existing `sendText` augmented to set `quoteTimestamp` / `quoteAuthor` / `quoteMessage` on the `send` RPC params.

- **Chat SDK bridge (`src/channels/chat-sdk-bridge.ts`)**: implement both via runtime feature-detect on the SDK instance. If `typeof sdk.react === 'function'` and `typeof sdk.reply === 'function'`, register; otherwise treat the channel as unsupported.

- **Capability discovery (new `session_capabilities` row in inbound.db)**: on session spawn, the host inspects the active adapter and writes a JSON blob describing what this session can do (`{ reactions, quoteReplies, attachments }`). Agent-runner reads it at startup and gates which MCP tools it registers. Tools that aren't supported never appear in the agent's toolset.

- **Bot's own message ids surfaced to the agent**:
  - Formatter (`container/agent-runner/src/formatter.ts`) renders each bot turn in the conversation history with its `platform_message_id` (from the `delivered` table on inbound.db, plumbed via a new join in the outbound→prompt path).
  - `send_message` (and the two new tools) return the new platform id from their tool-call result so the agent can capture it without re-reading the formatter.

- **Toggle state for `add_reaction`**: host scans `messages_out` for prior reaction rows with the same `(channel_type, platform_id, target_message_id, emoji, agent_group_id)` triple. Match found → emit `remove: true` to the adapter (toggle off). No match → emit add.

- **Inbound reactions (`src/channels/signal.ts:handleEnvelope`)**: currently fall through; will start writing one `messages_in` row per inbound reaction with `kind='chat'`, `trigger=0`, content `{ text: '[Reaction <emoji> by <senderName> on msg <targetMessageId> "<truncatedTargetText>"]', sender, senderId, senderName, isReaction: true }`. Agent gets passive context on the next wake; reactions alone don't trigger engagement.

- **Tests**:
  - Vitest (host): Signal adapter mocks RPC for `sendReaction` + the augmented `send` with quote params; delivery.ts dispatches by kind; toggle-state DB scan returns correct add/remove decisions; capability gating gates tools on the right boolean.
  - bun:test (container): MCP tool shape + outbound-DB write contract; capability-driven tool registration; tool error surface.
  - No live e2e in CI. Local-only signal-cli smoke after each.

## Capabilities

### New Capabilities

- `channel-reactions` — the host-side adapter contract + delivery dispatch for sending and receiving emoji reactions, with toggle state tracking and per-channel capability gating.
- `channel-quote-replies` — the host-side adapter contract + delivery wiring for sending platform-native quote-replies, with target message ids surfaced consistently to the agent.
- `agent-message-actions` — the agent-runner MCP surface (`add_reaction`, `send_quote_reply`), capability-driven tool registration from `session_capabilities`, and the formatter changes that surface bot message ids back into conversation history.

### Modified Capabilities

- (none — this change is purely additive. Existing `send_message` keeps its current signature; no shorthand for quote-replies is added.)

## Impact

- **Code**:
  - `container/agent-runner/src/mcp-tools/add-reaction.ts` + `send-quote-reply.ts` (new).
  - `container/agent-runner/src/formatter.ts` (surface bot platform ids in history).
  - `container/agent-runner/src/db/messages-out.ts` (accept `kind='reaction'` + `content.quote`).
  - `container/agent-runner/src/db/session-capabilities.ts` (new — read capabilities at startup, expose to tool registrar).
  - `src/channels/adapter.ts` (extend `ChannelAdapter` with two optional methods, extend `OutboundMessage` with `quote` field).
  - `src/channels/signal.ts` (sendReaction + quote params on send + inbound reaction handling).
  - `src/channels/chat-sdk-bridge.ts` (runtime feature-detect, both methods).
  - `src/delivery.ts` (dispatch on `msg.kind === 'reaction'`; pass `quote` field through for `kind='chat'`).
  - `src/db/session-db.ts` (new `getReactionState(outDb, ...)` for toggle decision).
  - `src/db/migrations/0NN-session-capabilities.ts` (new migration — `session_capabilities` row in inbound.db schema).
  - `src/session-manager.ts` (write capabilities on session create / spawn).

- **Database**:
  - New `session_capabilities` table in `inbound.db` schema: `(id INTEGER PRIMARY KEY CHECK (id=1), capabilities TEXT NOT NULL)`. Singleton row, JSON blob. Idempotent migration.
  - No central-DB schema change.

- **Tests**:
  - `src/channels/signal.test.ts` — extend with reaction RPC shape + quote-param assertions.
  - `src/channels/signal-reactions.test.ts` (new) — inbound reaction → messages_in row; outbound reaction → sendReaction RPC; toggle state add vs remove.
  - `src/channels/chat-sdk-bridge.test.ts` — capability detection + react/reply dispatch.
  - `src/delivery.test.ts` (new) — dispatch on kind='reaction'.
  - `container/agent-runner/src/mcp-tools/add-reaction.test.ts` + `send-quote-reply.test.ts` — tool shapes, error surface, outbound-DB write.

- **Docs**:
  - `container/skills/welcome/SKILL.md` (or a new tool-specific skill) — teach the agent when to react vs reply vs send.
  - `CLAUDE.md` — add `add_reaction` and `send_quote_reply` to the agent-tool reference.
  - No new operator-facing docs; capability discovery is automatic.

- **Backwards compatibility**:
  - `ChannelAdapter` contract additions are optional — no existing adapter (native or Chat SDK) needs to be touched if the maintainer doesn't want the feature.
  - `OutboundMessage` gains optional `quote` field; existing serializations continue to round-trip.
  - Existing `messages_out` rows untouched; new `kind='reaction'` values are forward-only.
  - No feature flag; behavior is automatic per-channel via capability gating.

- **Out of scope**:
  - Cross-conversation targets (reacting to / quote-replying to a message in a different chat) — rejected at the MCP tool layer.
  - Approval gating / rate limits — not added.
  - `send_message` does not get an `inReplyTo` shorthand — strict tool separation.
  - Inbound reactions do not wake the agent; they are passive context only.
  - Inbound reactions to bot's own past messages do not retroactively edit the bot's prior prompt — they appear as a new context entry on the next wake.

- **Blocked by**: nothing.
- **Blocks**: nothing currently active. Future work (e.g. typing-on-react, react-to-react chains) would build on the primitives this change introduces.
