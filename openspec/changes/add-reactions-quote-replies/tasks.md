## 1. Adapter contract + types

- [ ] 1.1 Add optional `sendReaction(platformId, threadId, payload)` and `sendQuoteReply(platformId, threadId, message, target)` methods to `ChannelAdapter` in `src/channels/adapter.ts`.
- [ ] 1.2 Extend `OutboundMessage` with optional `quote?: { messageId: string }` field. Document that adapters reading `quote` should address platform-native quote-reply at send time.
- [ ] 1.3 Add new payload + result types for reaction send (`{ targetMessageId, emoji, remove }`).
- [ ] 1.4 Vitest: `src/channels/adapter.test.ts` confirms `ChannelAdapter` shape compiles with and without the new methods; types-only test acceptable.

## 2. Outbound `kind='reaction'` + content quote field

- [ ] 2.1 In `src/delivery.ts`, branch on `msg.kind === 'reaction'` before the existing `system` / `agent` checks; if branch fires, call `adapter.sendReaction(...)` instead of `deliver(...)` and skip the attachments path.
- [ ] 2.2 In `src/delivery.ts`'s normal `deliver` path, parse `content.quote` and, when present + adapter implements `sendQuoteReply`, dispatch to `sendQuoteReply` instead of `deliver`. Quote target absent → unchanged.
- [ ] 2.3 Vitest: `src/delivery.test.ts` (new file) covers (a) kind='reaction' dispatch, (b) quote present + capability → quote dispatch, (c) quote present + capability absent → fall through with a logged warn.
- [ ] 2.4 In `container/agent-runner/src/db/messages-out.ts`, accept `kind='reaction'` and `content.quote` in the row writer; no schema change required (`messages_out` is JSON-content-tolerant).

## 3. Toggle state scan

- [ ] 3.1 Add `getPriorReactionState(outDb, { channelType, platformId, targetMessageId, emoji })` to `src/db/session-db.ts` — SELECT on `messages_out` with `kind='reaction'` + JSON LIKE filters. Returns `{ hasPriorAdd: boolean }`.
- [ ] 3.2 Add an index on `messages_out(kind)` via existing migration apparatus (cheap; covers the LIKE branch).
- [ ] 3.3 In `src/delivery.ts` reaction branch, before calling `adapter.sendReaction`, call `getPriorReactionState`. Set `remove = hasPriorAdd`.
- [ ] 3.4 Vitest: covers prior-add → remove decision; no-prior → add decision; multiple adds in a row produce alternating add/remove decisions.

## 4. `session_capabilities` schema + write path

- [ ] 4.1 Add new migration `src/db/migrations/0NN-session-capabilities.ts` creating `session_capabilities (id INTEGER PRIMARY KEY CHECK (id=1), capabilities TEXT NOT NULL)` in the inbound-DB schema template (`src/db/schema.ts`).
- [ ] 4.2 In `src/session-manager.ts`, after `resolveSession` returns a newly-created session OR on every spawn (TBD: confirm during impl), open inbound.db and `INSERT OR REPLACE` a capabilities blob. Compute via `typeof adapter.sendReaction === 'function'` style probes against the active adapter for the session's `channel_type`.
- [ ] 4.3 Vitest: `src/session-manager.test.ts` covers (a) fresh session gets capabilities row, (b) existing session refresh updates the row, (c) adapter without methods → blob has `false` for both.

## 5. Container-side capability read + tool gating

- [ ] 5.1 New file `container/agent-runner/src/db/session-capabilities.ts` reads the row at startup; falls back to `{ reactions: false, quoteReplies: false }` if the table is absent.
- [ ] 5.2 In `container/agent-runner/src/mcp-tools/index.ts` (or wherever the registrar lives), gate registration of the new tools on the relevant boolean.
- [ ] 5.3 bun:test: tool registrar respects the capability blob; absent capability → tool not in the registered list.

## 6. Agent MCP tool: `add_reaction`

- [ ] 6.1 `container/agent-runner/src/mcp-tools/add-reaction.ts` — new tool. Params: `target_message_id`, `emoji`. Writes a `messages_out` row with `kind='reaction'`, `content={targetMessageId, emoji}`. Returns `{ ok: true, platformMessageId?: string }` after host delivery completes, or `{ ok: false, error: string }`.
- [ ] 6.2 Tool description tells the agent: same emoji twice = toggle off; different emoji = adapter decides (replace on single-reaction platforms, add on multi-reaction).
- [ ] 6.3 Reject same-conversation invariant: if the target message id isn't in the current session's history (scan inbound.db), return error.
- [ ] 6.4 bun:test: write to outbound, malformed input rejected, same-conversation check, error path returns to agent.

## 7. Agent MCP tool: `send_quote_reply`

- [ ] 7.1 `container/agent-runner/src/mcp-tools/send-quote-reply.ts` — new tool. Params: `target_message_id`, `text`, optional `attachments`. Writes `messages_out` row with `kind='chat'`, `content={text, quote: {messageId}}, files=attachments`. Returns `{ ok: true, platformMessageId?: string }` or error.
- [ ] 7.2 Same-conversation invariant enforced like 6.3.
- [ ] 7.3 bun:test: outbound write shape, target validation, attachment passthrough.

## 8. Signal adapter — outbound

- [ ] 8.1 In `src/channels/signal.ts`, implement `sendReaction`. RPC: `tcp.rpc('sendReaction', { emoji, targetTimestamp, targetAuthor, recipient OR groupId, account, remove })`. `targetTimestamp` is parsed from `payload.targetMessageId`; `targetAuthor` resolved from `messages_in.content.sender` for the target row (lookup helper in `getSessionInboundLookup`).
- [ ] 8.2 Extend `sendText` (or factor a `sendTextOrQuote` helper) to set `quoteTimestamp`/`quoteAuthor`/`quoteMessage` on the `send` RPC params when an `OutboundMessage.quote` is present. Reuses the existing chunking + textStyle path.
- [ ] 8.3 Vitest: `src/channels/signal-reactions.test.ts` (new) — mock RPC, assert exact send-shape for reaction (add + remove), assert exact send-shape for quote-reply.
- [ ] 8.4 Vitest: assert outbound RPC errors propagate as `{ ok: false, error }` through the delivery → tool-result path.

## 9. Signal adapter — inbound reactions

- [ ] 9.1 In `src/channels/signal.ts:handleEnvelope`, detect `dataMessage.reaction`. Build a `messages_in`-shaped object with `kind='chat'`, `trigger=0` (handled by host write path), and content containing the rendered `[Reaction <emoji> ...]` text + structured `reaction` field.
- [ ] 9.2 Surface via `setup.onInbound(platformId, threadId, msg)`. Router must accept trigger=0 inbound for accumulation; verify the path already supports this (it does, via `ignored_message_policy='accumulate'`).
- [ ] 9.3 Vitest: inbound reaction envelope produces correct `messages_in` row with `isReaction: true` and the expected text format.

## 10. Chat SDK bridge

- [ ] 10.1 In `src/channels/chat-sdk-bridge.ts`, runtime feature-detect `react` + `reply` methods on the SDK instance per-channel.
- [ ] 10.2 Implement `sendReaction` → SDK's `react()` (or removal API as appropriate).
- [ ] 10.3 Implement `sendQuoteReply` → SDK's `reply()`.
- [ ] 10.4 Capability blob computed per-channel via feature-detect and surfaced to the host's `session_capabilities` writer.
- [ ] 10.5 Vitest: capability-detect with a mocked SDK instance; react/reply dispatch with correct args.

## 11. Formatter: bot's own message ids in history

- [ ] 11.1 Plumb `delivered.platform_message_id` from inbound.db's `delivered` table into the agent-runner's history-render path.
- [ ] 11.2 Update `container/agent-runner/src/formatter.ts` to emit each `<assistant_turn>` block with `message_id="<platformMessageId>"` attribute when available.
- [ ] 11.3 `send_message` tool's result now includes `platformMessageId` (await delivery completion before returning).
- [ ] 11.4 bun:test: rendered history includes the id; tool returns the id when delivery succeeds.

## 12. Documentation

- [ ] 12.1 Update `CLAUDE.md` agent-tool reference with `add_reaction` and `send_quote_reply` — short description + the toggle semantics caveat.
- [ ] 12.2 Update `container/skills/welcome/SKILL.md` (or a new `message-actions/SKILL.md`) with guidance on when to react vs. reply vs. send.
- [ ] 12.3 Mention in `docs/db-session.md`: new `session_capabilities` row in inbound.db.

## 13. Integration sweep

- [ ] 13.1 Live signal-cli smoke (local-only, not CI): send a DM, agent reacts, verify Alice's phone shows reaction; reply-quote in a group, verify it threads on her end.
- [ ] 13.2 Verify nothing in the host typecheck breaks across `pnpm exec tsc` (host) and the container's `tsc -p container/agent-runner/tsconfig.json --noEmit`.
- [ ] 13.3 Verify the existing Signal receipts test suite continues to pass (no regression).
