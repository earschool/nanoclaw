## ADDED Requirements

### Requirement: Adapter contract for outbound reactions
The `ChannelAdapter` interface in `src/channels/adapter.ts` SHALL define an optional method `sendReaction(platformId, threadId, payload)` where `payload = { targetMessageId: string; emoji: string; remove: boolean }`. Adapters that do not implement the method SHALL compile and run unchanged. The host's delivery dispatch MUST treat a missing `sendReaction` method as "reactions unsupported on this channel" and propagate that downstream into the session capability blob.

#### Scenario: Adapter compiles without implementing sendReaction
- **WHEN** a `ChannelAdapter` implementation omits `sendReaction`
- **THEN** TypeScript compilation succeeds AND `typeof adapter.sendReaction === 'function'` returns false AND the adapter's behavior for non-reaction outbound is unchanged

#### Scenario: Adapter implements sendReaction
- **WHEN** an adapter exposes `sendReaction(platformId, threadId, { targetMessageId, emoji, remove: false })`
- **THEN** the host's delivery dispatch routes `messages_out` rows with `kind='reaction'` to this method instead of the regular `deliver` method, passing the structured payload as-is

### Requirement: Outbound reaction persistence and delivery
The container's agent-runner SHALL write a row to `messages_out` with `kind='reaction'` and `content={ targetMessageId: string, emoji: string }` for every agent-initiated reaction. The host's delivery polling loop in `src/delivery.ts` SHALL detect this kind, scan `messages_out` to determine toggle state (`getPriorReactionState`), call `adapter.sendReaction(...)` with the computed `remove` boolean, and on success mark the row delivered via the standard `delivered` table.

#### Scenario: First reaction add
- **GIVEN** no prior `messages_out` row exists with `kind='reaction'` and the same `(channel_type, platform_id, targetMessageId, emoji)`
- **WHEN** the agent calls `add_reaction(targetMessageId='17xxxx', emoji='👍')`
- **THEN** `getPriorReactionState` returns `{ hasPriorAdd: false }` AND `adapter.sendReaction` is called with `remove: false` AND on success the outbound row is marked delivered

#### Scenario: Toggle off via duplicate emoji
- **GIVEN** a prior delivered `messages_out` row with `kind='reaction'`, same target, same emoji, no later remove
- **WHEN** the agent calls `add_reaction(target, sameEmoji)` again
- **THEN** `getPriorReactionState` returns `{ hasPriorAdd: true }` AND `adapter.sendReaction` is called with `remove: true`

### Requirement: Signal adapter reaction implementation
The Signal channel adapter in `src/channels/signal.ts` SHALL implement `sendReaction` by calling `tcp.rpc('sendReaction', { emoji, targetTimestamp, targetAuthor, recipient or groupId, account, remove })`. `targetTimestamp` is the numeric Signal protocol timestamp parsed from `payload.targetMessageId`. `targetAuthor` is the original sender's phone/UUID resolved from the corresponding `messages_in` row's `content.sender` field. For DM platformIds, the `recipient` field carries the sender's number; for group platformIds, `groupId` carries the group id and `recipient` is omitted.

#### Scenario: DM reaction RPC shape
- **GIVEN** a session with `platformId='+15551112222'`, an inbound message id `1700000000000` originally sent by `+15551112222`
- **WHEN** `sendReaction(platformId, null, { targetMessageId: '1700000000000', emoji: '👍', remove: false })` is called
- **THEN** the resulting `tcp.rpc('sendReaction', ...)` params exactly equal `{ emoji: '👍', targetTimestamp: 1700000000000, targetAuthor: '+15551112222', recipient: ['+15551112222'], account: '+15103942741', remove: false }`

#### Scenario: Group reaction RPC shape
- **GIVEN** a session with `platformId='group:abc123='`, an inbound from `+15555550999` in the group, target timestamp `1700000000000`
- **WHEN** `sendReaction(platformId, null, { targetMessageId, emoji: '🎉', remove: false })` is called
- **THEN** the RPC params include `groupId: 'abc123='` AND `targetAuthor: '+15555550999'` AND DO NOT include a `recipient` field

### Requirement: Capability gating at session spawn
On session spawn, the host SHALL compute `capabilities.reactions = typeof adapter.sendReaction === 'function'` for the session's `channel_type` and write the value into a `session_capabilities` singleton row in the session's `inbound.db`. The container's agent-runner reads this at startup and uses the value to gate registration of the `add_reaction` MCP tool. If the capabilities row is absent (older session DB), the agent-runner defaults reactions to false.

#### Scenario: Adapter implements reactions → tool registered
- **WHEN** the host spawns a session whose adapter implements `sendReaction`
- **THEN** `session_capabilities.capabilities.reactions === true` AND the container's agent-runner registers the `add_reaction` MCP tool AND the tool appears in the agent's toolset

#### Scenario: Adapter does not implement reactions → tool hidden
- **WHEN** the host spawns a session whose adapter does not implement `sendReaction`
- **THEN** `session_capabilities.capabilities.reactions === false` AND the agent-runner does not register `add_reaction` AND the tool is absent from the agent's toolset

### Requirement: Inbound reaction persistence and prompt surfacing
When a channel adapter receives an inbound reaction, it SHALL surface a `messages_in`-shaped object to the host with `kind='chat'`, accumulator semantics (`trigger=0`), and content containing both a rendered human-readable text line AND a structured `reaction` field. The text line MUST be in the format `[Reaction <emoji> by <senderName> on msg <targetMessageId>[ "<truncatedTargetText>"]]` so the agent reading the prompt understands the event at a glance. The structured `reaction: { emoji, targetMessageId, targetAuthor }` field MUST be present for downstream consumers.

#### Scenario: Signal group inbound reaction
- **GIVEN** an inbound Signal envelope with `dataMessage.reaction = { emoji: '🎉', targetSentTimestamp: 1700000000000, targetAuthor: '+15103942741' }` in group `group:abc123=`
- **WHEN** `handleEnvelope` processes the envelope
- **THEN** a `messages_in` row is written with `kind='chat'`, `trigger=0` (via accumulator policy), content text starting with `[Reaction 🎉 by Alice on msg 1700000000000`, and structured `reaction` field carrying the same values

#### Scenario: Inbound reaction does not wake the container
- **GIVEN** an inbound reaction arrives at a session whose container is not currently running
- **WHEN** the host completes write of the `messages_in` row
- **THEN** `countDueMessages` does NOT count the reaction row (because `trigger=0`) AND no container wake fires AND the next legitimately-engaging message will see the accumulated reaction in its prompt context

### Requirement: Reactions do not propagate cross-conversation
The `add_reaction` MCP tool SHALL reject any `target_message_id` that does not correspond to a `messages_in` row in the active session's inbound DB. The rejection MUST surface to the agent as a structured error result (`{ ok: false, error: 'target message not in this conversation' }`) without writing any `messages_out` row.

#### Scenario: Cross-session target rejected
- **GIVEN** session A holds inbound id `X` and session B holds inbound id `Y`
- **WHEN** the agent in session A calls `add_reaction(target_message_id='Y', emoji='👍')`
- **THEN** the tool returns `{ ok: false, error: 'target message not in this conversation' }` AND no row is written to A's `messages_out` AND no RPC fires
