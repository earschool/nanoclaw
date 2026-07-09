## ADDED Requirements

### Requirement: New MCP tool — add_reaction
The agent-runner SHALL expose a new MCP tool `add_reaction(target_message_id: string, emoji: string)` (registered only when `session_capabilities.capabilities.reactions === true`). The tool writes a `messages_out` row with `kind='reaction'` and `content={targetMessageId, emoji}`, awaits delivery completion, and returns either `{ ok: true, platformMessageId?: string }` or `{ ok: false, error: string }`. The tool's description in the MCP manifest SHALL document the toggle semantics: calling with the same emoji on the same target removes the bot's prior reaction.

#### Scenario: Tool registered when capability is true
- **GIVEN** the session capabilities blob has `reactions: true`
- **WHEN** the agent-runner starts
- **THEN** `add_reaction` is registered in the MCP server's tool list AND the tool's description includes the toggle-semantics caveat

#### Scenario: Tool hidden when capability is false
- **GIVEN** the session capabilities blob has `reactions: false` (or is absent)
- **WHEN** the agent-runner starts
- **THEN** `add_reaction` is NOT registered AND the agent's toolset does not include it

#### Scenario: Successful add
- **WHEN** `add_reaction(target='17xxxx', emoji='👍')` is invoked AND host delivery succeeds
- **THEN** the tool returns `{ ok: true, platformMessageId: <bot's new platform id or null> }` AND a `messages_out` row exists with `kind='reaction'` AND the delivered table marks it delivered

#### Scenario: Adapter error surfaced to agent
- **WHEN** `add_reaction` is invoked AND the adapter throws (e.g. target message deleted on platform)
- **THEN** the tool returns `{ ok: false, error: <error message> }` AND no delivered-row is recorded AND the agent's transcript shows the error message

### Requirement: New MCP tool — send_quote_reply
The agent-runner SHALL expose a new MCP tool `send_quote_reply(target_message_id: string, text: string, attachments?: Array<{ filename: string; data: string }>)` (registered only when `session_capabilities.capabilities.quoteReplies === true`). The tool writes a `messages_out` row with `kind='chat'` and `content={text, quote: {messageId}, attachments}`. Attachments are materialized to the session outbox via the standard write path. Tool result mirrors `add_reaction`'s shape: `{ ok: true, platformMessageId? }` or `{ ok: false, error }`.

#### Scenario: Tool registered when capability is true
- **GIVEN** `quoteReplies: true` in capabilities
- **WHEN** agent-runner starts
- **THEN** `send_quote_reply` is registered

#### Scenario: Text-only quote-reply round trip
- **WHEN** `send_quote_reply(target='17xxxx', text='thanks!')` is invoked
- **THEN** a `messages_out` row exists with `kind='chat'`, `content.text='thanks!'`, `content.quote={messageId: '17xxxx'}`, no `files` AND the adapter receives a `sendQuoteReply` call AND the tool returns `{ ok: true, platformMessageId: ... }`

### Requirement: Same-conversation invariant
Both `add_reaction` and `send_quote_reply` SHALL reject `target_message_id` values that do not correspond to a `messages_in` row in the active session's inbound DB. The rejection MUST be performed inside the tool implementation (not at the host layer) so no `messages_out` row is ever written. The error returned to the agent SHALL be a structured `{ ok: false, error: 'target message not in this conversation' }`.

#### Scenario: Invalid target id
- **WHEN** the agent calls either tool with a `target_message_id` that has no `messages_in` row in this session's inbound.db
- **THEN** the tool returns the structured error AND no `messages_out` row is created AND no adapter RPC fires

### Requirement: Capability discovery from session DB
The agent-runner SHALL read `session_capabilities` from the session's inbound DB at startup and expose the value to the MCP tool registrar. If the table or row is absent (older session DB), the agent-runner defaults all capabilities to `false`. The registrar SHALL only call `register(tool)` for tools whose required capability is `true`.

#### Scenario: Capabilities table absent
- **GIVEN** a session DB created before this change
- **WHEN** the agent-runner starts
- **THEN** capabilities default to `{ reactions: false, quoteReplies: false, attachments: <existing logic> }` AND `add_reaction` + `send_quote_reply` are not registered

#### Scenario: Capabilities table present with mixed values
- **GIVEN** capabilities `{ reactions: true, quoteReplies: false }`
- **WHEN** the agent-runner starts
- **THEN** `add_reaction` IS registered AND `send_quote_reply` is NOT registered

### Requirement: Bot's own message ids surfaced in conversation history
The container's prompt formatter SHALL render each bot turn in conversation history with its platform message id when available. The id source is `delivered.platform_message_id` from the session's inbound DB, joined against the agent's prior `messages_out` rows. The rendered form for an assistant turn MUST include the platform id as a structured attribute (e.g. `<assistant_turn message_id="17xxxx">`) so the agent can address the message in a subsequent `add_reaction` or `send_quote_reply` call.

#### Scenario: Bot turn with known platform id
- **GIVEN** the agent previously sent a message and the host delivered it with `platform_message_id='17xxxx'`
- **WHEN** the agent-runner formats the conversation history for the next prompt
- **THEN** the assistant-turn block includes `message_id="17xxxx"`

#### Scenario: Bot turn before delivery completes (rare)
- **GIVEN** a `messages_out` row that has not yet been delivered (no `delivered` row exists)
- **WHEN** the formatter renders the history
- **THEN** the assistant-turn block omits the `message_id` attribute (no placeholder, no stub)

### Requirement: Send-tool return values include platform message id
The existing `send_message` tool AND the new `add_reaction` and `send_quote_reply` tools SHALL each return `{ ok: true, platformMessageId: string | null }` after the host's delivery completes. Tools MUST NOT return until delivery has resolved (success or failure). If delivery never completes within a tool-execution timeout, the tool returns `{ ok: false, error: 'delivery timed out' }`.

#### Scenario: send_message returns the platform id of its sent message
- **WHEN** the agent calls `send_message(text='hi')` AND the host's adapter returns platform id `17xxxx`
- **THEN** the tool returns `{ ok: true, platformMessageId: '17xxxx' }` AND that id is usable as a target in a follow-up `add_reaction` call within the same turn

#### Scenario: send_message returns null platform id when adapter doesn't supply one
- **WHEN** the adapter returns `undefined` from `deliver`
- **THEN** the tool returns `{ ok: true, platformMessageId: null }` AND the agent can still call `add_reaction` later by reading the id from formatter context after the next inbound

### Requirement: Tool description discoverability
The MCP tool descriptions for `add_reaction` and `send_quote_reply` SHALL document (a) toggle semantics and platform variance for reactions, (b) the same-conversation constraint, (c) the `platformMessageId` return contract, and (d) the absence of the tools on channels that don't support them. Description text MUST be visible to the agent via the standard MCP tool-listing path.

#### Scenario: Tool description includes toggle clause
- **WHEN** the agent inspects `add_reaction` via the MCP tool-listing
- **THEN** the description text contains a clause naming the toggle semantics ("same emoji on same target removes") AND the platform-variance caveat ("Signal replaces; Discord coexists")
