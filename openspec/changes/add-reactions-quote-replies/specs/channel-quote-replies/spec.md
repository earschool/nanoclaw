## ADDED Requirements

### Requirement: Adapter contract for outbound quote-replies
The `ChannelAdapter` interface SHALL define an optional method `sendQuoteReply(platformId, threadId, message, target)` where `message: OutboundMessage` and `target: { messageId: string }`. Adapters that do not implement the method SHALL compile and run unchanged. The host's delivery dispatch MUST treat a missing `sendQuoteReply` method as "quote-replies unsupported on this channel" and propagate that downstream into the session capability blob.

#### Scenario: Adapter compiles without implementing sendQuoteReply
- **WHEN** a `ChannelAdapter` implementation omits `sendQuoteReply`
- **THEN** TypeScript compilation succeeds AND `typeof adapter.sendQuoteReply === 'function'` returns false

#### Scenario: Outbound carries quote, adapter implements method
- **GIVEN** an adapter implementing `sendQuoteReply`
- **WHEN** the agent writes a `messages_out` row with `kind='chat'`, `content.text='thanks!'`, `content.quote={ messageId: '17xxxx' }`
- **THEN** the host's delivery dispatch routes the row to `adapter.sendQuoteReply(platformId, threadId, message, { messageId: '17xxxx' })` AND NOT to the regular `deliver` method

### Requirement: Outbound message shape with quote field
The `OutboundMessage` interface SHALL gain an optional `quote?: { messageId: string }` field. When present, the host delivery layer SHALL parse it out of the JSON content before invoking the adapter. When absent, behavior is unchanged. Adapters that don't implement `sendQuoteReply` but receive a row with a quote field MUST fall back to delivering as a plain message and log a warn-level event indicating the fallback occurred.

#### Scenario: Quote-reply fallback on unsupporting adapter
- **GIVEN** an adapter that does NOT implement `sendQuoteReply`
- **WHEN** a `messages_out` row arrives with `content.quote={ messageId: '17xxxx' }`
- **THEN** the host's delivery loop calls the adapter's regular `deliver` method (no `sendQuoteReply` call) AND logs a warn-level event AND marks the row delivered on success

### Requirement: Signal adapter quote-reply implementation
The Signal channel adapter SHALL implement `sendQuoteReply` by extending the existing `send` JSON-RPC call with `quoteTimestamp`, `quoteAuthor`, and `quoteMessage` params. `quoteTimestamp` is the numeric Signal protocol timestamp from `target.messageId`; `quoteAuthor` is the original sender's phone/UUID resolved via the same `messages_in.content.sender` lookup used for reactions; `quoteMessage` is the truncated target text (≤200 chars, recommended) read from `messages_in.content.text` for that target.

#### Scenario: DM quote-reply RPC shape
- **GIVEN** a session DM with `platformId='+15551112222'`, target message id `1700000000000` originally sent by `+15551112222` with text "hi there"
- **WHEN** `sendQuoteReply(platformId, null, { kind: 'chat', content: { text: 'thanks!' } }, { messageId: '1700000000000' })` is called
- **THEN** the resulting `tcp.rpc('send', ...)` params include `message: 'thanks!'`, `quoteTimestamp: 1700000000000`, `quoteAuthor: '+15551112222'`, `quoteMessage: 'hi there'`, `recipient: ['+15551112222']`

#### Scenario: Group quote-reply RPC shape
- **GIVEN** a session group with `platformId='group:abc123='`, target message id `1700000000000` sent by `+15555550999` in the group
- **WHEN** `sendQuoteReply` is called with text 'got it'
- **THEN** the RPC params include `groupId: 'abc123='` AND `quoteAuthor: '+15555550999'` AND `quoteTimestamp: 1700000000000` AND DO NOT include a `recipient` field

### Requirement: Capability gating at session spawn
On session spawn, the host SHALL compute `capabilities.quoteReplies = typeof adapter.sendQuoteReply === 'function'` and write it into the `session_capabilities` singleton row. The container's agent-runner SHALL gate registration of the `send_quote_reply` MCP tool on this boolean.

#### Scenario: Tool registered when capability is true
- **WHEN** the host spawns a session whose adapter implements `sendQuoteReply`
- **THEN** `capabilities.quoteReplies === true` AND the agent-runner registers `send_quote_reply` AND the agent's toolset includes it

#### Scenario: Tool hidden when capability is false
- **WHEN** the host spawns a session whose adapter does NOT implement `sendQuoteReply`
- **THEN** `capabilities.quoteReplies === false` AND the agent-runner does not register the tool

### Requirement: Quote-replies stay within the active conversation
The `send_quote_reply` MCP tool SHALL reject any `target_message_id` that does not correspond to a `messages_in` row in the active session's inbound DB. The rejection MUST surface to the agent as `{ ok: false, error: 'target message not in this conversation' }` without writing a `messages_out` row.

#### Scenario: Cross-session target rejected
- **GIVEN** session A holds inbound id `X` and session B holds inbound id `Y`
- **WHEN** the agent in session A calls `send_quote_reply(target_message_id='Y', text='...')` 
- **THEN** the tool returns the structured error AND no row is written

### Requirement: Quote-reply payload accepts text and attachments
The `send_quote_reply` MCP tool SHALL accept the same `text` and optional `attachments` parameters as `send_message`. The host SHALL deliver text and attachments through the adapter's `sendQuoteReply` method, allowing the adapter to assemble a single quoted message with both. Attachments MUST traverse the standard outbox path (file written to session outbox; host reads + hands buffers to adapter).

#### Scenario: Text-only quote-reply
- **WHEN** `send_quote_reply(target, text='ok', attachments=undefined)` is called
- **THEN** the outbound row has `content.text='ok'`, `content.quote={messageId}`, no `files` field, AND the adapter receives a message with no attachments

#### Scenario: Quote-reply with one image attachment
- **WHEN** `send_quote_reply(target, text='see attached', attachments=[{filename:'x.png', data:base64}])` is called
- **THEN** the outbound row's content has text + quote, files are written to outbox, AND the adapter receives an OutboundMessage with `files` populated and the target message id intact
