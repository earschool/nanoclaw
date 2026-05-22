## ADDED Requirements

### Requirement: Outbound read receipts on container completion

The Signal channel adapter SHALL emit a `read` receipt back to the original sender after the host has observed the consuming container transition the inbound message's status to `'completed'`, when the per-agent-group `channel_settings.signal.readReceipts` configuration resolves to `true` (the default).

#### Scenario: DM consumed, receipt sent
- **WHEN** an inbound `dataMessage` from a 1:1 sender has been routed into `messages_in`, AND the consuming container has written a `processing_ack` row with `status='completed'`, AND the host's `syncProcessingAcks` sweep has transitioned `messages_in.status` from non-`'completed'` to `'completed'`
- **THEN** the host SHALL dispatch the completion hook registered for `channel_type='signal'`, AND the Signal adapter SHALL invoke the signal-cli JSON-RPC method `sendReceipt` exactly once with `{ recipient: <sender>, targetTimestamps: [<original-message-timestamp>], type: 'read' }`

#### Scenario: Read receipts disabled in config
- **WHEN** the consuming agent group's `channel_settings.signal.readReceipts` resolves to `false` AND the completion hook fires
- **THEN** the Signal adapter SHALL NOT invoke `sendReceipt` for that message

#### Scenario: Group inbound excluded
- **WHEN** the inbound row's `platform_id` begins with the prefix `group:` (indicating a group message)
- **THEN** the Signal adapter SHALL NOT invoke `sendReceipt` regardless of `readReceipts` configuration

#### Scenario: Container failure does not fire receipt
- **WHEN** the container writes `processing_ack` with `status='failed'` for an inbound message
- **THEN** `syncProcessingAcks` SHALL still transition `messages_in.status` to `'completed'` (existing behavior), BUT the completion hook MUST distinguish failed from succeeded transitions and SHALL NOT fire the Signal `sendReceipt` for failed ones

#### Scenario: Synthetic timestamp suppresses receipt
- **WHEN** the inbound `messages_in.id` does not parse as a valid integer in the Signal-protocol-timestamp range (e.g. the row was created with the `Date.now()` fallback because the original envelope lacked a `timestamp` field)
- **THEN** the Signal adapter SHALL NOT invoke `sendReceipt` and SHALL log a `debug` entry naming the reason

### Requirement: Inbound receipt envelopes recognized and logged

The Signal channel adapter SHALL recognize and log inbound `receiptMessage` envelopes received from peers via the `subscribeReceive` stream.

#### Scenario: Peer read receipt received
- **WHEN** an envelope arrives with `receiptMessage: { type: 'read', timestamps: [...] }`
- **THEN** the adapter SHALL emit a `debug`-level log entry containing the sender identifier, the receipt type `'read'`, and the targeted message timestamps; no database write SHALL occur; no further dispatch SHALL fire

#### Scenario: Peer viewed receipt received
- **WHEN** an envelope arrives with `receiptMessage: { type: 'viewed', timestamps: [...] }`
- **THEN** the adapter SHALL emit a `debug`-level log entry with the same fields as a `read` receipt; no database write SHALL occur

#### Scenario: Peer delivery receipt received
- **WHEN** an envelope arrives with `receiptMessage: { type: 'delivery', timestamps: [...] }`
- **THEN** the adapter SHALL emit a `debug`-level log entry with the same fields; no database write SHALL occur

#### Scenario: Other envelope kinds unaffected
- **WHEN** the envelope is a `dataMessage`, `editMessage`, `typingMessage`, or any other recognized non-receipt envelope
- **THEN** the existing dispatch path for that envelope kind SHALL continue unchanged

### Requirement: Runtime signal-cli capability detection

The Signal channel adapter SHALL detect at startup whether the connected signal-cli daemon supports the `sendReceipt` JSON-RPC method, cache the result, and use the cache to gate all outbound receipt firing.

#### Scenario: signal-cli at or above 0.13.0
- **WHEN** the adapter's `start()` issues the `version` RPC AND the returned signal-cli version is greater than or equal to `0.13.0`
- **THEN** the adapter SHALL cache `receiptsSupported = true` for the lifetime of the connection, AND outbound receipt firing SHALL be enabled subject to per-message config gates

#### Scenario: signal-cli below 0.13.0
- **WHEN** the `version` RPC returns a signal-cli version less than `0.13.0`
- **THEN** the adapter SHALL log a `warn`-level entry naming the detected version and the disabled feature, AND SHALL cache `receiptsSupported = false`; the adapter SHALL otherwise function normally

#### Scenario: Version RPC failure
- **WHEN** the `version` RPC throws, times out, or returns a response that cannot be parsed as a semver-shaped version
- **THEN** the adapter SHALL log a `warn`-level entry, SHALL cache `receiptsSupported = false` as a safe default, AND the adapter SHALL continue startup normally

#### Scenario: Daemon rejects sendReceipt at runtime
- **WHEN** the adapter has cached `receiptsSupported = true` AND a subsequent `sendReceipt` RPC fails with a method-not-found or equivalent error
- **THEN** the adapter SHALL log the failure at `warn` level, SHALL flip the cached `receiptsSupported` to `false` for the remainder of the session, AND SHALL NOT raise the error to the caller (completion-hook dispatch must not crash the sweep loop)

#### Scenario: Manual override
- **WHEN** the environment variable `SIGNAL_FORCE_RECEIPTS_DISABLED` is set to `'1'`
- **THEN** the adapter SHALL cache `receiptsSupported = false` regardless of the daemon's reported version, AND no `sendReceipt` RPC SHALL be issued for the lifetime of the connection

### Requirement: Completion-hook dispatch is per-channel and isolated

The host-side completion-hook registry SHALL dispatch transitions to per-channel hooks and SHALL isolate failures so a single hook's failure does not affect other channels or other transitions in the same sweep tick.

#### Scenario: Hook registration and dispatch
- **WHEN** a channel adapter calls `registerCompletionHook(channelType, hook)` at startup, AND the host-sweep observes a transitioned message whose `messages_in.channel_type` matches `channelType`
- **THEN** the registry SHALL invoke `hook({ id, platformId, threadId })` exactly once per transition

#### Scenario: Hook exception is contained
- **WHEN** a registered hook throws or rejects
- **THEN** the dispatcher SHALL log the rejection at `warn` level AND continue dispatching remaining transitions; the host-sweep tick SHALL not abort

#### Scenario: Channel with no hook
- **WHEN** a transitioned message's `channel_type` has no registered hook
- **THEN** the dispatcher SHALL skip silently (no log spam) and continue
