/**
 * Channel adapter registry.
 *
 * Channels self-register on import. The host calls initChannelAdapters() at startup
 * to instantiate and set up all registered adapters.
 */
import type { ChannelAdapter, ChannelRegistration, ChannelSetup } from './adapter.js';
import { log } from '../log.js';

const SETUP_RETRY_DELAYS_MS = [2000, 5000, 10000];

/** Duck-type check — adapters that throw an Error with `name === 'NetworkError'`
 * (Chat SDK's `@chat-adapter/shared.NetworkError` and similar) get a retry on
 * setup. Avoids depending on `@chat-adapter/shared` at trunk level. */
function isNetworkError(err: unknown): err is Error {
  return err instanceof Error && err.name === 'NetworkError';
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const registry = new Map<string, ChannelRegistration>();
const activeAdapters = new Map<string, ChannelAdapter>();

/**
 * Per-channel hook invoked by the host-sweep after the agent container has
 * finished processing an inbound message (i.e. `messages_in.status` just
 * transitioned to 'completed' for a row whose container `processing_ack.status`
 * was 'completed'). Failed transitions are NOT dispatched — host-sweep filters
 * those out before calling `dispatchCompletionHooks`.
 *
 * The hook is opt-in. Channels that do not register one are skipped silently
 * — the dispatcher does not log a warning for unregistered channel types.
 *
 * Errors thrown (or promises rejected) by a hook are isolated via
 * Promise.allSettled and logged at warn level. They MUST NOT propagate to the
 * sweep loop or affect sibling hooks for the same dispatch.
 */
export type CompletionHook = (msg: {
  id: string;
  platformId: string;
  threadId: string | null;
  agentGroupId: string;
  /**
   * Original sender's channel-native handle, parsed from the inbound row's
   * content JSON. For DMs this equals `platformId`; for group chats it's the
   * individual sender (the group id lives in `platformId`). Null when
   * messages_in.content can't be parsed or carries no `sender` field — e.g.
   * a2a-internal rows or pre-migration data.
   */
  sender: string | null;
}) => void | Promise<void>;

const completionHooks = new Map<string, CompletionHook[]>();

/**
 * Register a completion hook for a channel type. Multiple hooks are allowed
 * per channel (useful for tests and for layered adapters). Dispatch order is
 * not guaranteed — `Promise.allSettled` runs them concurrently.
 */
export function registerCompletionHook(channelType: string, hook: CompletionHook): void {
  const list = completionHooks.get(channelType);
  if (list) {
    list.push(hook);
  } else {
    completionHooks.set(channelType, [hook]);
  }
}

/**
 * Dispatch all hooks registered for `channelType` with the given message
 * payload. Resolves after every hook settles. Hooks that throw or reject are
 * warn-logged and do not affect siblings.
 *
 * No-op (and no log) when there are no hooks for `channelType` — the common
 * case for channels that don't implement completion semantics.
 */
export async function dispatchCompletionHooks(
  channelType: string,
  msg: { id: string; platformId: string; threadId: string | null; agentGroupId: string; sender: string | null },
): Promise<void> {
  const hooks = completionHooks.get(channelType);
  if (!hooks || hooks.length === 0) return;

  const results = await Promise.allSettled(
    hooks.map(async (hook) => {
      // Wrap synchronous throws as well so Promise.allSettled catches them.
      await hook(msg);
    }),
  );

  for (const result of results) {
    if (result.status === 'rejected') {
      log.warn('Completion hook rejected', {
        channelType,
        messageId: msg.id,
        err: result.reason,
      });
    }
  }
}

/**
 * Test-only: clear the completion-hook registry. The production code never
 * unregisters hooks (they live for the lifetime of the process), but tests
 * that exercise the registry need a way to reset between cases.
 */
export function _resetCompletionHooksForTesting(): void {
  completionHooks.clear();
}

/** Register a channel adapter factory. Called by channel modules on import. */
export function registerChannelAdapter(name: string, registration: ChannelRegistration): void {
  registry.set(name, registration);
}

/** Get a live adapter by channel type. */
export function getChannelAdapter(channelType: string): ChannelAdapter | undefined {
  return activeAdapters.get(channelType);
}

/** Get all active adapters. */
export function getActiveAdapters(): ChannelAdapter[] {
  return [...activeAdapters.values()];
}

/** Get all registered channel names. */
export function getRegisteredChannelNames(): string[] {
  return [...registry.keys()];
}

/** Get container config for a channel (used by container-runner for additional mounts/env). */
export function getChannelContainerConfig(name: string): ChannelRegistration['containerConfig'] {
  return registry.get(name)?.containerConfig;
}

/**
 * Instantiate and set up all registered channel adapters.
 * Skips adapters that return null (missing credentials).
 */
export async function initChannelAdapters(setupFn: (adapter: ChannelAdapter) => ChannelSetup): Promise<void> {
  for (const [name, registration] of registry) {
    try {
      const adapter = await registration.factory();
      if (!adapter) {
        log.warn('Channel credentials missing, skipping', { channel: name });
        continue;
      }

      const setup = setupFn(adapter);
      // Transient network failures during adapter init (e.g. Telegram deleteWebhook
      // hitting a DNS hiccup at boot) would otherwise leave the channel permanently
      // dead until manual restart. Retry only on NetworkError so misconfigs (bad
      // tokens, etc.) still fail fast.
      let attempt = 0;
      while (true) {
        try {
          await adapter.setup(setup);
          break;
        } catch (err) {
          if (isNetworkError(err) && attempt < SETUP_RETRY_DELAYS_MS.length) {
            const delay = SETUP_RETRY_DELAYS_MS[attempt]!;
            log.warn('Channel adapter setup failed with network error, retrying', {
              channel: name,
              attempt: attempt + 1,
              delayMs: delay,
              err: err.message,
            });
            await sleep(delay);
            attempt += 1;
            continue;
          }
          throw err;
        }
      }
      activeAdapters.set(adapter.channelType, adapter);
      log.info('Channel adapter started', { channel: name, type: adapter.channelType });
    } catch (err) {
      log.error('Failed to start channel adapter', { channel: name, err });
    }
  }
}

/** Tear down all active adapters. */
export async function teardownChannelAdapters(): Promise<void> {
  for (const [name, adapter] of activeAdapters) {
    try {
      await adapter.teardown();
      log.info('Channel adapter stopped', { channel: name });
    } catch (err) {
      log.error('Failed to stop channel adapter', { channel: name, err });
    }
  }
  activeAdapters.clear();
}
