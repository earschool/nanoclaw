/**
 * Signal read-receipts — Stream S3 tests.
 * Covers `sendReadReceipt`, runtime `version` detection, and the
 * completion-hook firing path. Separate from signal.test.ts to stay under
 * the 300-line cap on new files.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { EventEmitter } from 'events';

vi.mock('./channel-registry.js', async () => {
  const actual = await vi.importActual<typeof import('./channel-registry.js')>('./channel-registry.js');
  return { ...actual, registerChannelAdapter: vi.fn() };
});
vi.mock('../env.js', () => ({ readEnvFile: vi.fn(() => ({})) }));
vi.mock('../log.js', () => ({
  log: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));
vi.mock('node:child_process', () => ({
  spawn: vi.fn(),
  execFileSync: vi.fn(),
  execSync: vi.fn(),
}));
vi.mock('../db/container-configs.js', () => ({
  getChannelSettings: vi.fn(() => ({ signal: { readReceipts: true } })),
}));

const tcpRef = vi.hoisted(() => ({
  rpcResponses: new Map<string, unknown>(),
  rpcErrors: new Map<string, { message: string }>(),
  fakeSocket: null as any,
}));

function createFakeSocket() {
  const sock = new EventEmitter() as any;
  sock.destroyed = false;
  sock.destroy = vi.fn(() => {
    sock.destroyed = true;
    sock.emit('close');
  });
  sock.write = vi.fn((data: string) => {
    try {
      const req = JSON.parse(data.trim());
      const err = tcpRef.rpcErrors.get(req.method);
      const payload = err
        ? { jsonrpc: '2.0', id: req.id, error: err }
        : { jsonrpc: '2.0', id: req.id, result: tcpRef.rpcResponses.get(req.method) ?? { ok: true } };
      setImmediate(() => sock.emit('data', Buffer.from(JSON.stringify(payload) + '\n')));
    } catch {
      /* ignore */
    }
  });
  return sock;
}

vi.mock('node:net', () => ({
  createConnection: vi.fn((_port: number, _host: string, cb?: () => void) => {
    const sock = createFakeSocket();
    tcpRef.fakeSocket = sock;
    if (cb) setImmediate(cb);
    return sock;
  }),
}));

import type { ChannelSetup } from './adapter.js';
import { createSignalAdapter } from './signal.js';
import { log } from '../log.js';
import { _resetCompletionHooksForTesting, dispatchCompletionHooks } from './channel-registry.js';
import { getChannelSettings } from '../db/container-configs.js';

function createMockSetup() {
  return {
    onInbound: vi.fn() as unknown as ChannelSetup['onInbound'],
    onInboundEvent: vi.fn() as unknown as ChannelSetup['onInboundEvent'],
    onMetadata: vi.fn() as unknown as ChannelSetup['onMetadata'],
    onAction: vi.fn() as unknown as ChannelSetup['onAction'],
  };
}

function createAdapter() {
  return createSignalAdapter({
    cliPath: 'signal-cli',
    account: '+15551234567',
    tcpHost: '127.0.0.1',
    tcpPort: 7583,
    manageDaemon: false,
    signalDataDir: '/tmp/does-not-exist',
  });
}

function getRpcCallsForMethod(method: string) {
  if (!tcpRef.fakeSocket) return [];
  return tcpRef.fakeSocket.write.mock.calls
    .map((c: any[]) => {
      try {
        return JSON.parse(c[0].trim());
      } catch {
        return null;
      }
    })
    .filter((c: any) => c && c.method === method);
}

function dmHookMsg(id: string) {
  return { id, platformId: '+15555550123', threadId: null, agentGroupId: 'ag-1', sender: '+15555550123' };
}

function groupHookMsg(id: string, sender: string | null) {
  return { id, platformId: 'group:abc123', threadId: null, agentGroupId: 'ag-1', sender };
}

beforeEach(() => {
  vi.clearAllMocks();
  tcpRef.rpcResponses.clear();
  tcpRef.rpcErrors.clear();
  tcpRef.fakeSocket = null;
  tcpRef.rpcResponses.set('updateProfile', {});
  tcpRef.rpcResponses.set('updateConfiguration', {});
  tcpRef.rpcResponses.set('version', { version: '0.13.4' });
  tcpRef.rpcResponses.set('sendReceipt', {});
  _resetCompletionHooksForTesting();
  delete process.env.SIGNAL_FORCE_RECEIPTS_DISABLED;
  vi.mocked(getChannelSettings).mockReturnValue({ signal: { readReceipts: true } });
});

afterEach(() => {
  try {
    tcpRef.fakeSocket?.destroy();
  } catch {
    /* already closed */
  }
  delete process.env.SIGNAL_FORCE_RECEIPTS_DISABLED;
});

describe('signal sendReadReceipt RPC shape', () => {
  it('calls tcp.rpc("sendReceipt", {recipient, targetTimestamps:[ts], type:"read"})', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', {
      id: '1234567890',
      platformId: '+15555555555',
      threadId: null,
      agentGroupId: 'ag-1',
      sender: '+15555555555',
    });
    const calls = getRpcCallsForMethod('sendReceipt');
    expect(calls.length).toBe(1);
    expect(calls[0].params).toEqual({
      recipient: '+15555555555',
      targetTimestamps: [1234567890],
      type: 'read',
    });
    await adapter.teardown();
  });

  it('swallows RPC errors (no throw) and warn-logs', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    tcpRef.rpcErrors.set('sendReceipt', { message: 'transient blip' });
    await expect(dispatchCompletionHooks('signal', dmHookMsg('1234567890'))).resolves.toBeUndefined();
    expect(vi.mocked(log.warn).mock.calls.some((c) => /signal/i.test(String(c[0])))).toBe(true);
    await adapter.teardown();
  });

  it('flips receiptsSupported=false on method-not-found; later dispatches no-op', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    tcpRef.rpcErrors.set('sendReceipt', { message: 'Method not found: sendReceipt' });
    await dispatchCompletionHooks('signal', dmHookMsg('1111111111'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(1);
    tcpRef.rpcErrors.delete('sendReceipt');
    await dispatchCompletionHooks('signal', dmHookMsg('2222222222'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(1);
    await adapter.teardown();
  });
});

describe('signal completion-hook firing', () => {
  it('fires sendReceipt for default config + DM platformId + integer id', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(1);
    await adapter.teardown();
  });

  it('does not fire when getChannelSettings reports readReceipts=false', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    vi.mocked(getChannelSettings).mockReturnValue({ signal: { readReceipts: false } });
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    expect(vi.mocked(getChannelSettings)).toHaveBeenCalledWith('ag-1');
    await adapter.teardown();
  });

  it('fires for group platformId, addressing the receipt to the original sender (not the group id)', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', groupHookMsg('1700000000000', '+15555550999'));
    const calls = getRpcCallsForMethod('sendReceipt');
    expect(calls.length).toBe(1);
    expect(calls[0].params).toEqual({
      recipient: '+15555550999',
      targetTimestamps: [1700000000000],
      type: 'read',
    });
    await adapter.teardown();
  });

  it('skips group receipt when sender is missing (e.g. unparseable content)', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', groupHookMsg('1700000000000', null));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    await adapter.teardown();
  });

  it('respects readReceipts=false for groups too', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    vi.mocked(getChannelSettings).mockReturnValue({ signal: { readReceipts: false } });
    await dispatchCompletionHooks('signal', groupHookMsg('1700000000000', '+15555550999'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    await adapter.teardown();
  });

  it('does not fire when receiptsSupported=false (version 0.12.5)', async () => {
    tcpRef.rpcResponses.set('version', { version: '0.12.5' });
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    await adapter.teardown();
  });

  it('does not fire when SIGNAL_FORCE_RECEIPTS_DISABLED=1 even on a new daemon', async () => {
    process.env.SIGNAL_FORCE_RECEIPTS_DISABLED = '1';
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    await adapter.teardown();
  });

  it('does not fire when msg.id is not an integer; debug-logs the reason', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', {
      id: 'not-a-number',
      platformId: '+15555550123',
      threadId: null,
      agentGroupId: 'ag-1',
      sender: '+15555550123',
    });
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    const matched = vi
      .mocked(log.debug)
      .mock.calls.some((c) => /synthetic-or-invalid-timestamp|invalid timestamp/i.test(String(c[0])));
    expect(matched).toBe(true);
    await adapter.teardown();
  });

  // Regression: router (src/router.ts) composes inbound ids as
  // `${signalTimestamp}:${agentGroupId}`. Earlier code parseInt-ed the whole
  // string then strict-equal-checked against the original — every signal
  // message failed and the hook silently early-returned.
  it('extracts the timestamp prefix from router-composed `${ts}:${agentGroupId}` ids', async () => {
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', {
      id: '1700000000000:ag-1778766760008-8v8wnj',
      platformId: '+15555550123',
      threadId: null,
      agentGroupId: 'ag-1778766760008-8v8wnj',
      sender: '+15555550123',
    });
    const calls = getRpcCallsForMethod('sendReceipt');
    expect(calls.length).toBe(1);
    expect(calls[0].params).toEqual({
      recipient: '+15555550123',
      targetTimestamps: [1700000000000],
      type: 'read',
    });
    await adapter.teardown();
  });
});

describe('signal version detection', () => {
  it('marks receiptsSupported=true for exactly 0.13.0', async () => {
    tcpRef.rpcResponses.set('version', { version: '0.13.0' });
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(1);
    await adapter.teardown();
  });

  it('marks receiptsSupported=true for newer 0.13.4', async () => {
    tcpRef.rpcResponses.set('version', { version: '0.13.4' });
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(1);
    await adapter.teardown();
  });

  it('marks receiptsSupported=false for 0.12.99 and warn-logs naming the version', async () => {
    tcpRef.rpcResponses.set('version', { version: '0.12.99' });
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    expect(vi.mocked(log.warn).mock.calls.some((c) => /0\.12\.99/.test(JSON.stringify(c)))).toBe(true);
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    await adapter.teardown();
  });

  it('marks receiptsSupported=false when version RPC throws; warn-logged', async () => {
    tcpRef.rpcErrors.set('version', { message: 'method gone' });
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    expect(vi.mocked(log.warn)).toHaveBeenCalled();
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    await adapter.teardown();
  });

  it('marks receiptsSupported=false on unparseable version payload', async () => {
    tcpRef.rpcResponses.set('version', { weird: 'no-version-here' });
    const adapter = createAdapter();
    await adapter.setup(createMockSetup());
    expect(vi.mocked(log.warn)).toHaveBeenCalled();
    await dispatchCompletionHooks('signal', dmHookMsg('1700000000000'));
    expect(getRpcCallsForMethod('sendReceipt').length).toBe(0);
    await adapter.teardown();
  });
});
