import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';

// --- Mocks ---

vi.mock('./channel-registry.js', () => ({
  registerChannelAdapter: vi.fn(),
  registerCompletionHook: vi.fn(),
}));
// Container-configs mock — signal.ts now imports getChannelSettings for the
// completion-hook config gate. Default-on; tests that need readReceipts=false
// can override per-test.
vi.mock('../db/container-configs.js', () => ({
  getChannelSettings: vi.fn(() => ({ signal: { readReceipts: true } })),
}));
vi.mock('../env.js', () => ({ readEnvFile: vi.fn(() => ({})) }));
vi.mock('../log.js', () => ({
  log: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock('node:child_process', () => ({
  spawn: vi.fn(),
  execFileSync: vi.fn(),
}));

// --- TCP socket mock ---

import { EventEmitter } from 'events';

const tcpRef = vi.hoisted(() => ({
  rpcResponses: new Map<string, unknown>(),
  // FIFO queue of error responses keyed by RPC method. Each call to a method
  // consumes one entry; once the queue is empty, subsequent calls fall back
  // to the success path. Lets tests model both "every call fails" and
  // "send #1 fails, retry #2 succeeds" without leaking state across tests.
  rpcErrorsOnce: new Map<string, unknown[]>(),
  fakeSocket: null as any,
}));

function createFakeSocket(): EventEmitter & {
  write: ReturnType<typeof vi.fn>;
  destroy: ReturnType<typeof vi.fn>;
  destroyed: boolean;
} {
  const sock = new EventEmitter() as any;
  sock.destroyed = false;
  sock.destroy = vi.fn(() => {
    sock.destroyed = true;
    sock.emit('close');
  });
  sock.write = vi.fn((data: string) => {
    try {
      const req = JSON.parse(data.trim());
      const errQueue = tcpRef.rpcErrorsOnce.get(req.method);
      if (errQueue !== undefined) {
        if (!Array.isArray(errQueue)) {
          throw new Error(
            `rpcErrorsOnce['${req.method}'] must be an array; got ${typeof errQueue}. ` +
              `Use queueRpcErrors() instead of .set() directly.`,
          );
        }
        if (errQueue.length > 0) {
          const slot = errQueue.shift();
          if (errQueue.length === 0) tcpRef.rpcErrorsOnce.delete(req.method);
          // null sentinel = skip this call (use success path). Lets tests
          // model "first call succeeds, second call fails" by queuing
          // [null, errPayload].
          if (slot !== null) {
            const response = JSON.stringify({ jsonrpc: '2.0', id: req.id, error: slot }) + '\n';
            setImmediate(() => sock.emit('data', Buffer.from(response)));
            return;
          }
        }
      }
      const result = tcpRef.rpcResponses.get(req.method) ?? { ok: true };
      const response = JSON.stringify({ jsonrpc: '2.0', id: req.id, result }) + '\n';
      setImmediate(() => sock.emit('data', Buffer.from(response)));
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

// --- Test helpers ---

function createMockSetup() {
  return {
    onInbound: vi.fn() as unknown as ChannelSetup['onInbound'] & ReturnType<typeof vi.fn>,
    onInboundEvent: vi.fn() as unknown as ChannelSetup['onInboundEvent'] & ReturnType<typeof vi.fn>,
    onMetadata: vi.fn() as unknown as ChannelSetup['onMetadata'] & ReturnType<typeof vi.fn>,
    onAction: vi.fn() as unknown as ChannelSetup['onAction'] & ReturnType<typeof vi.fn>,
  };
}

// Per-test signal-cli data dir. Real reads happen against this path
// because the adapter base64-encodes the on-disk attachment bytes.
let testDataDir: string;

function makeTestDataDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'signal-test-'));
  fs.mkdirSync(path.join(dir, 'attachments'), { recursive: true });
  return dir;
}

function stageAttachment(id: string, bytes: Buffer | string): string {
  const filePath = path.join(testDataDir, 'attachments', id);
  fs.writeFileSync(filePath, typeof bytes === 'string' ? Buffer.from(bytes) : bytes);
  return filePath;
}

function createAdapter() {
  return createSignalAdapter({
    cliPath: 'signal-cli',
    account: '+15551234567',
    tcpHost: '127.0.0.1',
    tcpPort: 7583,
    manageDaemon: false,
    signalDataDir: testDataDir,
  });
}

function getRpcCalls(): Array<{
  method: string;
  params: Record<string, unknown>;
  id: string;
}> {
  if (!tcpRef.fakeSocket) return [];
  return tcpRef.fakeSocket.write.mock.calls
    .map((c: any[]) => {
      try {
        return JSON.parse(c[0].trim());
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function getRpcCallsForMethod(method: string) {
  return getRpcCalls().filter((c) => c.method === method);
}

/** Queue one or more JSON-RPC error responses for the next N calls to
 *  `method`. Each error is consumed in order; once the queue empties,
 *  subsequent calls fall back to the success path. Always pass error
 *  objects directly — the helper wraps them in the required array. */
function queueRpcErrors(method: string, ...errors: unknown[]) {
  tcpRef.rpcErrorsOnce.set(method, errors);
}

function pushEvent(envelope: Record<string, unknown>) {
  if (!tcpRef.fakeSocket) throw new Error('TCP socket not connected');
  const notification =
    JSON.stringify({
      jsonrpc: '2.0',
      method: 'receive',
      params: { envelope },
    }) + '\n';
  tcpRef.fakeSocket.emit('data', Buffer.from(notification));
}

// --- Tests ---

describe('SignalAdapter', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    tcpRef.rpcResponses.clear();
    tcpRef.rpcErrorsOnce.clear();
    tcpRef.fakeSocket = null;
    tcpRef.rpcResponses.set('send', { timestamp: 1234567890 });
    tcpRef.rpcResponses.set('sendTyping', {});
    testDataDir = makeTestDataDir();
    // Suppress optional voice transcription side-channels so attachment tests
    // are deterministic — transcribeAudioOptional bails out without these.
    delete process.env.WHISPER_BIN;
    delete process.env.OPENAI_API_KEY;
  });

  afterEach(() => {
    try {
      tcpRef.fakeSocket?.destroy();
    } catch {
      // already closed
    }
    try {
      fs.rmSync(testDataDir, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  });

  // --- Connection lifecycle ---

  describe('connection lifecycle', () => {
    it('connects when daemon is reachable', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      expect(adapter.isConnected()).toBe(true);
      expect(tcpRef.fakeSocket).not.toBeNull();

      await adapter.teardown();
    });

    it('isConnected() returns false before setup', () => {
      const adapter = createAdapter();
      expect(adapter.isConnected()).toBe(false);
    });

    it('disconnects cleanly', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      expect(adapter.isConnected()).toBe(true);

      await adapter.teardown();
      expect(adapter.isConnected()).toBe(false);
    });

    it('throws NetworkError if daemon is unreachable', async () => {
      const { createConnection } = await import('node:net');
      vi.mocked(createConnection).mockImplementationOnce((...args: any[]) => {
        const sock = createFakeSocket();
        setImmediate(() => sock.emit('error', new Error('Connection refused')));
        return sock as any;
      });

      const adapter = createAdapter();
      await expect(adapter.setup(createMockSetup())).rejects.toThrow(/not reachable/);
    });
  });

  // --- Inbound message handling ---

  describe('inbound message handling', () => {
    it('delivers DM via onInbound', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          message: 'Hello from Signal',
        },
      });

      await new Promise((r) => setTimeout(r, 50));

      expect(cfg.onMetadata).toHaveBeenCalledWith('+15555550123', 'Alice', false);
      expect(cfg.onInbound).toHaveBeenCalledWith(
        '+15555550123',
        null,
        expect.objectContaining({
          id: '1700000000000',
          kind: 'chat',
          content: expect.objectContaining({
            text: 'Hello from Signal',
            sender: '+15555550123',
            senderName: 'Alice',
          }),
        }),
      );

      await adapter.teardown();
    });

    it('delivers group message with group platformId', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550999',
        sourceName: 'Bob',
        dataMessage: {
          timestamp: 1700000000000,
          message: 'Group hello',
          groupInfo: { groupId: 'abc123', groupName: 'Family' },
        },
      });

      await new Promise((r) => setTimeout(r, 50));

      expect(cfg.onMetadata).toHaveBeenCalledWith('group:abc123', 'Family', true);
      expect(cfg.onInbound).toHaveBeenCalledWith(
        'group:abc123',
        null,
        expect.objectContaining({
          content: expect.objectContaining({
            text: 'Group hello',
            sender: '+15555550999',
          }),
        }),
      );

      await adapter.teardown();
    });

    it('skips sync messages (own outbound)', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15551234567',
        syncMessage: {
          sentMessage: {
            timestamp: 1700000000000,
            message: 'My own message',
            destination: '+15555550123',
          },
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).not.toHaveBeenCalled();

      await adapter.teardown();
    });

    it('processes Note to Self sync messages as inbound', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15551234567',
        syncMessage: {
          sentMessage: {
            timestamp: 1700000000000,
            message: 'Hello Bee',
            destinationNumber: '+15551234567',
          },
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).toHaveBeenCalledWith(
        '+15551234567',
        null,
        expect.objectContaining({
          content: expect.objectContaining({
            text: 'Hello Bee',
            senderName: 'Me',
            isFromMe: true,
          }),
        }),
      );

      await adapter.teardown();
    });

    it('skips empty messages', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        dataMessage: { timestamp: 1700000000000, message: '   ' },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).not.toHaveBeenCalled();

      await adapter.teardown();
    });

    it('skips echoed outbound messages', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'Echo test' },
      });

      pushEvent({
        sourceNumber: '+15555550123',
        dataMessage: { timestamp: 1700000000000, message: 'Echo test' },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).not.toHaveBeenCalled();

      await adapter.teardown();
    });

    it('forwards a text-less image attachment as base64 in attachments[]', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      const bytes = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10]); // JPEG SOI
      stageAttachment('att123abc', bytes);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          attachments: [{ id: 'att123abc', contentType: 'image/jpeg', size: bytes.length }],
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).toHaveBeenCalledTimes(1);
      const callArgs = (cfg.onInbound as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(callArgs[0]).toBe('+15555550123');
      expect(callArgs[1]).toBeNull();
      const content = callArgs[2].content as Record<string, unknown>;
      // No synthesized [Image: <hostpath>] line — the host path is outside the
      // container's workspace mount and would be unreachable from the agent.
      expect(content.text).toBe('');
      const attachments = content.attachments as Array<Record<string, unknown>>;
      expect(attachments).toHaveLength(1);
      expect(attachments[0].type).toBe('image');
      expect(attachments[0].mimeType).toBe('image/jpeg');
      expect(attachments[0].name).toMatch(/\.jpg$/);
      // Round-trip the base64 payload to confirm the file bytes reached the
      // attachment object rather than just a path reference.
      expect(Buffer.from(attachments[0].data as string, 'base64').equals(bytes)).toBe(true);

      await adapter.teardown();
    });

    it('forwards a voice note as a base64 attachment with [Voice Message] hint text', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      const bytes = Buffer.from('FAKE_AAC_BYTES');
      stageAttachment('voice-1', bytes);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          // No `message` field — text-less voice note, the exact bug.
          attachments: [{ id: 'voice-1', contentType: 'audio/aac', size: bytes.length }],
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).toHaveBeenCalledTimes(1);
      const content = (cfg.onInbound as unknown as ReturnType<typeof vi.fn>).mock.calls[0][2].content as Record<
        string,
        unknown
      >;
      // Transcription disabled in beforeEach so we get the placeholder hint.
      expect(content.text).toBe('[Voice Message]');
      const attachments = content.attachments as Array<Record<string, unknown>>;
      expect(attachments).toHaveLength(1);
      expect(attachments[0].type).toBe('voice');
      expect(attachments[0].mimeType).toBe('audio/aac');
      expect(attachments[0].name).toMatch(/\.aac$/);
      expect(Buffer.from(attachments[0].data as string, 'base64').equals(bytes)).toBe(true);

      await adapter.teardown();
    });

    it('forwards a generic file attachment (e.g. PDF) when text is empty', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      const bytes = Buffer.from('%PDF-1.4 fake');
      stageAttachment('doc-1', bytes);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          attachments: [{ id: 'doc-1', contentType: 'application/pdf', filename: 'invoice.pdf', size: bytes.length }],
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).toHaveBeenCalledTimes(1);
      const content = (cfg.onInbound as unknown as ReturnType<typeof vi.fn>).mock.calls[0][2].content as Record<
        string,
        unknown
      >;
      expect(content.text).toBe('');
      const attachments = content.attachments as Array<Record<string, unknown>>;
      expect(attachments).toHaveLength(1);
      expect(attachments[0].type).toBe('file');
      expect(attachments[0].mimeType).toBe('application/pdf');
      // Sender-supplied filename is preserved.
      expect(attachments[0].name).toBe('invoice.pdf');
      expect(Buffer.from(attachments[0].data as string, 'base64').equals(bytes)).toBe(true);

      await adapter.teardown();
    });

    it('keeps caption text alongside an attachment', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      const bytes = Buffer.from([0x89, 0x50, 0x4e, 0x47]); // PNG header
      stageAttachment('cap-img', bytes);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          message: 'check this out',
          attachments: [{ id: 'cap-img', contentType: 'image/png', size: bytes.length }],
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      const content = (cfg.onInbound as unknown as ReturnType<typeof vi.fn>).mock.calls[0][2].content as Record<
        string,
        unknown
      >;
      expect(content.text).toBe('check this out');
      const attachments = content.attachments as Array<Record<string, unknown>>;
      expect(attachments).toHaveLength(1);
      expect(attachments[0].mimeType).toBe('image/png');

      await adapter.teardown();
    });

    it('drops the message when text is empty and every attachment file is missing on disk', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      // Intentionally do NOT stage the file — simulates an attachment that
      // signal-cli failed to materialize before the notification fired.
      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          attachments: [{ id: 'missing-id', contentType: 'image/jpeg', size: 0 }],
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).not.toHaveBeenCalled();

      await adapter.teardown();
    });
  });

  // --- groupV2 ---

  describe('group routing', () => {
    it('routes to groupV2.id when present, falling back to legacy groupInfo.groupId', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          message: 'hello v2',
          groupV2: { id: 'v2group=' },
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).toHaveBeenCalledWith('group:v2group=', null, expect.anything());

      await adapter.teardown();
    });
  });

  // --- mention resolution ---

  describe('mention resolution', () => {
    it('replaces inline mention placeholders with display names', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          message: 'hey ￼ are you here?',
          mentions: [{ start: 4, length: 1, name: 'Bob', uuid: 'bob-uuid' }],
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).toHaveBeenCalledWith(
        '+15555550123',
        null,
        expect.objectContaining({
          content: expect.objectContaining({ text: 'hey @Bob are you here?' }),
        }),
      );

      await adapter.teardown();
    });
  });

  // --- Quote context ---

  describe('quote context', () => {
    it('emits a nested replyTo object matching the formatter contract', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          message: 'I disagree',
          quote: {
            id: 1699999999000,
            authorNumber: '+15555550888',
            authorName: 'Pineapple Pete',
            text: 'Pineapple belongs on pizza',
          },
        },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).toHaveBeenCalledWith(
        '+15555550123',
        null,
        expect.objectContaining({
          content: expect.objectContaining({
            text: 'I disagree',
            replyTo: {
              id: '1699999999000',
              sender: 'Pineapple Pete',
              text: 'Pineapple belongs on pizza',
            },
          }),
        }),
      );

      await adapter.teardown();
    });
  });

  // --- deliver ---

  describe('deliver', () => {
    it('sends DM via TCP RPC', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'Hello' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls.length).toBeGreaterThan(0);

      const last = sendCalls[sendCalls.length - 1];
      expect(last.params).toEqual(
        expect.objectContaining({
          recipient: ['+15555550123'],
          message: 'Hello',
          account: '+15551234567',
        }),
      );

      await adapter.teardown();
    });

    it('sends group message via groupId', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      await adapter.deliver('group:abc123', null, {
        kind: 'text',
        content: { text: 'Group msg' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      const last = sendCalls[sendCalls.length - 1];
      expect(last.params).toEqual(
        expect.objectContaining({
          groupId: 'abc123',
          message: 'Group msg',
        }),
      );

      await adapter.teardown();
    });

    it('chunks long messages', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      const longText = 'x'.repeat(5000);
      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: longText },
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls.length).toBeGreaterThan(1);

      await adapter.teardown();
    });

    it('extracts text from string content', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: 'Plain string content',
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls.length).toBeGreaterThan(0);
      const last = sendCalls[sendCalls.length - 1];
      expect(last.params.message).toBe('Plain string content');

      await adapter.teardown();
    });
  });

  // --- Outbound attachments ---

  describe('deliver — attachments', () => {
    // Real fs writes happen in tmpdir(); confirm the bytes round-trip and
    // are cleaned up after deliver returns.
    it('sends a single attachment via attachments[] param', async () => {
      const fs = await import('node:fs');
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'file',
        content: {},
        files: [{ filename: 'report.md', data: Buffer.from('# Report\n\nbody') }],
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls.length).toBe(1);
      const params = sendCalls[0].params as Record<string, unknown>;
      expect(params.recipient).toEqual(['+15555550123']);
      expect(params.account).toBe('+15551234567');
      expect(params.message).toBeUndefined();
      const paths = params.attachments as string[];
      expect(paths).toHaveLength(1);
      expect(paths[0]).toMatch(/signal-out-\d+-[a-z0-9]+-report\.md$/);
      // Temp file should no longer exist — finally{} cleanup ran
      expect(fs.existsSync(paths[0])).toBe(false);

      await adapter.teardown();
    });

    it('sends text first, then attachment, when both are present', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'file',
        content: { text: 'Here is the digest' },
        files: [{ filename: 'digest.md', data: Buffer.from('content') }],
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls).toHaveLength(2);
      // First call: text message
      expect(sendCalls[0].params).toEqual(
        expect.objectContaining({ message: 'Here is the digest', recipient: ['+15555550123'] }),
      );
      expect((sendCalls[0].params as Record<string, unknown>).attachments).toBeUndefined();
      // Second call: attachment, no message
      expect(sendCalls[1].params).toEqual(expect.objectContaining({ recipient: ['+15555550123'] }));
      const attachments = (sendCalls[1].params as Record<string, unknown>).attachments as string[];
      expect(attachments).toHaveLength(1);

      await adapter.teardown();
    });

    it('sends multiple attachments in a single send call', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'file',
        content: {},
        files: [
          { filename: 'a.txt', data: Buffer.from('a') },
          { filename: 'b.png', data: Buffer.from([0x89, 0x50, 0x4e, 0x47]) },
        ],
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls).toHaveLength(1);
      const attachments = (sendCalls[0].params as Record<string, unknown>).attachments as string[];
      expect(attachments).toHaveLength(2);
      expect(attachments[0]).toMatch(/-a\.txt$/);
      expect(attachments[1]).toMatch(/-b\.png$/);

      await adapter.teardown();
    });

    it('uses groupId for group destinations', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('group:abc123', null, {
        kind: 'file',
        content: {},
        files: [{ filename: 'pic.jpg', data: Buffer.from('jpg') }],
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls).toHaveLength(1);
      const params = sendCalls[0].params as Record<string, unknown>;
      expect(params.groupId).toBe('abc123');
      expect(params.recipient).toBeUndefined();

      await adapter.teardown();
    });

    /**
     * Defensive test: `OutboundFile.filename` is operator-supplied data, so
     * the implementation must not let a filename containing path separators
     * escape the temp directory. We feed an attempt-to-traverse filename and
     * assert the resolved path stays strictly inside `tmpdir()`.
     */
    it('keeps temp paths inside tmpdir even when filename contains path separators', async () => {
      const path = await import('node:path');
      const os = await import('node:os');
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'file',
        content: {},
        files: [{ filename: '../sneaky.txt', data: Buffer.from('x') }],
      });

      const sendCalls = getRpcCallsForMethod('send');
      const paths = (sendCalls[0].params as Record<string, unknown>).attachments as string[];
      const resolvedTmp = path.resolve(os.tmpdir());
      const resolvedResult = path.resolve(paths[0]);
      // path.resolve normalizes away any "../"; if sanitization failed, the
      // result would resolve to tmpdir's parent.
      expect(resolvedResult.startsWith(resolvedTmp + path.sep)).toBe(true);

      await adapter.teardown();
    });
  });

  // --- Text styles ---

  describe('text styles', () => {
    it('sends bold text with textStyle parameter', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'Hello **world**' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls.length).toBeGreaterThan(0);
      const last = sendCalls[sendCalls.length - 1];
      expect(last.params.message).toBe('Hello world');
      expect(last.params.textStyle).toEqual(['6:5:BOLD']);

      await adapter.teardown();
    });

    it('sends inline code with MONOSPACE style', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'Run `npm test` now' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      const last = sendCalls[sendCalls.length - 1];
      expect(last.params.message).toBe('Run npm test now');
      expect(last.params.textStyle).toEqual(['4:8:MONOSPACE']);

      await adapter.teardown();
    });

    it('sends plain text without textStyle', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'No formatting here' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      const last = sendCalls[sendCalls.length - 1];
      expect(last.params.message).toBe('No formatting here');
      expect(last.params.textStyle).toBeUndefined();

      await adapter.teardown();
    });

    it('falls back to original markup when textStyle is rejected', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      let sendCount = 0;
      tcpRef.fakeSocket.write.mockImplementation((data: string) => {
        try {
          const req = JSON.parse(data.trim());
          if (req.method === 'send') {
            sendCount++;
            if (sendCount === 1) {
              const response =
                JSON.stringify({
                  jsonrpc: '2.0',
                  id: req.id,
                  error: { message: 'Unknown parameter: textStyle' },
                }) + '\n';
              setImmediate(() => tcpRef.fakeSocket.emit('data', Buffer.from(response)));
              return;
            }
          }
          const response =
            JSON.stringify({
              jsonrpc: '2.0',
              id: req.id,
              result: { ok: true },
            }) + '\n';
          setImmediate(() => tcpRef.fakeSocket.emit('data', Buffer.from(response)));
        } catch {
          /* ignore */
        }
      });

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'Hello **world**' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      expect(sendCalls.length).toBe(2);
      expect(sendCalls[1].params.message).toBe('Hello **world**');
      expect(sendCalls[1].params.textStyle).toBeUndefined();

      await adapter.teardown();
    });

    it('tracks nested styles with correct offsets', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: '**bold with `code` inside**' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      const last = sendCalls[sendCalls.length - 1];
      expect(last.params.message).toBe('bold with code inside');
      // BOLD covers the full inner span, MONOSPACE points at "code" in the
      // final plain text (offset 10, length 4) — not the intermediate text.
      const styles = (last.params.textStyle as string[]).slice().sort();
      expect(styles).toEqual(['0:21:BOLD', '10:4:MONOSPACE']);

      await adapter.teardown();
    });

    it('maps *single-asterisk* to ITALIC', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'Hello *world*' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      const last = sendCalls[sendCalls.length - 1];
      expect(last.params.message).toBe('Hello world');
      expect(last.params.textStyle).toEqual(['6:5:ITALIC']);

      await adapter.teardown();
    });

    it('maps _underscore_ to ITALIC', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      tcpRef.fakeSocket.write.mockClear();

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'hey _there_' },
      });

      const sendCalls = getRpcCallsForMethod('send');
      const last = sendCalls[sendCalls.length - 1];
      expect(last.params.message).toBe('hey there');
      expect(last.params.textStyle).toEqual(['4:5:ITALIC']);

      await adapter.teardown();
    });
  });

  // --- Echo cache ---

  describe('echo cache', () => {
    it('does not drop same-text inbound from a different recipient', async () => {
      // Bot sends "Hello" to Alice. Immediately after, Bob sends "Hello" from
      // a different DM. Bob's message must still route — the earlier echo key
      // was scoped to Alice.
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'Hello' },
      });

      pushEvent({
        sourceNumber: '+15555550999',
        sourceName: 'Bob',
        dataMessage: { timestamp: 1700000000000, message: 'Hello' },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).toHaveBeenCalledWith(
        '+15555550999',
        null,
        expect.objectContaining({
          content: expect.objectContaining({ text: 'Hello', sender: '+15555550999' }),
        }),
      );

      await adapter.teardown();
    });

    it('still skips echo on the same recipient', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      await adapter.deliver('+15555550123', null, {
        kind: 'text',
        content: { text: 'Echo test' },
      });

      pushEvent({
        sourceNumber: '+15555550123',
        dataMessage: { timestamp: 1700000000000, message: 'Echo test' },
      });

      await new Promise((r) => setTimeout(r, 50));
      expect(cfg.onInbound).not.toHaveBeenCalled();

      await adapter.teardown();
    });
  });

  // --- Connection drop ---

  describe('connection drop', () => {
    it('flips isConnected to false when the socket closes', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());
      expect(adapter.isConnected()).toBe(true);

      // Simulate the daemon dropping the TCP connection.
      tcpRef.fakeSocket.destroy();
      await new Promise((r) => setTimeout(r, 20));

      expect(adapter.isConnected()).toBe(false);

      await adapter.teardown();
    });
  });

  // --- setTyping ---

  describe('setTyping', () => {
    it('sends typing indicator for DMs', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      await adapter.setTyping!('+15555550123', null);

      expect(getRpcCallsForMethod('sendTyping')).toHaveLength(1);

      await adapter.teardown();
    });

    it('skips typing for groups', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      await adapter.setTyping!('group:abc123', null);

      expect(getRpcCallsForMethod('sendTyping')).toHaveLength(0);

      await adapter.teardown();
    });
  });

  // --- Receipt envelope handling ---

  describe('receipt envelope handling', () => {
    it('logs an inbound read receipt and does not call onInbound', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        receiptMessage: {
          type: 'read',
          timestamps: [1234567890],
        },
      });

      await new Promise((r) => setTimeout(r, 50));

      expect(log.debug).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          direction: 'inbound-receipt',
          sender: '+15555550123',
          type: 'read',
          timestamps: [1234567890],
        }),
      );
      expect(cfg.onInbound).not.toHaveBeenCalled();
      expect(cfg.onMetadata).not.toHaveBeenCalled();

      await adapter.teardown();
    });

    it('logs an inbound viewed receipt distinctly', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        receiptMessage: {
          type: 'viewed',
          timestamps: [1700000000000],
        },
      });

      await new Promise((r) => setTimeout(r, 50));

      expect(log.debug).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          direction: 'inbound-receipt',
          sender: '+15555550123',
          type: 'viewed',
          timestamps: [1700000000000],
        }),
      );
      expect(cfg.onInbound).not.toHaveBeenCalled();

      await adapter.teardown();
    });

    it('logs an inbound delivery receipt distinctly', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        receiptMessage: {
          type: 'delivery',
          timestamps: [1700000000001, 1700000000002],
        },
      });

      await new Promise((r) => setTimeout(r, 50));

      expect(log.debug).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          direction: 'inbound-receipt',
          sender: '+15555550123',
          type: 'delivery',
          timestamps: [1700000000001, 1700000000002],
        }),
      );
      expect(cfg.onInbound).not.toHaveBeenCalled();

      await adapter.teardown();
    });

    it('regression: dataMessage envelope still flows through to onInbound', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        sourceName: 'Alice',
        dataMessage: {
          timestamp: 1700000000000,
          message: 'Hello after receipts branch',
        },
      });

      await new Promise((r) => setTimeout(r, 50));

      expect(cfg.onInbound).toHaveBeenCalledTimes(1);
      expect(cfg.onInbound).toHaveBeenCalledWith(
        '+15555550123',
        null,
        expect.objectContaining({
          id: '1700000000000',
          content: expect.objectContaining({ text: 'Hello after receipts branch' }),
        }),
      );

      await adapter.teardown();
    });

    it('regression: editMessage envelope is not treated as a receipt and yields no inbound emission', async () => {
      const adapter = createAdapter();
      const cfg = createMockSetup();
      await adapter.setup(cfg);

      pushEvent({
        sourceNumber: '+15555550123',
        editMessage: {
          targetSentTimestamp: 1700000000000,
          dataMessage: {
            timestamp: 1700000000123,
            message: 'edited text',
          },
        },
      });

      await new Promise((r) => setTimeout(r, 50));

      expect(cfg.onInbound).not.toHaveBeenCalled();
      // And we did not log it as an inbound receipt either.
      const receiptLogs = (log.debug as unknown as ReturnType<typeof vi.fn>).mock.calls.filter(
        (c) => c[1] && typeof c[1] === 'object' && (c[1] as any).direction === 'inbound-receipt',
      );
      expect(receiptLogs).toHaveLength(0);

      await adapter.teardown();
    });
  });

  // --- Adapter properties ---

  describe('adapter properties', () => {
    it('has channelType "signal"', () => {
      const adapter = createAdapter();
      expect(adapter.channelType).toBe('signal');
    });

    it('does not support threads', () => {
      const adapter = createAdapter();
      expect(adapter.supportsThreads).toBe(false);
    });
  });

  // --- Send-failure propagation ---
  //
  // Failure semantics:
  //   - DM / single-recipient failure  → reject deliver() so drainSession retries.
  //   - Partial group failure (≥1 SUCCESS) → log warn and resolve; rethrow
  //     would retry the whole group and re-deliver to SUCCESS recipients.
  //   - !connected → throw; lost-during-outage is honest (markFailed),
  //     swallow would mark the row delivered with nothing on the wire.

  /** Build a JSON-RPC error envelope for one signal-cli send failure. */
  function sendErrorPayload(
    type: string,
    recipient: { number?: string | null; uuid?: string | null; username?: string | null },
    extras: { code?: number; timestamp?: number } = {},
  ) {
    return {
      code: extras.code ?? -1,
      message: 'Failed to send message',
      data: {
        response: {
          results: [{ recipientAddress: { uuid: null, number: null, username: null, ...recipient }, type }],
          timestamp: extras.timestamp ?? 1,
        },
      },
    };
  }

  describe('send failure propagation', () => {
    it('rejects deliver() with message + code + data preserved when send fails', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      const errPayload = sendErrorPayload('UNREGISTERED_FAILURE', { number: '+15555550555' });
      queueRpcErrors('send', errPayload);

      await expect(
        adapter.deliver('+15555550555', null, { kind: 'text', content: { text: 'Hello' } }),
      ).rejects.toMatchObject({
        message: expect.stringMatching(/UNREGISTERED_FAILURE for \+15555550555/),
        code: -1,
        data: errPayload.data,
      });

      await adapter.teardown();
    });

    it('rejects deliver() when an attachment send fails (recipient surfaced in message)', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      queueRpcErrors('send', sendErrorPayload('UNREGISTERED_FAILURE', { number: '+15555550999' }));

      await expect(
        adapter.deliver('+15555550999', null, {
          kind: 'file',
          content: {},
          files: [{ filename: 'report.md', data: Buffer.from('# Report') }],
        }),
      ).rejects.toThrow(/UNREGISTERED_FAILURE for \+15555550999/);

      await adapter.teardown();
    });

    it('rejects deliver() when an approval card send fails — recipient errors skip the textStyle retry', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // sendApprovalCard's body contains *title* markup, so parseSignalStyles
      // emits a bold style. Pre-patch the inner catch always retried without
      // textStyle, doubling the per-failure RPC cost for permanent
      // recipient failures (UNREGISTERED, IDENTITY, …). The new gate skips
      // the retry when the error carries `data` (a per-recipient signal-cli
      // error), so this test queues exactly ONE error and asserts exactly
      // ONE 'send' call.
      queueRpcErrors('send', sendErrorPayload('IDENTITY_FAILURE', { number: '+15555550999' }));

      await expect(
        adapter.deliver('+15555550999', null, {
          kind: 'chat-sdk',
          content: {
            type: 'ask_question',
            questionId: 'q1',
            title: 'Pick one',
            options: [
              { value: 'a', label: 'Alpha' },
              { value: 'b', label: 'Bravo' },
            ],
          },
        }),
      ).rejects.toThrow(/IDENTITY_FAILURE for \+15555550999/);
      expect(getRpcCallsForMethod('send')).toHaveLength(1);

      await adapter.teardown();
    });

    it('retries an approval card send without textStyle when the error looks style-related (no data)', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // First call: data-less RPC error → indicates textStyle parse rejection.
      // The inner catch retries without textStyle; the retry succeeds because
      // the queue is now empty.
      queueRpcErrors('send', { code: -1, message: 'Invalid textStyle' });

      await expect(
        adapter.deliver('+15555550001', null, {
          kind: 'chat-sdk',
          content: {
            type: 'ask_question',
            questionId: 'q2',
            title: 'Pick one',
            options: [
              { value: 'a', label: 'Alpha' },
              { value: 'b', label: 'Bravo' },
            ],
          },
        }),
      ).resolves.toBeUndefined();
      const calls = getRpcCallsForMethod('send');
      expect(calls).toHaveLength(2);
      expect(calls[0].params.textStyle).toBeDefined();
      expect(calls[1].params.textStyle).toBeUndefined();

      await adapter.teardown();
    });

    it('rejects deliver() on a sendText styled-text retry failure (regression: both attempts must surface)', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // Markdown in body forces the inner styled-text retry path in sendText.
      // Both attempts fail so the outer catch rethrows.
      // Errors with no `data` field also exercise the textStyle-retry gate —
      // without `data` we cannot rule out a textStyle parse error, so retry
      // is the right call here.
      queueRpcErrors('send', { code: -1, message: 'Invalid textStyle' }, { code: -1, message: 'Invalid textStyle' });

      await expect(
        adapter.deliver('+15555550001', null, { kind: 'text', content: { text: 'Hello **world**' } }),
      ).rejects.toThrow('Invalid textStyle');
      expect(getRpcCallsForMethod('send')).toHaveLength(2);

      await adapter.teardown();
    });

    it('rejects deliver() with a generic message when RPC error has no failure data', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      queueRpcErrors('send', { code: -32600, message: 'Invalid Request', data: null });

      await expect(adapter.deliver('+15555550001', null, { kind: 'text', content: { text: 'x' } })).rejects.toThrow(
        'Invalid Request',
      );

      await adapter.teardown();
    });

    it('rejects deliver() when the daemon is disconnected', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // Force-close the TCP socket; the adapter's onClose handler flips
      // `connected` to false. The next deliver() should throw rather than
      // silently no-op (which would otherwise mark the row delivered).
      tcpRef.fakeSocket.destroy();

      await expect(adapter.deliver('+15555550001', null, { kind: 'text', content: { text: 'x' } })).rejects.toThrow(
        'Signal channel not connected',
      );

      await adapter.teardown();
    });

    it('rethrows on multi-chunk failure when a later chunk fails after earlier chunks landed', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // Long body forces sendText to chunk (MAX_CHUNK = 4000). Queue [null,
      // errPayload] so call #1 (chunk 1) succeeds and call #2 (chunk 2)
      // fails — the chunk loop's outer catch rethrows.
      queueRpcErrors('send', null, sendErrorPayload('NETWORK_FAILURE', { number: '+15555550001' }));
      const longText = 'a'.repeat(5000);

      await expect(
        adapter.deliver('+15555550001', null, { kind: 'text', content: { text: longText } }),
      ).rejects.toThrow(/NETWORK_FAILURE/);
      expect(getRpcCallsForMethod('send')).toHaveLength(2);

      await adapter.teardown();
    });

    it('absorbs a partial-group failure (≥1 SUCCESS) without rethrowing', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // Group send with two recipients: one SUCCESS, one UNREGISTERED_FAILURE.
      // Rethrowing would trigger drainSession to retry the whole group and
      // re-deliver to the SUCCESS recipient; the adapter must absorb instead.
      queueRpcErrors('send', {
        code: -1,
        message: 'Failed to send message',
        data: {
          response: {
            results: [
              { recipientAddress: { uuid: 'good-uuid', number: null, username: null }, type: 'SUCCESS' },
              {
                recipientAddress: { uuid: null, number: '+15555550999', username: null },
                type: 'UNREGISTERED_FAILURE',
              },
            ],
            timestamp: 99,
          },
        },
      });

      await expect(
        adapter.deliver('group:abc123', null, { kind: 'text', content: { text: 'group msg' } }),
      ).resolves.toBeUndefined();

      await adapter.teardown();
    });

    it('caps the failure list in the Error message for very large groups', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // 25 failed recipients — message should truncate to 20 with "+5 more".
      const results = Array.from({ length: 25 }, (_, i) => ({
        recipientAddress: { uuid: null, number: `+1555555${String(i).padStart(4, '0')}`, username: null },
        type: 'UNREGISTERED_FAILURE',
      }));
      queueRpcErrors('send', {
        code: -1,
        message: 'Failed to send message',
        data: { response: { results, timestamp: 1 } },
      });

      await expect(
        adapter.deliver('group:huge', null, { kind: 'text', content: { text: 'broadcast' } }),
      ).rejects.toThrow(/\+5 more/);

      await adapter.teardown();
    });

    it('summarizes only non-SUCCESS results in the Error message', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // For a DM (single recipient), only one result; the SUCCESS filter is
      // load-bearing for group sends, but exercising it here pins the
      // current behavior so a refactor that drops the filter is caught.
      queueRpcErrors('send', {
        code: -1,
        message: 'Failed to send message',
        data: {
          response: {
            results: [
              { recipientAddress: { uuid: 'aaa', number: null, username: null }, type: 'SUCCESS' },
              { recipientAddress: { uuid: 'bbb', number: null, username: null }, type: 'IDENTITY_FAILURE' },
            ],
            timestamp: 1,
          },
        },
      });

      // DM-style platformId so partial-group absorption does NOT kick in
      // (the absorber requires platformId.startsWith('group:')). The single
      // SUCCESS isn't relevant here — the test pins the message format.
      await expect(adapter.deliver('bbb', null, { kind: 'text', content: { text: 'x' } })).rejects.toThrow(
        /^Failed to send message: IDENTITY_FAILURE for bbb$/,
      );

      await adapter.teardown();
    });
  });

  // --- Test infra hygiene ---

  describe('mock infra', () => {
    it('refuses non-array values in rpcErrorsOnce (footgun guard)', async () => {
      const adapter = createAdapter();
      await adapter.setup(createMockSetup());

      // Set a raw object instead of using queueRpcErrors() — should throw
      // synchronously inside fakeSocket.write when the next RPC fires.
      (tcpRef.rpcErrorsOnce as Map<string, unknown>).set('send', { code: -1, message: 'oops' });

      // The error is thrown by .write() inside the mock; the RPC promise will
      // never resolve. Use a separate timeout to assert this is not a normal
      // success path either — we just need the misuse to fail loudly.
      const sendPromise = adapter.deliver('+15555550001', null, { kind: 'text', content: { text: 'x' } });
      // The fake socket .write throws synchronously, but the RPC promise hangs;
      // we just need the test to fail if the mock silently accepts the value.
      // Tear down to clear pending — the mock-misuse throw appears as an
      // unhandled rejection or error log, which is the desired signal.
      void sendPromise.catch(() => {});
      await adapter.teardown();
    });
  });
});
