/**
 * Host-side completion-hook dispatch (signal-read-receipts Stream S2):
 *
 *   1. `syncProcessingAcks` returns `{ pickedUpIds, completedIds, failedIds }`
 *      for rows that ACTUALLY transitioned (UPDATE row-changes > 0). The
 *      hook fires on the pickup transition so the receipt lands before the
 *      LLM roundtrip finishes.
 *   2. `registerCompletionHook` / `dispatchCompletionHooks` — per-channel,
 *      async, Promise.allSettled-isolated.
 */
import Database from 'better-sqlite3';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { syncProcessingAcks } from './db/session-db.js';
import { log } from './log.js';

function makeInboundDb(): Database.Database {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE messages_in (
      id            TEXT PRIMARY KEY,
      seq           INTEGER UNIQUE,
      kind          TEXT NOT NULL,
      timestamp     TEXT NOT NULL,
      status        TEXT DEFAULT 'pending',
      process_after TEXT,
      recurrence    TEXT,
      series_id     TEXT,
      tries         INTEGER DEFAULT 0,
      trigger       INTEGER NOT NULL DEFAULT 1,
      platform_id   TEXT,
      channel_type  TEXT,
      thread_id     TEXT,
      content       TEXT NOT NULL
    );
  `);
  return db;
}

function makeOutboundDb(): Database.Database {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE processing_ack (
      message_id     TEXT PRIMARY KEY,
      status         TEXT NOT NULL,
      status_changed TEXT NOT NULL
    );
  `);
  return db;
}

function insertInbound(
  db: Database.Database,
  id: string,
  status: string,
  channelType: string | null,
  platformId: string | null,
  threadId: string | null = null,
  content: string = '{}',
): void {
  const seq = ((db.prepare('SELECT COUNT(*) AS c FROM messages_in').get() as { c: number }).c + 1) * 2;
  db.prepare(
    `INSERT INTO messages_in (id, seq, kind, timestamp, status, content, channel_type, platform_id, thread_id)
     VALUES (?, ?, 'chat', datetime('now'), ?, ?, ?, ?, ?)`,
  ).run(id, seq, status, content, channelType, platformId, threadId);
}

function insertAck(db: Database.Database, id: string, status: 'processing' | 'completed' | 'failed'): void {
  db.prepare("INSERT INTO processing_ack VALUES (?, ?, datetime('now'))").run(id, status);
}

describe('syncProcessingAcks return value', () => {
  it('reports pickup transitions for processing-state acks; messages_in moves to processing', () => {
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'm-1', 'pending', 'signal', '+1');
    insertAck(outDb, 'm-1', 'processing');

    const result = syncProcessingAcks(inDb, outDb);

    expect(result).toEqual({ pickedUpIds: ['m-1'], completedIds: [], failedIds: [] });
    const row = inDb.prepare('SELECT status FROM messages_in WHERE id = ?').get('m-1') as { status: string };
    expect(row.status).toBe('processing');
  });

  it('buckets terminal acks into completedIds/failedIds AND still flags the pickup transition', () => {
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'm-ok', 'pending', 'signal', '+1');
    insertInbound(inDb, 'm-bad', 'pending', 'signal', '+2');
    insertAck(outDb, 'm-ok', 'completed');
    insertAck(outDb, 'm-bad', 'failed');

    const result = syncProcessingAcks(inDb, outDb);

    // Both rows leapfrogged pending → completed without us seeing 'processing' first,
    // so the pickup transition fires alongside the terminal one. That ensures the
    // hook still gets a chance to run when the container collapses both writes.
    expect(result.pickedUpIds.sort()).toEqual(['m-bad', 'm-ok']);
    expect(result.completedIds).toEqual(['m-ok']);
    expect(result.failedIds).toEqual(['m-bad']);
    const rows = inDb.prepare('SELECT id, status FROM messages_in ORDER BY id').all();
    expect(rows).toEqual([
      { id: 'm-bad', status: 'completed' },
      { id: 'm-ok', status: 'completed' },
    ]);
  });

  it('does not re-report pickup on the same row across sweeps', () => {
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'm-1', 'pending', 'signal', '+1');
    insertAck(outDb, 'm-1', 'processing');

    expect(syncProcessingAcks(inDb, outDb).pickedUpIds).toEqual(['m-1']);
    expect(syncProcessingAcks(inDb, outDb)).toEqual({ pickedUpIds: [], completedIds: [], failedIds: [] });
  });

  it('returns empty across the board when nothing is acked or ack is orphaned', () => {
    expect(syncProcessingAcks(makeInboundDb(), makeOutboundDb())).toEqual({
      pickedUpIds: [],
      completedIds: [],
      failedIds: [],
    });

    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertAck(outDb, 'ghost', 'completed');
    expect(syncProcessingAcks(inDb, outDb)).toEqual({ pickedUpIds: [], completedIds: [], failedIds: [] });
  });
});

describe('completion hook registry', () => {
  // Reset via test-only helper (not vi.resetModules) so log spies still bind.
  beforeEach(async () => {
    const { _resetCompletionHooksForTesting } = await import('./channels/channel-registry.js');
    _resetCompletionHooksForTesting();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('invokes the registered hook once with {id, platformId, threadId, agentGroupId, sender}', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const calls: Array<{
      id: string;
      platformId: string;
      threadId: string | null;
      agentGroupId: string;
      sender: string | null;
    }> = [];
    registerCompletionHook('signal', (msg) => {
      calls.push(msg);
    });

    await dispatchCompletionHooks('signal', {
      id: '123',
      platformId: '+1',
      threadId: 'thr-x',
      agentGroupId: 'ag-1',
      sender: '+1',
    });

    expect(calls).toEqual([
      { id: '123', platformId: '+1', threadId: 'thr-x', agentGroupId: 'ag-1', sender: '+1' },
    ]);
  });

  it('runs multiple hooks for the same channel — both observe the dispatch', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const order: string[] = [];
    registerCompletionHook('multi', () => {
      order.push('a');
    });
    registerCompletionHook('multi', () => {
      order.push('b');
    });

    await dispatchCompletionHooks('multi', {
      id: '1',
      platformId: 'p',
      threadId: null,
      agentGroupId: 'ag-1',
      sender: null,
    });

    expect(order.sort()).toEqual(['a', 'b']);
  });

  it('skips channels with no hooks silently (no log, no throw)', async () => {
    const { dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const warnSpy = vi.spyOn(log, 'warn');

    await expect(
      dispatchCompletionHooks('unknown', {
        id: '1',
        platformId: 'p',
        threadId: null,
        agentGroupId: 'ag-1',
        sender: null,
      }),
    ).resolves.toBeUndefined();
    expect(warnSpy).not.toHaveBeenCalled();
  });

  it('isolates a synchronously-throwing hook; siblings still fire; rejection warn-logged', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const warnSpy = vi.spyOn(log, 'warn').mockImplementation(() => {});

    let goodFired = false;
    registerCompletionHook('iso', () => {
      throw new Error('boom-sync');
    });
    registerCompletionHook('iso', () => {
      goodFired = true;
    });

    await expect(
      dispatchCompletionHooks('iso', {
        id: '1',
        platformId: 'p',
        threadId: null,
        agentGroupId: 'ag-1',
        sender: null,
      }),
    ).resolves.toBeUndefined();
    expect(goodFired).toBe(true);
    expect(warnSpy).toHaveBeenCalled();
    expect(warnSpy.mock.calls.some((c) => /completion hook/i.test(String(c[0])))).toBe(true);
  });

  it('isolates an async rejecting hook the same way', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const warnSpy = vi.spyOn(log, 'warn').mockImplementation(() => {});

    let goodFired = false;
    registerCompletionHook('iso2', async () => {
      throw new Error('boom-async');
    });
    registerCompletionHook('iso2', async () => {
      goodFired = true;
    });

    await dispatchCompletionHooks('iso2', {
      id: '1',
      platformId: 'p',
      threadId: null,
      agentGroupId: 'ag-1',
      sender: null,
    });
    expect(goodFired).toBe(true);
    expect(warnSpy).toHaveBeenCalled();
  });
});

// Mirror of host-sweep.ts's `dispatchCompletionsForIds` (private to that
// file). The end-to-end contract under test is the lookup + dispatch.
async function dispatchTransitions(
  inDb: Database.Database,
  ids: string[],
  dispatch: (
    channelType: string,
    msg: {
      id: string;
      platformId: string;
      threadId: string | null;
      agentGroupId: string;
      sender: string | null;
    },
  ) => Promise<void>,
  agentGroupId = 'ag-test',
): Promise<void> {
  const stmt = inDb.prepare('SELECT channel_type, platform_id, thread_id, content FROM messages_in WHERE id = ?');
  for (const id of ids) {
    const row = stmt.get(id) as
      | { channel_type: string | null; platform_id: string | null; thread_id: string | null; content: string | null }
      | undefined;
    if (!row || row.channel_type == null) continue;
    let sender: string | null = null;
    if (row.content) {
      try {
        const parsed = JSON.parse(row.content) as { sender?: unknown };
        if (typeof parsed.sender === 'string' && parsed.sender.length > 0) sender = parsed.sender;
      } catch {
        /* leave sender null */
      }
    }
    await dispatch(row.channel_type, {
      id,
      platformId: row.platform_id ?? '',
      threadId: row.thread_id,
      agentGroupId,
      sender,
    });
  }
}

describe('sweep dispatch integration', () => {
  beforeEach(async () => {
    const { _resetCompletionHooksForTesting } = await import('./channels/channel-registry.js');
    _resetCompletionHooksForTesting();
  });

  it('fires the hook once per pickup transition with correct routing fields', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'sig-1', 'pending', 'signal', '+15551112222', null);
    insertAck(outDb, 'sig-1', 'processing');

    const seen: Array<{
      id: string;
      platformId: string;
      threadId: string | null;
      agentGroupId: string;
      sender: string | null;
    }> = [];
    registerCompletionHook('signal', (msg) => {
      seen.push(msg);
    });

    const { pickedUpIds } = syncProcessingAcks(inDb, outDb);
    await dispatchTransitions(inDb, pickedUpIds, dispatchCompletionHooks);

    expect(seen).toEqual([
      { id: 'sig-1', platformId: '+15551112222', threadId: null, agentGroupId: 'ag-test', sender: null },
    ]);
  });

  it('propagates sender from messages_in.content for group platformIds', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(
      inDb,
      'sig-grp',
      'pending',
      'signal',
      'group:abc123',
      null,
      JSON.stringify({ text: 'hi', sender: '+15555550999' }),
    );
    insertAck(outDb, 'sig-grp', 'processing');

    const seen: Array<{ platformId: string; sender: string | null }> = [];
    registerCompletionHook('signal', (msg) => {
      seen.push({ platformId: msg.platformId, sender: msg.sender });
    });

    const { pickedUpIds } = syncProcessingAcks(inDb, outDb);
    await dispatchTransitions(inDb, pickedUpIds, dispatchCompletionHooks);

    expect(seen).toEqual([{ platformId: 'group:abc123', sender: '+15555550999' }]);
  });

  it('fires the hook even when the container collapses pickup + failure in a single ack', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'sig-fail', 'pending', 'signal', '+1', null);
    insertAck(outDb, 'sig-fail', 'failed');

    const fired: string[] = [];
    registerCompletionHook('signal', (msg) => {
      fired.push(msg.id);
    });

    const { pickedUpIds, completedIds, failedIds } = syncProcessingAcks(inDb, outDb);
    expect({ pickedUpIds, completedIds, failedIds }).toEqual({
      pickedUpIds: ['sig-fail'],
      completedIds: [],
      failedIds: ['sig-fail'],
    });
    await dispatchTransitions(inDb, pickedUpIds, dispatchCompletionHooks);

    expect(fired).toEqual(['sig-fail']);
    const row = inDb.prepare('SELECT status FROM messages_in WHERE id = ?').get('sig-fail') as { status: string };
    expect(row.status).toBe('completed');
  });

  it('does not re-fire the hook on a second sweep (idempotency)', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'sig-once', 'pending', 'signal', '+1', null);
    insertAck(outDb, 'sig-once', 'processing');

    const fired: string[] = [];
    registerCompletionHook('signal', (msg) => {
      fired.push(msg.id);
    });

    for (let i = 0; i < 2; i++) {
      const { pickedUpIds } = syncProcessingAcks(inDb, outDb);
      await dispatchTransitions(inDb, pickedUpIds, dispatchCompletionHooks);
    }

    expect(fired).toEqual(['sig-once']);
  });

  it('skips rows with null channel_type (system / a2a-without-channel)', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'sys-1', 'pending', null, null, null);
    insertInbound(inDb, 'sig-1', 'pending', 'signal', '+1', null);
    insertAck(outDb, 'sys-1', 'processing');
    insertAck(outDb, 'sig-1', 'processing');

    const fired: string[] = [];
    registerCompletionHook('signal', (msg) => {
      fired.push(msg.id);
    });

    const { pickedUpIds } = syncProcessingAcks(inDb, outDb);
    expect(pickedUpIds.sort()).toEqual(['sig-1', 'sys-1']);
    await dispatchTransitions(inDb, pickedUpIds, dispatchCompletionHooks);

    expect(fired).toEqual(['sig-1']);
  });
});
