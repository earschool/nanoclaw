/**
 * Host-side completion-hook dispatch (signal-read-receipts Stream S2):
 *
 *   1. `syncProcessingAcks` returns `{ completedIds, failedIds }` for rows
 *      that ACTUALLY transitioned (UPDATE row-changes > 0).
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
): void {
  const seq = ((db.prepare('SELECT COUNT(*) AS c FROM messages_in').get() as { c: number }).c + 1) * 2;
  db.prepare(
    `INSERT INTO messages_in (id, seq, kind, timestamp, status, content, channel_type, platform_id, thread_id)
     VALUES (?, ?, 'chat', datetime('now'), ?, '{}', ?, ?, ?)`,
  ).run(id, seq, status, channelType, platformId, threadId);
}

function insertAck(db: Database.Database, id: string, status: 'processing' | 'completed' | 'failed'): void {
  db.prepare("INSERT INTO processing_ack VALUES (?, ?, datetime('now'))").run(id, status);
}

describe('syncProcessingAcks return value', () => {
  it('buckets transitioned rows into completedIds / failedIds; still marks messages_in completed', () => {
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'm-ok', 'pending', 'signal', '+1');
    insertInbound(inDb, 'm-bad', 'pending', 'signal', '+2');
    insertAck(outDb, 'm-ok', 'completed');
    insertAck(outDb, 'm-bad', 'failed');

    const result = syncProcessingAcks(inDb, outDb);

    expect(result).toEqual({ completedIds: ['m-ok'], failedIds: ['m-bad'] });
    // Existing side-effect: both rows marked completed regardless of bucket.
    const rows = inDb.prepare('SELECT id, status FROM messages_in ORDER BY id').all();
    expect(rows).toEqual([
      { id: 'm-bad', status: 'completed' },
      { id: 'm-ok', status: 'completed' },
    ]);
  });

  it('returns empty on re-run; already-completed rows do not re-transition', () => {
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'm-1', 'pending', 'signal', '+1');
    insertAck(outDb, 'm-1', 'completed');

    expect(syncProcessingAcks(inDb, outDb).completedIds).toEqual(['m-1']);
    expect(syncProcessingAcks(inDb, outDb)).toEqual({ completedIds: [], failedIds: [] });
  });

  it('returns empty when nothing is acked, and when ack refers to missing messages_in row', () => {
    expect(syncProcessingAcks(makeInboundDb(), makeOutboundDb())).toEqual({ completedIds: [], failedIds: [] });

    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertAck(outDb, 'ghost', 'completed');
    expect(syncProcessingAcks(inDb, outDb)).toEqual({ completedIds: [], failedIds: [] });
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

  it('invokes the registered hook once with {id, platformId, threadId, agentGroupId}', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const calls: Array<{ id: string; platformId: string; threadId: string | null; agentGroupId: string }> = [];
    registerCompletionHook('signal', (msg) => {
      calls.push(msg);
    });

    await dispatchCompletionHooks('signal', { id: '123', platformId: '+1', threadId: 'thr-x', agentGroupId: 'ag-1' });

    expect(calls).toEqual([{ id: '123', platformId: '+1', threadId: 'thr-x', agentGroupId: 'ag-1' }]);
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

    await dispatchCompletionHooks('multi', { id: '1', platformId: 'p', threadId: null, agentGroupId: 'ag-1' });

    expect(order.sort()).toEqual(['a', 'b']);
  });

  it('skips channels with no hooks silently (no log, no throw)', async () => {
    const { dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const warnSpy = vi.spyOn(log, 'warn');

    await expect(
      dispatchCompletionHooks('unknown', { id: '1', platformId: 'p', threadId: null, agentGroupId: 'ag-1' }),
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
      dispatchCompletionHooks('iso', { id: '1', platformId: 'p', threadId: null, agentGroupId: 'ag-1' }),
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

    await dispatchCompletionHooks('iso2', { id: '1', platformId: 'p', threadId: null, agentGroupId: 'ag-1' });
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
    msg: { id: string; platformId: string; threadId: string | null; agentGroupId: string },
  ) => Promise<void>,
  agentGroupId = 'ag-test',
): Promise<void> {
  const stmt = inDb.prepare('SELECT channel_type, platform_id, thread_id FROM messages_in WHERE id = ?');
  for (const id of ids) {
    const row = stmt.get(id) as
      | { channel_type: string | null; platform_id: string | null; thread_id: string | null }
      | undefined;
    if (!row || row.channel_type == null) continue;
    await dispatch(row.channel_type, {
      id,
      platformId: row.platform_id ?? '',
      threadId: row.thread_id,
      agentGroupId,
    });
  }
}

describe('sweep dispatch integration', () => {
  beforeEach(async () => {
    const { _resetCompletionHooksForTesting } = await import('./channels/channel-registry.js');
    _resetCompletionHooksForTesting();
  });

  it('fires the hook once per completed transition with correct routing fields', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'sig-1', 'pending', 'signal', '+15551112222', null);
    insertAck(outDb, 'sig-1', 'completed');

    const seen: Array<{ id: string; platformId: string; threadId: string | null; agentGroupId: string }> = [];
    registerCompletionHook('signal', (msg) => {
      seen.push(msg);
    });

    const { completedIds } = syncProcessingAcks(inDb, outDb);
    await dispatchTransitions(inDb, completedIds, dispatchCompletionHooks);

    expect(seen).toEqual([{ id: 'sig-1', platformId: '+15551112222', threadId: null, agentGroupId: 'ag-test' }]);
  });

  it('does NOT fire the hook for failed transitions, but messages_in.status is still completed', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'sig-fail', 'pending', 'signal', '+1', null);
    insertAck(outDb, 'sig-fail', 'failed');

    const fired: string[] = [];
    registerCompletionHook('signal', (msg) => {
      fired.push(msg.id);
    });

    const { completedIds, failedIds } = syncProcessingAcks(inDb, outDb);
    expect({ completedIds, failedIds }).toEqual({ completedIds: [], failedIds: ['sig-fail'] });
    await dispatchTransitions(inDb, completedIds, dispatchCompletionHooks);

    expect(fired).toEqual([]);
    const row = inDb.prepare('SELECT status FROM messages_in WHERE id = ?').get('sig-fail') as { status: string };
    expect(row.status).toBe('completed');
  });

  it('does not re-fire the hook on a second sweep (idempotency)', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'sig-once', 'pending', 'signal', '+1', null);
    insertAck(outDb, 'sig-once', 'completed');

    const fired: string[] = [];
    registerCompletionHook('signal', (msg) => {
      fired.push(msg.id);
    });

    for (let i = 0; i < 2; i++) {
      const { completedIds } = syncProcessingAcks(inDb, outDb);
      await dispatchTransitions(inDb, completedIds, dispatchCompletionHooks);
    }

    expect(fired).toEqual(['sig-once']);
  });

  it('skips rows with null channel_type (system / a2a-without-channel)', async () => {
    const { registerCompletionHook, dispatchCompletionHooks } = await import('./channels/channel-registry.js');
    const inDb = makeInboundDb();
    const outDb = makeOutboundDb();
    insertInbound(inDb, 'sys-1', 'pending', null, null, null);
    insertInbound(inDb, 'sig-1', 'pending', 'signal', '+1', null);
    insertAck(outDb, 'sys-1', 'completed');
    insertAck(outDb, 'sig-1', 'completed');

    const fired: string[] = [];
    registerCompletionHook('signal', (msg) => {
      fired.push(msg.id);
    });

    const { completedIds } = syncProcessingAcks(inDb, outDb);
    expect(completedIds.sort()).toEqual(['sig-1', 'sys-1']);
    await dispatchTransitions(inDb, completedIds, dispatchCompletionHooks);

    expect(fired).toEqual(['sig-1']);
  });
});
