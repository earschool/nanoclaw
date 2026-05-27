import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import { log } from '../log.js';
import {
  closeDb,
  ensureContainerConfig,
  getChannelSettings,
  getContainerConfig,
  getDb,
  initTestDb,
  runMigrations,
  setChannelSettings,
  updateContainerConfigJson,
} from './index.js';

function now() {
  return new Date().toISOString();
}

function seedGroup(id = 'ag-1'): void {
  getDb()
    .prepare(
      `INSERT INTO agent_groups (id, name, folder, agent_provider, created_at)
       VALUES (?, ?, ?, ?, ?)`,
    )
    .run(id, `Group ${id}`, `folder-${id}`, null, now());
}

beforeEach(() => {
  const db = initTestDb();
  runMigrations(db);
});

afterEach(() => {
  closeDb();
  vi.restoreAllMocks();
});

describe('channel_settings column', () => {
  it('is created with NOT NULL default of empty JSON object', () => {
    const cols = getDb().prepare(`PRAGMA table_info(container_configs)`).all() as Array<{
      name: string;
      notnull: number;
      dflt_value: string | null;
    }>;
    const col = cols.find((c) => c.name === 'channel_settings');
    expect(col).toBeDefined();
    expect(col!.notnull).toBe(1);
    expect(col!.dflt_value).toBe(`'{}'`);
  });

  it('defaults to empty object when ensureContainerConfig is called', () => {
    seedGroup();
    ensureContainerConfig('ag-1');
    const row = getContainerConfig('ag-1');
    expect(row).toBeDefined();
    expect(row!.channel_settings).toBe('{}');
  });
});

describe('migration idempotency', () => {
  it('does not error when migrations are run twice', () => {
    const db = getDb();
    expect(() => runMigrations(db)).not.toThrow();
    const cols = db.prepare(`PRAGMA table_info(container_configs)`).all() as Array<{
      name: string;
    }>;
    expect(cols.some((c) => c.name === 'channel_settings')).toBe(true);
  });

  it('preserves channel_settings default on a fresh group after re-running migrations', () => {
    const db = getDb();
    runMigrations(db);
    seedGroup('ag-fresh');
    ensureContainerConfig('ag-fresh');
    const row = getContainerConfig('ag-fresh');
    expect(row!.channel_settings).toBe('{}');
  });
});

describe('getChannelSettings', () => {
  beforeEach(() => {
    seedGroup();
    ensureContainerConfig('ag-1');
  });

  it('returns the hardcoded defaults when column is the empty default {}', () => {
    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: true } });
  });

  it('returns the hardcoded defaults when no row exists for the agent group', () => {
    expect(getChannelSettings('ag-missing')).toEqual({ signal: { readReceipts: true } });
  });

  it('merges stored settings on top of defaults', () => {
    setChannelSettings('ag-1', { signal: { readReceipts: false } });
    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: false } });
  });

  it('fills missing fields with defaults when partial settings are stored (signal:{})', () => {
    setChannelSettings('ag-1', { signal: {} });
    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: true } });
  });

  it('preserves unknown channel keys while filling signal defaults', () => {
    // Forward-compat: future channel keys should round-trip even if the
    // resolver only knows about signal today.
    setChannelSettings('ag-1', { signal: { readReceipts: false }, discord: { foo: 'bar' } } as unknown as Parameters<
      typeof setChannelSettings
    >[1]);
    const settings = getChannelSettings('ag-1') as { signal?: unknown; discord?: unknown };
    expect(settings.signal).toEqual({ readReceipts: false });
    expect(settings.discord).toEqual({ foo: 'bar' });
  });

  it('falls back to defaults and logs warn when stored JSON is malformed', () => {
    const warnSpy = vi.spyOn(log, 'warn').mockImplementation(() => {});
    // Bypass the writer to inject invalid JSON
    getDb()
      .prepare(`UPDATE container_configs SET channel_settings = ?, updated_at = ? WHERE agent_group_id = ?`)
      .run('{not-json', now(), 'ag-1');

    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: true } });
    expect(warnSpy).toHaveBeenCalledTimes(1);
    const [msg, data] = warnSpy.mock.calls[0]!;
    expect(typeof msg).toBe('string');
    expect((data as Record<string, unknown>).agent_group_id).toBe('ag-1');
  });

  it('falls back to defaults and logs warn when stored JSON is not a plain object', () => {
    const warnSpy = vi.spyOn(log, 'warn').mockImplementation(() => {});
    getDb()
      .prepare(`UPDATE container_configs SET channel_settings = ?, updated_at = ? WHERE agent_group_id = ?`)
      .run('[1,2,3]', now(), 'ag-1');

    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: true } });
    expect(warnSpy).toHaveBeenCalledTimes(1);
  });
});

describe('setChannelSettings', () => {
  beforeEach(() => {
    seedGroup();
    ensureContainerConfig('ag-1');
  });

  it('round-trips a full signal override', () => {
    setChannelSettings('ag-1', { signal: { readReceipts: false } });
    const stored = getContainerConfig('ag-1')!.channel_settings;
    expect(JSON.parse(stored)).toEqual({ signal: { readReceipts: false } });
    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: false } });
  });

  it('updates the updated_at column', () => {
    // Force a known stale timestamp so we don't depend on ms-resolution clock deltas.
    const stale = '2000-01-01T00:00:00.000Z';
    getDb().prepare(`UPDATE container_configs SET updated_at = ? WHERE agent_group_id = ?`).run(stale, 'ag-1');
    setChannelSettings('ag-1', { signal: { readReceipts: false } });
    const after = getContainerConfig('ag-1')!.updated_at;
    expect(after).not.toBe(stale);
    expect(after > stale).toBe(true);
  });

  it('overwrites prior settings wholesale (replaces, does not merge)', () => {
    setChannelSettings('ag-1', { signal: { readReceipts: false } });
    setChannelSettings('ag-1', { signal: {} });
    expect(JSON.parse(getContainerConfig('ag-1')!.channel_settings)).toEqual({ signal: {} });
  });
});

describe('updateContainerConfigJson accepts channel_settings', () => {
  beforeEach(() => {
    seedGroup();
    ensureContainerConfig('ag-1');
  });

  it('writes via the generic JSON updater', () => {
    updateContainerConfigJson('ag-1', 'channel_settings', { signal: { readReceipts: false } });
    expect(JSON.parse(getContainerConfig('ag-1')!.channel_settings)).toEqual({
      signal: { readReceipts: false },
    });
  });

  it('rejects unknown JSON columns', () => {
    expect(() =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      updateContainerConfigJson('ag-1', 'not_a_real_column' as any, {}),
    ).toThrow();
  });
});
