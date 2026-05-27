/**
 * Tests for the `--channel-settings` flag on `ncl groups config update`.
 *
 * The flag is the user-facing path for flipping the per-group
 * `channel_settings.signal.readReceipts` flag (and any future per-channel
 * feature flag). It accepts a JSON string and writes it via
 * `setChannelSettings`, identical to the JSON-string pattern already used by
 * `config add-mcp-server --args` and `--env`.
 *
 * These tests exercise the registered command end-to-end via `dispatch()`
 * with `caller: 'host'`, against an in-memory DB so the SQL writes are real.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';

import {
  closeDb,
  ensureContainerConfig,
  getChannelSettings,
  getContainerConfig,
  getDb,
  initTestDb,
  runMigrations,
} from '../../db/index.js';
import { dispatch } from '../dispatch.js';

// Side-effect import: register the `groups` resource and its custom ops.
import '../resources/groups.js';

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
  ensureContainerConfig(id);
}

beforeEach(() => {
  const db = initTestDb();
  runMigrations(db);
  seedGroup('ag-1');
});

afterEach(() => {
  closeDb();
});

describe('groups config update --channel-settings', () => {
  it('persists parsed JSON via setChannelSettings (hyphenated flag form)', async () => {
    // Hyphenated CLI flag → underscore arg via normalizeArgs in crud.ts.
    const resp = await dispatch(
      {
        id: 't-1',
        command: 'groups-config-update',
        args: { id: 'ag-1', 'channel-settings': '{"signal":{"readReceipts":false}}' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(true);
    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: false } });
    // The stored column should be the wholesale JSON we wrote.
    const row = getContainerConfig('ag-1')!;
    expect(JSON.parse(row.channel_settings)).toEqual({ signal: { readReceipts: false } });
  });

  it('accepts the underscore arg form (channel_settings)', async () => {
    // Some callers (notably dispatch.test.ts-style direct dispatch) pass
    // already-normalized underscore keys. Make sure both forms work.
    const resp = await dispatch(
      {
        id: 't-2',
        command: 'groups-config-update',
        args: { id: 'ag-1', channel_settings: '{"signal":{"readReceipts":false}}' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(true);
    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: false } });
  });

  it('round-trips: write false then read back via getChannelSettings', async () => {
    await dispatch(
      {
        id: 't-3a',
        command: 'groups-config-update',
        args: { id: 'ag-1', 'channel-settings': '{"signal":{"readReceipts":false}}' },
      },
      { caller: 'host' },
    );
    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: false } });

    // Round-trip the other direction — explicit true overrides the false.
    await dispatch(
      {
        id: 't-3b',
        command: 'groups-config-update',
        args: { id: 'ag-1', 'channel-settings': '{"signal":{"readReceipts":true}}' },
      },
      { caller: 'host' },
    );
    expect(getChannelSettings('ag-1')).toEqual({ signal: { readReceipts: true } });
  });

  it('returns the updated config with channel_settings reflected', async () => {
    const resp = await dispatch(
      {
        id: 't-4',
        command: 'groups-config-update',
        args: { id: 'ag-1', 'channel-settings': '{"signal":{"readReceipts":false}}' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(true);
    if (resp.ok) {
      // presentConfig deserializes JSON columns for display — channel_settings
      // should appear as a parsed object, not a JSON string.
      const data = resp.data as { channel_settings?: unknown };
      expect(data.channel_settings).toEqual({ signal: { readReceipts: false } });
    }
  });

  it('rejects invalid JSON with a clear, typed error', async () => {
    const resp = await dispatch(
      {
        id: 't-5',
        command: 'groups-config-update',
        args: { id: 'ag-1', 'channel-settings': '{not-json' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(false);
    if (!resp.ok) {
      expect(resp.error.code).toBe('handler-error');
      expect(resp.error.message.toLowerCase()).toContain('channel-settings');
      expect(resp.error.message.toLowerCase()).toContain('json');
    }
    // Make sure the bad write did not partially apply.
    expect(getContainerConfig('ag-1')!.channel_settings).toBe('{}');
  });

  it('rejects non-object JSON (array) with a clear error', async () => {
    const resp = await dispatch(
      {
        id: 't-6',
        command: 'groups-config-update',
        args: { id: 'ag-1', 'channel-settings': '[1,2,3]' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(false);
    if (!resp.ok) {
      expect(resp.error.code).toBe('handler-error');
      expect(resp.error.message.toLowerCase()).toContain('object');
    }
    expect(getContainerConfig('ag-1')!.channel_settings).toBe('{}');
  });

  it('rejects non-object JSON (null) with a clear error', async () => {
    const resp = await dispatch(
      {
        id: 't-7',
        command: 'groups-config-update',
        args: { id: 'ag-1', 'channel-settings': 'null' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(false);
    if (!resp.ok) {
      expect(resp.error.code).toBe('handler-error');
    }
    expect(getContainerConfig('ag-1')!.channel_settings).toBe('{}');
  });

  it('coexists with other scalar flag updates in the same invocation', async () => {
    const resp = await dispatch(
      {
        id: 't-8',
        command: 'groups-config-update',
        args: {
          id: 'ag-1',
          model: 'claude-opus-4-5',
          'channel-settings': '{"signal":{"readReceipts":false}}',
        },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(true);
    const row = getContainerConfig('ag-1')!;
    expect(row.model).toBe('claude-opus-4-5');
    expect(JSON.parse(row.channel_settings)).toEqual({ signal: { readReceipts: false } });
  });

  it('preserves existing scalar updates when only --channel-settings is passed', async () => {
    // Pre-seed a non-default scalar so we can confirm we don't clobber it.
    getDb()
      .prepare(`UPDATE container_configs SET model = ?, updated_at = ? WHERE agent_group_id = ?`)
      .run('claude-sonnet-4-7', now(), 'ag-1');

    const resp = await dispatch(
      {
        id: 't-9',
        command: 'groups-config-update',
        args: { id: 'ag-1', 'channel-settings': '{"signal":{"readReceipts":false}}' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(true);
    const row = getContainerConfig('ag-1')!;
    expect(row.model).toBe('claude-sonnet-4-7'); // untouched
    expect(JSON.parse(row.channel_settings)).toEqual({ signal: { readReceipts: false } });
  });

  it('does not break the "nothing to update" error path for empty invocations', async () => {
    // Passing only --id with no real updates should still surface the
    // existing "nothing to update" error — `--channel-settings` must not
    // accidentally count itself as present-but-empty.
    const resp = await dispatch(
      { id: 't-10', command: 'groups-config-update', args: { id: 'ag-1' } },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(false);
    if (!resp.ok) {
      expect(resp.error.code).toBe('handler-error');
      expect(resp.error.message.toLowerCase()).toContain('nothing to update');
      // The new flag should be advertised in the error message.
      expect(resp.error.message).toContain('--channel-settings');
    }
  });

  it('still rejects when --id is missing', async () => {
    const resp = await dispatch(
      {
        id: 't-11',
        command: 'groups-config-update',
        args: { 'channel-settings': '{"signal":{"readReceipts":false}}' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(false);
    if (!resp.ok) {
      expect(resp.error.code).toBe('handler-error');
      expect(resp.error.message.toLowerCase()).toContain('--id');
    }
  });

  it('errors when the agent group has no container config row', async () => {
    const resp = await dispatch(
      {
        id: 't-12',
        command: 'groups-config-update',
        args: { id: 'ag-missing', 'channel-settings': '{"signal":{"readReceipts":false}}' },
      },
      { caller: 'host' },
    );

    expect(resp.ok).toBe(false);
    if (!resp.ok) {
      expect(resp.error.code).toBe('handler-error');
      expect(resp.error.message).toContain('ag-missing');
    }
  });
});
