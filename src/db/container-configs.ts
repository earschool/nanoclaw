import { log } from '../log.js';
import type { ChannelSettings, ContainerConfigRow } from '../types.js';
import { getDb } from './connection.js';

const SCALAR_COLUMNS = new Set([
  'provider',
  'model',
  'effort',
  'image_tag',
  'assistant_name',
  'max_messages_per_prompt',
  'cli_scope',
]);
const JSON_COLUMNS = new Set([
  'skills',
  'mcp_servers',
  'packages_apt',
  'packages_npm',
  'additional_mounts',
  'channel_settings',
]);

const CHANNEL_SETTINGS_DEFAULTS: ChannelSettings = { signal: { readReceipts: true } };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function mergeChannelSettings(stored: Record<string, unknown>): ChannelSettings {
  // Shallow-merge per-channel keys (signal, future: discord, telegram, …) so
  // a stored `{signal:{}}` still resolves readReceipts to the default `true`.
  const out: Record<string, unknown> = { ...stored };
  for (const [channel, defaults] of Object.entries(CHANNEL_SETTINGS_DEFAULTS)) {
    const storedChannel = stored[channel];
    if (isPlainObject(storedChannel) && isPlainObject(defaults)) {
      out[channel] = { ...defaults, ...storedChannel };
    } else {
      out[channel] = defaults;
    }
  }
  return out as ChannelSettings;
}

export function getContainerConfig(agentGroupId: string): ContainerConfigRow | undefined {
  return getDb().prepare('SELECT * FROM container_configs WHERE agent_group_id = ?').get(agentGroupId) as
    | ContainerConfigRow
    | undefined;
}

export function getAllContainerConfigs(): ContainerConfigRow[] {
  return getDb().prepare('SELECT * FROM container_configs').all() as ContainerConfigRow[];
}

/** Insert a new config row. Caller must supply all JSON fields (use defaults for empty). */
export function createContainerConfig(config: ContainerConfigRow): void {
  getDb()
    .prepare(
      `INSERT INTO container_configs (
        agent_group_id, provider, model, effort, image_tag, assistant_name,
        max_messages_per_prompt, skills, mcp_servers, packages_apt, packages_npm,
        additional_mounts, updated_at
      ) VALUES (
        @agent_group_id, @provider, @model, @effort, @image_tag, @assistant_name,
        @max_messages_per_prompt, @skills, @mcp_servers, @packages_apt, @packages_npm,
        @additional_mounts, @updated_at
      )`,
    )
    .run(config);
}

/** Create an empty config row with sensible defaults. Idempotent — no-ops if row exists. */
export function ensureContainerConfig(agentGroupId: string): void {
  getDb()
    .prepare(
      `INSERT OR IGNORE INTO container_configs (agent_group_id, updated_at)
       VALUES (?, ?)`,
    )
    .run(agentGroupId, new Date().toISOString());
}

/** Update scalar fields on a config row. Only touches fields present in `updates`. */
export function updateContainerConfigScalars(
  agentGroupId: string,
  updates: Partial<
    Pick<
      ContainerConfigRow,
      'provider' | 'model' | 'effort' | 'image_tag' | 'assistant_name' | 'max_messages_per_prompt' | 'cli_scope'
    >
  >,
): void {
  const fields: string[] = [];
  const values: Record<string, unknown> = { agent_group_id: agentGroupId };

  for (const [key, value] of Object.entries(updates)) {
    if (value !== undefined) {
      if (!SCALAR_COLUMNS.has(key)) throw new Error(`Invalid scalar column: ${key}`);
      fields.push(`${key} = @${key}`);
      values[key] = value;
    }
  }
  if (fields.length === 0) return;

  fields.push('updated_at = @updated_at');
  values.updated_at = new Date().toISOString();

  getDb()
    .prepare(`UPDATE container_configs SET ${fields.join(', ')} WHERE agent_group_id = @agent_group_id`)
    .run(values);
}

/** Overwrite a JSON column wholesale. Used for skills, mcp_servers, packages_*, additional_mounts, channel_settings. */
export function updateContainerConfigJson(
  agentGroupId: string,
  column: 'skills' | 'mcp_servers' | 'packages_apt' | 'packages_npm' | 'additional_mounts' | 'channel_settings',
  value: unknown,
): void {
  if (!JSON_COLUMNS.has(column)) throw new Error(`Invalid JSON column: ${column}`);
  const now = new Date().toISOString();
  getDb()
    .prepare(`UPDATE container_configs SET ${column} = ?, updated_at = ? WHERE agent_group_id = ?`)
    .run(JSON.stringify(value), now, agentGroupId);
}

export function deleteContainerConfig(agentGroupId: string): void {
  getDb().prepare('DELETE FROM container_configs WHERE agent_group_id = ?').run(agentGroupId);
}

/**
 * Resolved per-channel feature flags for an agent group. Missing rows,
 * missing keys, or malformed JSON all degrade to hardcoded defaults
 * (`{ signal: { readReceipts: true } }`).
 */
export function getChannelSettings(agentGroupId: string): ChannelSettings {
  const row = getDb()
    .prepare('SELECT channel_settings FROM container_configs WHERE agent_group_id = ?')
    .get(agentGroupId) as { channel_settings: string } | undefined;
  if (!row) return mergeChannelSettings({});

  let parsed: unknown;
  try {
    parsed = JSON.parse(row.channel_settings);
  } catch (err) {
    log.warn('container_configs.channel_settings is not valid JSON, using defaults', {
      agent_group_id: agentGroupId,
      err: String(err),
    });
    return mergeChannelSettings({});
  }

  if (!isPlainObject(parsed)) {
    log.warn('container_configs.channel_settings is not a JSON object, using defaults', {
      agent_group_id: agentGroupId,
    });
    return mergeChannelSettings({});
  }

  return mergeChannelSettings(parsed);
}

export function setChannelSettings(agentGroupId: string, settings: ChannelSettings): void {
  updateContainerConfigJson(agentGroupId, 'channel_settings', settings);
}
