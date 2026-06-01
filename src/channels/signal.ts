/**
 * Signal channel adapter for NanoClaw v2.
 *
 * Uses signal-cli's TCP JSON-RPC daemon for bidirectional messaging.
 * Requires signal-cli (https://github.com/AsamK/signal-cli) installed
 * and a linked account.
 *
 * Ported from v1 — see v1 source for commit history.
 */
import { execFileSync, execSync, spawn } from 'node:child_process';
import { existsSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs';
import { createConnection, type Socket } from 'node:net';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';

import type { ChannelAdapter, ChannelSetup, InboundMessage, OutboundMessage } from './adapter.js';
import { registerChannelAdapter, registerCompletionHook } from './channel-registry.js';
import { getChannelSettings } from '../db/container-configs.js';
import { readEnvFile } from '../env.js';
import { log } from '../log.js';

// ---------------------------------------------------------------------------
// Signal CLI daemon management
// ---------------------------------------------------------------------------

interface DaemonHandle {
  stop: () => void;
  exited: Promise<void>;
  isExited: () => boolean;
}

function spawnSignalDaemon(cliPath: string, account: string, host: string, port: number): DaemonHandle {
  const args: string[] = [];
  if (account) args.push('-a', account);
  args.push('daemon', '--tcp', `${host}:${port}`, '--no-receive-stdout');
  args.push('--receive-mode', 'on-start');

  const child = spawn(cliPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  let exited = false;

  const exitedPromise = new Promise<void>((resolve) => {
    child.once('exit', (code, signal) => {
      exited = true;
      if (code !== 0 && code !== null) {
        const reason = signal ? `signal ${signal}` : `code ${code}`;
        log.error('signal-cli daemon exited', { reason });
      }
      resolve();
    });
    child.on('error', (err) => {
      exited = true;
      log.error('signal-cli spawn error', { err });
      resolve();
    });
  });

  child.stdout?.on('data', (data: Buffer) => {
    for (const line of data.toString().split(/\r?\n/)) {
      if (line.trim()) log.debug('signal-cli stdout', { line: line.trim() });
    }
  });
  child.stderr?.on('data', (data: Buffer) => {
    for (const line of data.toString().split(/\r?\n/)) {
      if (!line.trim()) continue;
      if (/\b(ERROR|WARN|FAILED|SEVERE)\b/i.test(line)) {
        log.warn('signal-cli stderr', { line: line.trim() });
      } else {
        log.debug('signal-cli stderr', { line: line.trim() });
      }
    }
  });

  return {
    stop: () => {
      if (!child.killed && !exited) child.kill('SIGTERM');
    },
    exited: exitedPromise,
    isExited: () => exited,
  };
}

// ---------------------------------------------------------------------------
// TCP JSON-RPC client for signal-cli daemon (--tcp mode)
//
// signal-cli 0.14.x --tcp exposes a newline-delimited JSON-RPC socket.
// Requests are sent as JSON + newline; responses and push notifications
// (inbound messages) arrive the same way.
// ---------------------------------------------------------------------------

const RPC_TIMEOUT_MS = 15_000;

/** Cap on per-recipient failures rendered into the Error message — bounds
 *  log line length when a large group has many failed members. */
const MAX_FAILURES_IN_MESSAGE = 20;

/** Rich error built from a signal-cli JSON-RPC error response.
 *
 *  `allFailed`: true when every result in the response is non-SUCCESS (or
 *  the response has no results array — treated as opaque failure). When
 *  false, at least one recipient SUCCEEDED — used by group-send sites to
 *  decide whether to rethrow (full failure → retry) or warn-and-return
 *  (partial failure → don't re-deliver to SUCCESS recipients). */
type SignalRpcError = Error & {
  code?: number;
  data?: unknown;
  allFailed?: boolean;
};

/**
 * Build a rich Error from a signal-cli JSON-RPC error response.
 *
 * signal-cli reports the actual send failure (UNREGISTERED_FAILURE,
 * IDENTITY_FAILURE, NETWORK_FAILURE, …) inside `error.data.response.results`,
 * while `error.message` stays generic ("Failed to send message"). We lift
 * the per-recipient failure types into the Error's message so they survive
 * structured-log serialization, and stash the raw error.data + code on the
 * Error object for any callers that want to introspect.
 *
 * `err.data` is preserved on the object but is NOT serialized by the
 * default logger (src/log.ts only emits message + stack). Callers that
 * forward errors to external sinks (Sentry-style reporters, etc.) must
 * scrub `data.response.results[].recipientAddress` before egress — the
 * field carries phone numbers, UUIDs, and usernames together.
 */
function buildSignalRpcError(rpcError: { message?: string; code?: number; data?: unknown }): SignalRpcError {
  const baseMessage = rpcError.message ?? 'Signal RPC error';
  const summary = summarizeSendFailures(rpcError.data);
  const message = summary ? `${baseMessage}: ${summary}` : baseMessage;
  const err = new Error(message) as SignalRpcError;
  if (rpcError.code !== undefined) err.code = rpcError.code;
  if (rpcError.data !== undefined) err.data = rpcError.data;
  err.allFailed = computeAllFailed(rpcError.data);
  return err;
}

/** Returns true when the response has no SUCCESS result, false when at
 *  least one recipient succeeded. Defaults to true for non-send error
 *  shapes (no results array) so DM-style failures rethrow as before. */
function computeAllFailed(data: unknown): boolean {
  if (!data || typeof data !== 'object') return true;
  const response = (data as { response?: unknown }).response;
  if (!response || typeof response !== 'object') return true;
  const results = (response as { results?: unknown }).results;
  if (!Array.isArray(results) || results.length === 0) return true;
  return !results.some((r) => r && typeof r === 'object' && (r as { type?: unknown }).type === 'SUCCESS');
}

/**
 * Extract per-recipient failure types + addresses from a signal-cli send
 * error payload, formatted as a compact "TYPE for recipient" list.
 * Returns null for shapes that don't look like a send response — callers
 * fall back to the generic RPC message. Iteration is capped at
 * MAX_FAILURES_IN_MESSAGE entries with a trailing `… +N more` suffix so
 * the error message stays bounded for very large groups.
 */
function summarizeSendFailures(data: unknown): string | null {
  if (!data || typeof data !== 'object') return null;
  const response = (data as { response?: unknown }).response;
  if (!response || typeof response !== 'object') return null;
  const results = (response as { results?: unknown }).results;
  if (!Array.isArray(results) || results.length === 0) return null;

  const parts: string[] = [];
  let overflow = 0;
  for (const r of results) {
    if (!r || typeof r !== 'object') continue;
    const type = (r as { type?: unknown }).type;
    if (typeof type !== 'string' || type === 'SUCCESS') continue;
    if (parts.length >= MAX_FAILURES_IN_MESSAGE) {
      overflow++;
      continue;
    }
    const addr = (r as { recipientAddress?: unknown }).recipientAddress;
    let recipient = 'unknown';
    if (addr && typeof addr === 'object') {
      const a = addr as { number?: string | null; uuid?: string | null; username?: string | null };
      recipient = a.number ?? a.uuid ?? a.username ?? 'unknown';
    }
    parts.push(`${type} for ${recipient}`);
  }
  if (parts.length === 0) return null;
  const summary = parts.join('; ');
  return overflow > 0 ? `${summary}; … +${overflow} more` : summary;
}

class SignalTcpClient {
  private socket: Socket | null = null;
  private buffer = '';
  private pending = new Map<
    string,
    {
      resolve: (value: unknown) => void;
      reject: (err: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  private onNotification: ((method: string, params: unknown) => void) | null = null;
  private onClose: (() => void) | null = null;

  constructor(
    private host: string,
    private port: number,
  ) {}

  connect(handlers?: {
    onNotification?: (method: string, params: unknown) => void;
    onClose?: () => void;
  }): Promise<void> {
    this.onNotification = handlers?.onNotification ?? null;
    this.onClose = handlers?.onClose ?? null;
    return new Promise((resolve, reject) => {
      const sock = createConnection(this.port, this.host, () => {
        this.socket = sock;
        resolve();
      });
      sock.on('error', (err) => {
        if (!this.socket) {
          reject(err);
          return;
        }
        log.warn('Signal TCP socket error', { err });
      });
      sock.on('data', (chunk) => this.onData(chunk));
      sock.on('close', () => {
        const wasConnected = this.socket !== null;
        this.socket = null;
        for (const [, p] of this.pending) {
          clearTimeout(p.timer);
          p.reject(new Error('Signal TCP connection closed'));
        }
        this.pending.clear();
        if (wasConnected) this.onClose?.();
      });
    });
  }

  async rpc<T = unknown>(method: string, params?: Record<string, unknown>): Promise<T> {
    if (!this.socket) throw new Error('Signal TCP not connected');
    const id = Math.random().toString(36).slice(2);
    const msg = JSON.stringify({ jsonrpc: '2.0', method, params, id }) + '\n';

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Signal RPC timeout: ${method}`));
      }, RPC_TIMEOUT_MS);

      this.pending.set(id, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });
      this.socket!.write(msg);
    });
  }

  close() {
    this.socket?.destroy();
    this.socket = null;
  }

  isConnected(): boolean {
    return this.socket !== null && !this.socket.destroyed;
  }

  private onData(chunk: Buffer) {
    this.buffer += chunk.toString();
    let newlineIdx = this.buffer.indexOf('\n');
    while (newlineIdx !== -1) {
      const line = this.buffer.slice(0, newlineIdx).trim();
      this.buffer = this.buffer.slice(newlineIdx + 1);
      if (line) this.handleLine(line);
      newlineIdx = this.buffer.indexOf('\n');
    }
  }

  private handleLine(line: string) {
    let parsed: any;
    try {
      parsed = JSON.parse(line);
    } catch {
      log.debug('Signal TCP: unparseable line', { line: line.slice(0, 200) });
      return;
    }

    if (parsed.id && this.pending.has(parsed.id)) {
      const p = this.pending.get(parsed.id)!;
      this.pending.delete(parsed.id);
      clearTimeout(p.timer);
      if (parsed.error) {
        p.reject(buildSignalRpcError(parsed.error));
      } else {
        p.resolve(parsed.result);
      }
      return;
    }

    if (parsed.method && this.onNotification) {
      this.onNotification(parsed.method, parsed.params);
    }
  }
}

async function signalTcpCheck(host: string, port: number): Promise<boolean> {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (result: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      sock.destroy();
      resolve(result);
    };
    const sock = createConnection(port, host, () => finish(true));
    sock.on('error', () => finish(false));
    const timer = setTimeout(() => finish(false), 5000);
  });
}

// ---------------------------------------------------------------------------
// Echo cache
// ---------------------------------------------------------------------------

const ECHO_TTL_MS = 10_000;

/**
 * Per-recipient dedup for messages we sent ourselves.
 *
 * signal-cli echoes our own outbound back via syncMessage (and, for Note to
 * Self, via sentMessage-with-self-destination). Without dedup, the agent sees
 * its own replies as new inbound and loops. We remember `(platformId, text)`
 * briefly after every send, and drop the first match within TTL.
 *
 * Keying on text alone is not enough: if we send "hi" to Alice and Bob then
 * sends "hi" from a different chat, Bob's real message gets silently dropped.
 */
class EchoCache {
  private entries = new Map<string, number>();

  private keyFor(platformId: string, text: string): string {
    return `${platformId}\x00${text.trim()}`;
  }

  remember(platformId: string, text: string): void {
    const trimmed = text.trim();
    if (!trimmed) return;
    this.entries.set(this.keyFor(platformId, trimmed), Date.now());
    this.cleanup();
  }

  isEcho(platformId: string, text: string): boolean {
    const trimmed = text.trim();
    if (!trimmed) return false;
    const key = this.keyFor(platformId, trimmed);
    const ts = this.entries.get(key);
    if (!ts) return false;
    if (Date.now() - ts > ECHO_TTL_MS) {
      this.entries.delete(key);
      return false;
    }
    this.entries.delete(key);
    return true;
  }

  private cleanup(): void {
    const now = Date.now();
    for (const [key, ts] of this.entries) {
      if (now - ts > ECHO_TTL_MS) this.entries.delete(key);
    }
  }
}

// ---------------------------------------------------------------------------
// Approval card tracking
//
// Signal has no interactive card UI. Approval cards are delivered as a
// formatted text message; the user replies with a Signal quote-reply
// containing the option number ("1", "2") or a keyword ("approve",
// "reject"). The Signal protocol message timestamp of the card is the
// lookup key for matching the inbound `dataMessage.quote.id`.
//
// Entries auto-expire (24h) so a stale approval doesn't pin memory if the
// user never replies. Successful lookup deletes the entry, so duplicate
// replies are dropped silently instead of re-firing onAction.
// ---------------------------------------------------------------------------

const APPROVAL_TTL_MS = 24 * 60 * 60 * 1000;

interface ApprovalOption {
  value: string;
  label?: string;
}

interface PendingApproval {
  questionId: string;
  options: ApprovalOption[];
  createdAt: number;
}

const approvalMessages = new Map<number, PendingApproval>();

function cleanupApprovalMessages(): void {
  const now = Date.now();
  for (const [ts, entry] of approvalMessages) {
    if (now - entry.createdAt > APPROVAL_TTL_MS) approvalMessages.delete(ts);
  }
}

interface AskQuestionContent {
  type: 'ask_question';
  questionId: string;
  title?: string;
  question?: string;
  options?: ApprovalOption[];
}

function formatApprovalText(content: AskQuestionContent): { body: string; options: ApprovalOption[] } {
  const opts: ApprovalOption[] =
    Array.isArray(content.options) && content.options.length > 0
      ? content.options
      : [
          { value: 'approve', label: 'Approve' },
          { value: 'reject', label: 'Reject' },
        ];

  const lines: string[] = [];
  if (content.title) lines.push(`*${content.title}*`);
  if (content.question) {
    if (lines.length > 0) lines.push('');
    lines.push(content.question);
  }
  lines.push('');
  opts.forEach((o, i) => {
    lines.push(`${i + 1}. ${o.label ?? o.value}`);
  });
  lines.push('', `_Reply to this message with the option number or one of: ${opts.map((o) => o.value).join(', ')}._`);

  return { body: lines.join('\n'), options: opts };
}

/**
 * Map an inbound reply text to one of the offered option values.
 *
 * Match order: leading digit (1-indexed into options), exact value match,
 * then approve/reject keyword aliases. Returns null when nothing matches,
 * so the caller can fall through to treating the message as regular chat.
 */
function parseApprovalReply(rawText: string, options: ApprovalOption[]): string | null {
  const t = rawText.trim().toLowerCase();
  if (!t) return null;
  const numMatch = t.match(/^([0-9]+)/);
  if (numMatch) {
    const idx = parseInt(numMatch[1], 10) - 1;
    if (idx >= 0 && idx < options.length) return options[idx].value;
  }
  for (const o of options) {
    if (o.value.toLowerCase() === t) return o.value;
  }
  if (/^(approve|approved|yes|y|ok|okay|confirm|✅)\b/.test(t)) {
    return options.find((o) => o.value === 'approve')?.value ?? null;
  }
  if (/^(reject|rejected|deny|denied|no|n|cancel|❌)\b/.test(t)) {
    return options.find((o) => o.value === 'reject')?.value ?? null;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Signal envelope types
// ---------------------------------------------------------------------------

interface SignalQuote {
  id?: number;
  author?: string;
  authorNumber?: string;
  authorUuid?: string;
  authorName?: string;
  text?: string;
}

interface SignalMention {
  start?: number;
  length?: number;
  uuid?: string;
  number?: string;
  name?: string;
}

interface SignalDataMessage {
  timestamp?: number;
  message?: string;
  mentions?: SignalMention[];
  groupInfo?: { groupId?: string; groupName?: string; type?: string };
  groupV2?: { id?: string };
  quote?: SignalQuote;
  attachments?: Array<{
    id?: string;
    contentType?: string;
    filename?: string;
    size?: number;
  }>;
}

interface SignalReceiptMessage {
  type?: 'read' | 'viewed' | 'delivery' | string;
  timestamps?: number[];
}

interface SignalEnvelope {
  source?: string;
  sourceName?: string;
  sourceNumber?: string;
  sourceUuid?: string;
  dataMessage?: SignalDataMessage;
  receiptMessage?: SignalReceiptMessage;
  syncMessage?: {
    sentMessage?: SignalDataMessage & {
      destination?: string;
      destinationNumber?: string;
    };
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Replace inline `@<placeholder>` mention markers with display names so the
 * agent sees `@Alice` instead of a raw UUID. Signal's protocol uses a single
 * placeholder character (typically U+FFFC) at each mention's `start` offset.
 */
function resolveMentions(text: string, mentions?: SignalMention[]): string {
  if (!mentions || mentions.length === 0) return text;
  const sorted = [...mentions].sort((a, b) => (a.start ?? 0) - (b.start ?? 0));
  let result = '';
  let cursor = 0;
  for (const m of sorted) {
    const start = m.start ?? 0;
    const length = m.length ?? 1;
    const name = m.name || m.number || (m.uuid ? m.uuid.slice(0, 8) : 'someone');
    if (start < cursor) continue;
    result += text.slice(cursor, start) + `@${name}`;
    cursor = start + length;
  }
  result += text.slice(cursor);
  return result;
}

/**
 * Map a signal-cli `contentType` to a canonical semantic class. Drives both
 * the synthesized filename extension and the rendered `[type: name — saved
 * to …]` label in the agent's prompt (see `formatAttachments` in the
 * container formatter).
 *
 * Signal voice notes arrive as `audio/aac` with no explicit flag; any
 * `audio/*` is treated as voice because there is no meaningful "background
 * music attachment" use case on Signal — users send voice notes.
 */
function classifySignalAttachment(contentType: string | undefined): string {
  if (!contentType) return 'file';
  const c = contentType.split(';')[0].trim().toLowerCase();
  if (c.startsWith('image/')) return 'image';
  if (c.startsWith('video/')) return 'video';
  if (c.startsWith('audio/')) return 'voice';
  return 'file';
}

/**
 * Best-effort MIME → extension fallback for when signal-cli omits a
 * `filename`. Keeps the synthesized name informative so the agent's Read
 * tool and downstream tools (image previewers, ffmpeg, etc.) can branch on
 * extension. Aligned with `src/attachment-naming.ts` plus the AAC entry
 * that Signal voice notes need.
 */
function extForSignalContentType(contentType: string | undefined): string {
  if (!contentType) return '';
  const clean = contentType.split(';')[0].trim().toLowerCase();
  const map: Record<string, string> = {
    'image/jpeg': 'jpg',
    'image/png': 'png',
    'image/webp': 'webp',
    'image/gif': 'gif',
    'image/heic': 'heic',
    'audio/aac': 'aac',
    'audio/ogg': 'ogg',
    'audio/mpeg': 'mp3',
    'audio/mp4': 'm4a',
    'audio/wav': 'wav',
    'video/mp4': 'mp4',
    'video/webm': 'webm',
    'video/quicktime': 'mov',
    'application/pdf': 'pdf',
    'text/plain': 'txt',
    'application/zip': 'zip',
    'application/json': 'json',
  };
  return map[clean] || '';
}

/**
 * Optional voice-note transcription. Tries (in order):
 *   1. local whisper.cpp CLI when `WHISPER_BIN` is set
 *   2. OpenAI Whisper API when `OPENAI_API_KEY` is set
 * Returns null if neither path is configured or transcription fails — caller
 * falls back to a `[Voice Message]` placeholder.
 *
 * Signal voice notes are AAC/ADTS; whisper-cpp wants WAV. ffmpeg is invoked
 * if available to convert; if ffmpeg is missing the local path is skipped.
 */
async function transcribeAudioOptional(filePath: string): Promise<string | null> {
  const whisperBin = process.env.WHISPER_BIN;
  if (whisperBin) {
    try {
      const wavPath = `${filePath}.wav`;
      execSync(`ffmpeg -y -loglevel error -i "${filePath}" -ar 16000 -ac 1 "${wavPath}"`, { stdio: 'ignore' });
      const model = process.env.WHISPER_MODEL || `${homedir()}/.local/share/whisper/models/ggml-base.en.bin`;
      const out = execSync(`"${whisperBin}" -m "${model}" -f "${wavPath}" -nt -otxt -of "${wavPath}"`, {
        encoding: 'utf-8',
        stdio: ['ignore', 'pipe', 'ignore'],
      });
      try {
        unlinkSync(wavPath);
        unlinkSync(`${wavPath}.txt`);
      } catch {}
      const text = out.replace(/\[[^\]]*\]/g, '').trim();
      if (text) return text;
    } catch (err) {
      log.debug('Signal: local whisper transcription failed, trying OpenAI', { err });
    }
  }

  const apiKey = process.env.OPENAI_API_KEY;
  if (apiKey) {
    try {
      const buf = readFileSync(filePath);
      const boundary = `----nanoclaw-${Date.now()}`;
      const body = Buffer.concat([
        Buffer.from(
          `--${boundary}\r\nContent-Disposition: form-data; name="model"\r\n\r\nwhisper-1\r\n--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="audio.aac"\r\nContent-Type: audio/aac\r\n\r\n`,
        ),
        buf,
        Buffer.from(`\r\n--${boundary}--\r\n`),
      ]);
      const res = await fetch('https://api.openai.com/v1/audio/transcriptions', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
        },
        body,
      });
      if (res.ok) {
        const json = (await res.json()) as { text?: string };
        if (json.text) return json.text.trim();
      }
    } catch (err) {
      log.debug('Signal: OpenAI transcription failed', { err });
    }
  }

  return null;
}

function chunkText(text: string, limit: number): string[] {
  const chunks: string[] = [];
  let remaining = text;
  while (remaining.length > 0) {
    if (remaining.length <= limit) {
      chunks.push(remaining);
      break;
    }
    let splitAt = remaining.lastIndexOf('\n', limit);
    if (splitAt <= 0) splitAt = limit;
    chunks.push(remaining.slice(0, splitAt));
    remaining = remaining.slice(splitAt).replace(/^\n/, '');
  }
  return chunks;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// Signal text styles — convert Markdown to Signal's offset-based formatting
// ---------------------------------------------------------------------------

interface SignalTextStyle {
  style: 'BOLD' | 'ITALIC' | 'STRIKETHROUGH' | 'MONOSPACE' | 'SPOILER';
  start: number;
  length: number;
}

interface StyledText {
  text: string;
  textStyles: SignalTextStyle[];
}

/**
 * Convert Markdown-ish input to Signal's offset-based style ranges.
 *
 * Walks the input recursively: at each level we find the leftmost matching
 * pattern, descend into its captured inner text (so `**bold with \`code\`
 * inside**` stays bold-plus-monospace rather than leaking stripped markers),
 * then continue past the match. Style offsets are recorded against the
 * *output* text length as it's built, so nested styles always point at the
 * right span of the final plain text.
 */
function parseSignalStyles(input: string): StyledText {
  const styles: SignalTextStyle[] = [];

  // Ordering matters: longer/greedier delimiters first so `` ``` `` beats
  // `` ` ``, `**` beats `*`. The italic-`*` pattern refuses to start on
  // whitespace so `*` isn't mistakenly opened on " * " in list-like text.
  const patterns: Array<{ regex: RegExp; style: SignalTextStyle['style'] }> = [
    { regex: /```([\s\S]+?)```/, style: 'MONOSPACE' },
    { regex: /`([^`]+)`/, style: 'MONOSPACE' },
    { regex: /\*\*([^]+?)\*\*/, style: 'BOLD' },
    { regex: /~~([^]+?)~~/, style: 'STRIKETHROUGH' },
    { regex: /\|\|([^]+?)\|\|/, style: 'SPOILER' },
    { regex: /\*([^*\s][^*]*?)\*/, style: 'ITALIC' },
    { regex: /_([^_\s][^_]*?)_/, style: 'ITALIC' },
  ];

  function walk(segment: string, outputBase: number): string {
    let earliest: { start: number; match: RegExpExecArray; style: SignalTextStyle['style'] } | null = null;
    for (const { regex, style } of patterns) {
      const m = regex.exec(segment);
      if (!m) continue;
      if (earliest === null || m.index < earliest.start) {
        earliest = { start: m.index, match: m, style };
      }
    }
    if (!earliest) return segment;

    const before = segment.slice(0, earliest.start);
    const fullMatch = earliest.match[0];
    const inner = earliest.match[1];
    const afterStart = earliest.start + fullMatch.length;
    const after = segment.slice(afterStart);

    const innerOut = walk(inner, outputBase + before.length);
    styles.push({
      style: earliest.style,
      start: outputBase + before.length,
      length: innerOut.length,
    });
    const afterOut = walk(after, outputBase + before.length + innerOut.length);

    return before + innerOut + afterOut;
  }

  const text = walk(input, 0);
  return { text, textStyles: styles };
}

// ---------------------------------------------------------------------------
// SignalAdapter — v2 ChannelAdapter implementation
// ---------------------------------------------------------------------------

/**
 * Platform ID format:
 *   DM:    phone number or UUID (e.g. "+15555550123")
 *   Group: "group:<groupId>" (e.g. "group:abc123")
 *
 * channelType is always "signal". The router combines channelType + platformId
 * to look up or create the messaging_group.
 */
export function createSignalAdapter(config: {
  cliPath: string;
  account: string;
  tcpHost: string;
  tcpPort: number;
  manageDaemon: boolean;
  signalDataDir: string;
}): ChannelAdapter {
  let daemon: DaemonHandle | null = null;
  let tcp: SignalTcpClient | null = null;
  let connected = false;
  const echoCache = new EchoCache();
  let setup: ChannelSetup | null = null;

  // Cached at adapter start() via the `version` RPC. Gates outbound read
  // receipts: false → completion hook returns early. May flip from true to
  // false later if a `sendReceipt` RPC fails with method-not-found shape.
  // SIGNAL_FORCE_RECEIPTS_DISABLED=1 forces this to stay false regardless
  // of detected daemon version. See signal-read-receipts design.md Dec. 5.
  let receiptsSupported = false;
  let hookRegistered = false;

  /**
   * Compare two semver-shaped strings. Returns -1/0/1.
   * Permissive — extra labels (e.g. "0.13.4-snapshot") are ignored after the
   * 3-tuple. NaN segments are treated as 0. We only care about >= 0.13.0,
   * so a strict parser would be overkill.
   */
  function compareSemver(a: string, b: string): number {
    const pa = a
      .split(/[.+-]/)
      .slice(0, 3)
      .map((n) => parseInt(n, 10) || 0);
    const pb = b
      .split(/[.+-]/)
      .slice(0, 3)
      .map((n) => parseInt(n, 10) || 0);
    for (let i = 0; i < 3; i++) {
      const av = pa[i] ?? 0;
      const bv = pb[i] ?? 0;
      if (av !== bv) return av < bv ? -1 : 1;
    }
    return 0;
  }

  /** Extract first <digits>.<digits>.<digits> from any string field on the
   * RPC result, or null if nothing parses. signal-cli returns
   * `{ version: "0.13.4" }`, but we stay permissive in case future builds
   * add prefixes / suffixes. */
  function parseVersion(result: unknown): string | null {
    if (!result || typeof result !== 'object') return null;
    for (const v of Object.values(result as Record<string, unknown>)) {
      if (typeof v !== 'string') continue;
      const m = v.match(/(\d+)\.(\d+)\.(\d+)/);
      if (m) return `${m[1]}.${m[2]}.${m[3]}`;
    }
    return null;
  }

  /** Send a `read` receipt for the given Signal protocol timestamp. Swallows
   * RPC errors — a failed receipt MUST NOT propagate into the host sweep
   * loop. On a "method not found"-shaped error we flip the cached
   * `receiptsSupported` so subsequent attempts no-op (self-healing in case
   * version detection was wrong or the daemon was downgraded). */
  async function sendReadReceipt(recipient: string, targetTimestamp: number): Promise<void> {
    if (!connected || !tcp) return;
    try {
      // Shape pinned by signal-read-receipts spec: { recipient,
      // targetTimestamps:[ts], type:'read' }. No `account` field — single-
      // account daemon mode handles routing internally, and the test
      // contract pins this exact shape.
      await tcp.rpc('sendReceipt', {
        recipient,
        targetTimestamps: [targetTimestamp],
        type: 'read',
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (/method not found|unknown method|no such method/i.test(msg)) {
        receiptsSupported = false;
        log.warn('Signal: sendReceipt rejected by daemon, disabling receipts for this session', {
          recipient,
          targetTimestamp,
          err: msg,
        });
      } else {
        log.warn('Signal: sendReceipt RPC failed', { recipient, targetTimestamp, err: msg });
      }
    }
  }

  /** Completion-hook implementation registered for channel_type='signal'.
   * See signal-read-receipts design.md Decision 2. */
  async function onCompleted(msgIn: {
    id: string;
    platformId: string;
    threadId: string | null;
    agentGroupId: string;
  }): Promise<void> {
    if (!receiptsSupported) return;
    if (msgIn.platformId.startsWith('group:')) return;
    const ts = parseInt(msgIn.id, 10);
    if (!Number.isFinite(ts) || String(ts) !== msgIn.id) {
      log.debug('Signal: skipping receipt — synthetic-or-invalid-timestamp', {
        messageId: msgIn.id,
        platformId: msgIn.platformId,
      });
      return;
    }
    let settings;
    try {
      settings = getChannelSettings(msgIn.agentGroupId);
    } catch (err) {
      log.warn('Signal: getChannelSettings failed, skipping receipt', {
        agentGroupId: msgIn.agentGroupId,
        err,
      });
      return;
    }
    if (settings?.signal?.readReceipts === false) return;
    await sendReadReceipt(msgIn.platformId, ts);
  }

  // -- inbound handling --

  function handleNotification(method: string, params: unknown): void {
    if (method === 'receive') {
      const envelope = (params as any)?.envelope;
      if (envelope) {
        handleEnvelope(envelope).catch((err) => {
          log.error('Signal: error handling envelope', { err });
        });
      }
    }
  }

  /**
   * Read signal-cli's on-disk attachment store for each declared attachment,
   * base64-encode the bytes, classify type, synthesize a filename when
   * signal-cli omitted one, and best-effort transcribe `audio/*` payloads.
   *
   * Reused by both the dataMessage branch (messages received from others) and
   * the syncMessage.sentMessage branch (messages we sent from another device,
   * including Note-to-Self) — the on-disk layout is identical.
   */
  async function loadAttachments(
    attachments: NonNullable<SignalDataMessage['attachments']>,
    platformId: string,
    startingContent: string,
  ): Promise<{
    attachmentRefs: Array<{ name: string; type: string; mimeType?: string; data: string }>;
    content: string;
  }> {
    const attachmentRefs: Array<{ name: string; type: string; mimeType?: string; data: string }> = [];
    let content = startingContent;

    for (let i = 0; i < attachments.length; i++) {
      const att = attachments[i];
      const filePath = join(config.signalDataDir, 'attachments', att.id!);
      if (!existsSync(filePath)) {
        log.warn('Signal: attachment file missing on disk', { platformId, id: att.id, path: filePath });
        continue;
      }

      const type = classifySignalAttachment(att.contentType);

      let name = att.filename?.trim() || '';
      if (!name) {
        const ext = extForSignalContentType(att.contentType);
        name = ext ? `${type}-${i + 1}.${ext}` : `${type}-${i + 1}`;
      }

      let buf: Buffer;
      try {
        buf = readFileSync(filePath);
      } catch (err) {
        log.warn('Signal: failed to read attachment file', { id: att.id, path: filePath, err });
        continue;
      }

      attachmentRefs.push({
        name,
        type,
        ...(att.contentType ? { mimeType: att.contentType } : {}),
        data: buf.toString('base64'),
      });

      if (type === 'voice') {
        const transcript = await transcribeAudioOptional(filePath);
        if (transcript) {
          const line = `[Voice: ${transcript}]`;
          content = content ? `${content}\n${line}` : line;
          log.info('Signal: voice transcribed', { platformId, length: transcript.length });
        } else if (!content) {
          content = '[Voice Message]';
        }
      }
    }

    return { attachmentRefs, content };
  }

  async function handleEnvelope(envelope: SignalEnvelope): Promise<void> {
    if (!setup) return;

    // Peer receipt envelopes (read/viewed/delivery) — log only, no DB write,
    // no further dispatch. Persistent storage of peer receipts is deferred
    // (see signal-read-receipts proposal). We still need to recognize the
    // envelope so it does not fall through to the dataMessage branch as a
    // no-op and to give operators visibility via debug logs.
    if (envelope.receiptMessage) {
      const sender = (envelope.sourceNumber ?? envelope.sourceUuid ?? envelope.source ?? '').trim();
      log.debug('Signal: inbound receipt envelope', {
        direction: 'inbound-receipt',
        sender,
        type: envelope.receiptMessage.type,
        timestamps: envelope.receiptMessage.timestamps ?? [],
      });
      return;
    }

    // Sync messages (sent from another device)
    const syncSent = envelope.syncMessage?.sentMessage;
    if (syncSent) {
      const dest = (syncSent.destinationNumber ?? syncSent.destination ?? '').trim();
      // "Note to Self" — destination is our own account. Attachments-only
      // notes (voice memos with no text) are the common case here; do NOT
      // early-return on empty `syncSent.message`.
      if (dest === config.account) {
        const text = (syncSent.message ?? '').trim();
        const platformId = config.account;
        const validAttachments = (syncSent.attachments ?? []).filter((a) => a.id);
        if (!text && validAttachments.length === 0) return;
        if (text && echoCache.isEcho(platformId, text)) return;

        const timestamp = syncSent.timestamp ? new Date(syncSent.timestamp).toISOString() : new Date().toISOString();
        setup.onMetadata(platformId, 'Note to Self', false);

        const { attachmentRefs, content } = await loadAttachments(validAttachments, platformId, text);
        if (!content && attachmentRefs.length === 0) return;

        const msg: InboundMessage = {
          id: String(syncSent.timestamp ?? Date.now()),
          kind: 'chat',
          content: {
            text: content,
            sender: config.account,
            senderId: `signal:${config.account}`,
            senderName: 'Me',
            isFromMe: true,
            ...(attachmentRefs.length > 0 ? { attachments: attachmentRefs } : {}),
            ...(syncSent.quote ? quoteToContent(syncSent.quote) : {}),
          },
          timestamp,
        };
        await setup.onInbound(platformId, null, msg);

        log.info('Signal message received', {
          platformId,
          sender: 'Me',
          attachments: attachmentRefs.length,
        });
        return;
      }
      // Other sync messages are our outbound — skip
      return;
    }

    const dataMessage = envelope.dataMessage;
    if (!dataMessage) return;

    const rawText = (dataMessage.message ?? '').trim();
    const text = rawText ? resolveMentions(rawText, dataMessage.mentions) : '';

    const validAttachments = (dataMessage.attachments ?? []).filter((a) => a.id);

    if (!text && validAttachments.length === 0) return;

    const sender = (envelope.sourceNumber ?? envelope.sourceUuid ?? envelope.source ?? '').trim();
    if (!sender) return;

    const senderName = (envelope.sourceName?.trim() || sender).trim();

    // Modern Signal groups use groupV2; legacy groupInfo.groupId is the
    // pre-V2 fallback. Without the V2 read, V2-only groups appear as DMs
    // because `groupInfo` is undefined.
    const groupInfo = dataMessage.groupInfo;
    const groupId = dataMessage.groupV2?.id ?? groupInfo?.groupId;
    const isGroup = Boolean(groupId);

    const platformId = isGroup ? `group:${groupId}` : sender;

    if (text && echoCache.isEcho(platformId, text)) {
      log.debug('Signal: skipping echo', { platformId });
      return;
    }

    // Approval quote-reply correlation. If this inbound is a Signal
    // quote-reply to a card we sent earlier (matched by quote.id =
    // outbound card's protocol timestamp), and the reply text parses to
    // one of the card's offered options, route through setup.onAction
    // instead of waking the agent with a chat message. Unparseable
    // replies to a known card fall through as ordinary chat — the agent
    // can sort it out.
    if (dataMessage.quote?.id != null) {
      const quotedTs = typeof dataMessage.quote.id === 'number' ? dataMessage.quote.id : Number(dataMessage.quote.id);
      const entry = approvalMessages.get(quotedTs);
      if (entry) {
        const selected = parseApprovalReply(text, entry.options);
        if (selected) {
          approvalMessages.delete(quotedTs);
          log.info('Signal approval reply matched', {
            platformId,
            questionId: entry.questionId,
            selected,
          });
          setup.onAction(entry.questionId, selected, `signal:${sender}`);
          return;
        }
        log.info('Signal approval reply unparseable, falling through to chat', {
          platformId,
          questionId: entry.questionId,
          preview: text.slice(0, 64),
        });
      }
    }

    const timestamp = dataMessage.timestamp ? new Date(dataMessage.timestamp).toISOString() : new Date().toISOString();

    const chatName = groupInfo?.groupName ?? (isGroup ? `Group ${groupId?.slice(0, 8)}` : senderName);

    setup.onMetadata(platformId, chatName, isGroup);

    // Read every attachment off disk and ship it as base64 in the message
    // content. The host's `extractAttachmentFiles` (session-manager.ts)
    // decodes, writes to `<sessionDir>/inbox/<msgId>/<name>`, sets
    // `localPath`, and strips `data` before insert. The container formatter
    // then prepends `/workspace/` so the agent can Read the file.
    const { attachmentRefs, content } = await loadAttachments(validAttachments, platformId, text);

    // If every attachment failed to read AND there is no text, suppress so
    // we don't wake the agent for an empty payload.
    if (!content && attachmentRefs.length === 0) return;

    const msg: InboundMessage = {
      id: String(dataMessage.timestamp ?? Date.now()),
      kind: 'chat',
      content: {
        text: content,
        sender,
        senderId: `signal:${sender}`,
        senderName,
        ...(attachmentRefs.length > 0 ? { attachments: attachmentRefs } : {}),
        ...(dataMessage.quote ? quoteToContent(dataMessage.quote) : {}),
      },
      timestamp,
    };
    await setup.onInbound(platformId, null, msg);

    log.info('Signal message received', {
      platformId,
      sender: senderName,
      attachments: attachmentRefs.length,
    });
  }

  /**
   * Build the `replyTo` object the agent-runner formatter expects (see
   * `container/agent-runner/src/formatter.ts:formatReplyContext`). The
   * formatter requires both `sender` and `text` to render the
   * `<quoted_message>` block; absent either, it omits the block entirely.
   *
   * The previous shape (`replyToSenderName` / `replyToMessageContent` /
   * `replyToMessageId` flat keys) did not match the formatter contract, so
   * quote-reply context was silently dropped end-to-end.
   */
  function quoteToContent(quote: SignalQuote): Record<string, unknown> {
    const sender = quote.authorName || quote.authorNumber || quote.author || quote.authorUuid || 'someone';
    const text = quote.text || '';
    return {
      replyTo: {
        id: quote.id ? String(quote.id) : undefined,
        sender,
        text,
      },
    };
  }

  // -- send helpers --

  async function sendText(platformId: string, text: string): Promise<void> {
    if (!connected || !tcp) {
      throw new Error('Signal channel not connected');
    }

    const MAX_CHUNK = 4000;
    const chunks = text.length <= MAX_CHUNK ? [text] : chunkText(text, MAX_CHUNK);

    // Multi-chunk failure tradeoff: chunks send sequentially; a rethrow from
    // chunk N+1 leaves chunks 1..N on the recipient's screen, and the
    // delivery layer's retry (MAX_DELIVERY_ATTEMPTS=3) re-runs the whole
    // sendText, so earlier chunks may be delivered up to 3 times. Accepted
    // here because the alternative (swallow and mark delivered) silently
    // truncates the user's message — a worse failure mode. Per-chunk
    // idempotency would need msg_id-keyed state across deliver() calls,
    // out of scope for the adapter.
    for (const chunk of chunks) {
      try {
        const { text: plainText, textStyles } = parseSignalStyles(chunk);
        const params: Record<string, unknown> = { message: plainText };
        if (config.account) params.account = config.account;
        if (textStyles.length > 0) {
          params.textStyle = textStyles.map((s) => `${s.start}:${s.length}:${s.style}`);
        }

        if (platformId.startsWith('group:')) {
          params.groupId = platformId.slice('group:'.length);
        } else {
          params.recipient = [platformId];
        }

        try {
          await tcp.rpc('send', params);
        } catch (styledErr) {
          // Only retry without textStyle for plausibly-style-related
          // failures. Per-recipient terminal failures (UNREGISTERED,
          // IDENTITY, NETWORK) have nothing to do with markup syntax —
          // retrying doubles signal-cli's load with no chance of success.
          if (textStyles.length > 0 && !(styledErr as SignalRpcError)?.data) {
            log.debug('Signal: textStyle rejected, retrying with markup');
            delete params.textStyle;
            params.message = chunk;
            await tcp.rpc('send', params);
          } else {
            throw styledErr;
          }
        }
        // Remember per-chunk, on success only. signal-cli echoes each
        // chunk back as a separate syncMessage, so the cache must key by
        // the on-wire chunk text (plainText) for handleEnvelope.isEcho to
        // suppress them. A throw before this point leaves no stale entry
        // priming the cache against a future legitimate inbound.
        echoCache.remember(platformId, plainText);
      } catch (err) {
        if (shouldAbsorbPartialFailure(err, platformId)) {
          log.warn('Signal: partial group send — some recipients failed', {
            platformId,
            chunkIndex: chunks.indexOf(chunk),
            err,
          });
          continue;
        }
        // Surface the failure so the delivery layer can retry / markFailed.
        // Swallowing here would let drainSession mark the message delivered
        // when nothing reached the recipient.
        log.error('Signal: send failed', { platformId, err });
        throw err;
      }
    }

    log.info('Signal message sent', { platformId, length: text.length });
  }

  /** A group send where signal-cli reports at least one SUCCESS recipient
   *  is treated as "delivered enough" — rethrowing would trigger a full
   *  retry that re-delivers to every SUCCESS member of the group. DM
   *  failures (no platformId 'group:' prefix) always propagate. */
  function shouldAbsorbPartialFailure(err: unknown, platformId: string): boolean {
    if (!platformId.startsWith('group:')) return false;
    const e = err as SignalRpcError;
    return e?.allFailed === false;
  }

  /**
   * Send one or more file attachments via signal-cli's `send` JSON-RPC, which
   * accepts an `attachments` array of host filesystem paths. The OutboundFile
   * Buffer is materialized to an OS temp file so signal-cli can read it, then
   * removed in the finally block.
   *
   * Caption text, if any, is sent first via `sendText` (which handles chunking
   * + textStyles) — keeps this function single-purpose and avoids a long
   * caption colliding with signal-cli's per-message size limits.
   */
  async function sendAttachments(platformId: string, files: { filename: string; data: Buffer }[]): Promise<void> {
    if (!connected || !tcp) {
      throw new Error('Signal channel not connected');
    }
    if (files.length === 0) return;

    const tempPaths: string[] = [];
    try {
      // Stage files inside the try so a writeFileSync mid-loop still hits
      // the finally for cleanup of partially-written tempPaths.
      for (const file of files) {
        const safeName = file.filename.replace(/[/\\\0]/g, '_');
        const tempPath = join(
          tmpdir(),
          `signal-out-${Date.now()}-${Math.random().toString(36).slice(2, 8)}-${safeName}`,
        );
        writeFileSync(tempPath, file.data);
        tempPaths.push(tempPath);
      }

      const params: Record<string, unknown> = { attachments: tempPaths };
      if (config.account) params.account = config.account;
      if (platformId.startsWith('group:')) {
        params.groupId = platformId.slice('group:'.length);
      } else {
        params.recipient = [platformId];
      }
      try {
        await tcp.rpc('send', params);
      } catch (err) {
        if (shouldAbsorbPartialFailure(err, platformId)) {
          log.warn('Signal: partial group attachment send — some recipients failed', {
            platformId,
            count: files.length,
            err,
          });
        } else {
          throw err;
        }
      }
      log.info('Signal attachments sent', { platformId, count: files.length, filenames: files.map((f) => f.filename) });
    } catch (err) {
      log.error('Signal: attachment send failed', { platformId, count: files.length, err });
      throw err;
    } finally {
      for (const p of tempPaths) {
        try {
          unlinkSync(p);
        } catch {
          /* best-effort cleanup */
        }
      }
    }
  }

  /**
   * Deliver an approval card as a numbered-options text message. Captures
   * the Signal protocol timestamp returned by signal-cli's `send` RPC and
   * stashes (timestamp → questionId, options) so an inbound quote-reply
   * can be correlated back to this approval via dataMessage.quote.id.
   */
  async function sendApprovalCard(platformId: string, content: AskQuestionContent): Promise<void> {
    if (!connected || !tcp) {
      throw new Error('Signal channel not connected');
    }
    const { body, options } = formatApprovalText(content);
    const { text: plainText, textStyles } = parseSignalStyles(body);

    const params: Record<string, unknown> = { message: plainText };
    if (config.account) params.account = config.account;
    if (textStyles.length > 0) {
      params.textStyle = textStyles.map((s) => `${s.start}:${s.length}:${s.style}`);
    }
    if (platformId.startsWith('group:')) {
      params.groupId = platformId.slice('group:'.length);
    } else {
      params.recipient = [platformId];
    }

    try {
      let result: { timestamp?: number } | undefined;
      try {
        result = await tcp.rpc<{ timestamp?: number }>('send', params);
      } catch (styledErr) {
        // Gate retry on textStyle-shaped failures only — see sendText for
        // the same rationale.
        if (textStyles.length > 0 && !(styledErr as SignalRpcError)?.data) {
          log.debug('Signal: textStyle rejected on approval, retrying with raw markup');
          delete params.textStyle;
          params.message = body;
          result = await tcp.rpc<{ timestamp?: number }>('send', params);
        } else {
          throw styledErr;
        }
      }

      const ts = result?.timestamp;
      if (typeof ts === 'number') {
        approvalMessages.set(ts, { questionId: content.questionId, options, createdAt: Date.now() });
        cleanupApprovalMessages();
        echoCache.remember(platformId, plainText);
        log.info('Signal approval card sent', { platformId, questionId: content.questionId, timestamp: ts });
      } else {
        log.warn('Signal: approval send returned no timestamp — replies will not correlate', {
          platformId,
          questionId: content.questionId,
        });
      }
    } catch (err) {
      if (shouldAbsorbPartialFailure(err, platformId)) {
        log.warn('Signal: partial group approval card send — some recipients failed', {
          platformId,
          questionId: content.questionId,
          err,
        });
        return;
      }
      log.error('Signal: approval card send failed', { platformId, questionId: content.questionId, err });
      throw err;
    }
  }

  async function waitForDaemon(): Promise<boolean> {
    const maxWait = 30_000;
    const pollInterval = 1000;
    const start = Date.now();

    while (Date.now() - start < maxWait) {
      if (daemon?.isExited()) return false;
      const ok = await signalTcpCheck(config.tcpHost, config.tcpPort);
      if (ok) return true;
      await sleep(pollInterval);
    }
    return false;
  }

  // -- adapter --

  const adapter: ChannelAdapter = {
    name: 'signal',
    channelType: 'signal',
    supportsThreads: false,

    async setup(cfg: ChannelSetup): Promise<void> {
      setup = cfg;

      if (config.manageDaemon) {
        daemon = spawnSignalDaemon(config.cliPath, config.account, config.tcpHost, config.tcpPort);
        const ready = await waitForDaemon();
        if (!ready) {
          daemon.stop();
          throw new Error('Signal daemon failed to start. Is signal-cli installed and your account linked?');
        }
      } else {
        const ok = await signalTcpCheck(config.tcpHost, config.tcpPort);
        if (!ok) {
          const err = new Error(
            `Signal daemon not reachable at ${config.tcpHost}:${config.tcpPort}. Start it manually or set SIGNAL_MANAGE_DAEMON=true`,
          );
          (err as any).name = 'NetworkError';
          throw err;
        }
      }

      tcp = new SignalTcpClient(config.tcpHost, config.tcpPort);
      await tcp.connect({
        onNotification: handleNotification,
        // Signal the adapter that the daemon dropped us. No auto-reconnect
        // yet — subsequent deliver() calls throw "Signal channel not
        // connected" so the delivery layer retries (giving a manual
        // signal-cli restart a chance) and eventually markFailed. setTyping
        // still short-circuits silently because the typing module re-fires
        // on a heartbeat — a missed tick is not user-visible data loss.
        onClose: () => {
          if (!connected) return;
          connected = false;
          log.warn('Signal channel lost TCP connection to signal-cli daemon', {
            account: config.account,
            host: config.tcpHost,
            port: config.tcpPort,
          });
        },
      });

      try {
        await tcp.rpc('updateProfile', {
          name: 'NanoClaw',
          account: config.account,
        });
      } catch {
        log.debug('Signal: could not set profile name');
      }

      try {
        await tcp.rpc('updateConfiguration', {
          typingIndicators: true,
          account: config.account,
        });
      } catch {
        log.debug('Signal: could not enable typing indicators');
      }

      // Read-receipt capability detection. Honored knobs (in priority order):
      //   1. SIGNAL_FORCE_RECEIPTS_DISABLED=1 → off, ignore daemon version.
      //   2. `version` RPC throws or returns unparseable payload → off.
      //   3. detected version < 0.13.0 → off, warn naming the version.
      //   4. detected version >= 0.13.0 → on.
      // Logged at info regardless so operators can see the resolved value.
      if (process.env.SIGNAL_FORCE_RECEIPTS_DISABLED === '1') {
        receiptsSupported = false;
        log.info('Signal: read receipts forced off via SIGNAL_FORCE_RECEIPTS_DISABLED');
      } else {
        try {
          const versionResult = await tcp.rpc('version', {});
          const detected = parseVersion(versionResult);
          if (!detected) {
            receiptsSupported = false;
            log.warn('Signal: version RPC returned unparseable payload, disabling read receipts', {
              result: versionResult,
            });
          } else if (compareSemver(detected, '0.13.0') < 0) {
            receiptsSupported = false;
            log.warn('Signal: signal-cli version below 0.13.0, disabling read receipts', { version: detected });
          } else {
            receiptsSupported = true;
            log.info('Signal: read receipts enabled', { version: detected });
          }
        } catch (err) {
          receiptsSupported = false;
          log.warn('Signal: version RPC failed, disabling read receipts', { err });
        }
      }

      // Register the completion hook once. Multiple setup() calls (e.g.
      // reconnect path, tests) must not stack duplicate hooks on the
      // registry — register-once + closure over the current state.
      if (!hookRegistered) {
        registerCompletionHook('signal', onCompleted);
        hookRegistered = true;
      }

      connected = true;
      log.info('Signal channel connected', {
        account: config.account,
        host: config.tcpHost,
        port: config.tcpPort,
      });
    },

    async teardown(): Promise<void> {
      connected = false;
      tcp?.close();
      tcp = null;
      if (daemon && config.manageDaemon) {
        daemon.stop();
        await daemon.exited;
      }
      daemon = null;
      log.info('Signal channel disconnected');
    },

    isConnected(): boolean {
      return connected;
    },

    async deliver(platformId: string, _threadId: string | null, message: OutboundMessage): Promise<string | undefined> {
      const content = message.content as Record<string, unknown> | string | undefined;

      // Approval / ask_question card → render as numbered text. The user
      // quote-replies to choose; handleEnvelope correlates the reply back
      // to setup.onAction via the approvalMessages map.
      if (
        message.kind === 'chat-sdk' &&
        content &&
        typeof content === 'object' &&
        (content as Record<string, unknown>).type === 'ask_question' &&
        typeof (content as Record<string, unknown>).questionId === 'string'
      ) {
        await sendApprovalCard(platformId, content as unknown as AskQuestionContent);
        return undefined;
      }

      let text: string | null = null;
      if (typeof content === 'string') {
        text = content;
      } else if (content && typeof content === 'object' && typeof content.text === 'string') {
        text = content.text;
      }

      const files = message.files ?? [];

      // Send accompanying text first so it lands above the attachment(s) in
      // the recipient's chat. Both branches no-op cleanly if their input is
      // empty, so any combination of (text, files) works.
      if (text) await sendText(platformId, text);
      if (files.length > 0) await sendAttachments(platformId, files);
      return undefined;
    },

    async setTyping(platformId: string, _threadId: string | null): Promise<void> {
      if (!connected || !tcp) return;
      if (platformId.startsWith('group:')) return;

      try {
        const params: Record<string, unknown> = { recipient: [platformId] };
        if (config.account) params.account = config.account;
        await tcp.rpc('sendTyping', params);
      } catch (err) {
        log.debug('Signal: typing indicator failed', { platformId, err });
      }
    },
  };

  return adapter;
}

// ---------------------------------------------------------------------------
// Self-registration
// ---------------------------------------------------------------------------

const DEFAULT_TCP_HOST = '127.0.0.1';
const DEFAULT_TCP_PORT = 7583;

registerChannelAdapter('signal', {
  factory: () => {
    const envVars = readEnvFile([
      'SIGNAL_ACCOUNT',
      'SIGNAL_TCP_HOST',
      'SIGNAL_TCP_PORT',
      'SIGNAL_CLI_PATH',
      'SIGNAL_MANAGE_DAEMON',
      'SIGNAL_DATA_DIR',
    ]);

    const account = process.env.SIGNAL_ACCOUNT || envVars.SIGNAL_ACCOUNT || '';
    if (!account) {
      log.debug('Signal: SIGNAL_ACCOUNT not set, skipping channel');
      return null;
    }

    const cliPath = process.env.SIGNAL_CLI_PATH || envVars.SIGNAL_CLI_PATH || 'signal-cli';
    const tcpHost = process.env.SIGNAL_TCP_HOST || envVars.SIGNAL_TCP_HOST || DEFAULT_TCP_HOST;
    const tcpPort = parseInt(process.env.SIGNAL_TCP_PORT || envVars.SIGNAL_TCP_PORT || String(DEFAULT_TCP_PORT), 10);
    const manageDaemon = (process.env.SIGNAL_MANAGE_DAEMON || envVars.SIGNAL_MANAGE_DAEMON || 'true') === 'true';

    const signalDataDir =
      process.env.SIGNAL_DATA_DIR || envVars.SIGNAL_DATA_DIR || join(homedir(), '.local', 'share', 'signal-cli');

    // Only check for `signal-cli` on PATH when the operator left cliPath at
    // the default AND asked us to manage the daemon. A custom absolute path
    // is treated as an explicit promise and spawn will surface its own ENOENT.
    if (manageDaemon && cliPath === 'signal-cli') {
      try {
        execFileSync('which', ['signal-cli'], { stdio: 'ignore' });
      } catch {
        log.debug('Signal: signal-cli binary not found, skipping channel');
        return null;
      }
    }

    return createSignalAdapter({
      cliPath,
      account,
      tcpHost,
      tcpPort,
      manageDaemon,
      signalDataDir,
    });
  },
});
