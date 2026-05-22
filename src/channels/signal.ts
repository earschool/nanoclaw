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
import { registerChannelAdapter } from './channel-registry.js';
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
        p.reject(new Error(parsed.error.message ?? 'Signal RPC error'));
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

interface SignalEnvelope {
  source?: string;
  sourceName?: string;
  sourceNumber?: string;
  sourceUuid?: string;
  dataMessage?: SignalDataMessage;
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
    if (!connected || !tcp) return;

    echoCache.remember(platformId, text);

    const MAX_CHUNK = 4000;
    const chunks = text.length <= MAX_CHUNK ? [text] : chunkText(text, MAX_CHUNK);

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
          if (textStyles.length > 0) {
            log.debug('Signal: textStyle rejected, retrying with markup');
            delete params.textStyle;
            params.message = chunk;
            await tcp.rpc('send', params);
          } else {
            throw styledErr;
          }
        }
      } catch (err) {
        log.error('Signal: send failed', { platformId, err });
      }
    }

    log.info('Signal message sent', { platformId, length: text.length });
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
    if (!connected || !tcp) return;
    if (files.length === 0) return;

    const tempPaths: string[] = [];
    for (const file of files) {
      const safeName = file.filename.replace(/[/\\\0]/g, '_');
      const tempPath = join(tmpdir(), `signal-out-${Date.now()}-${Math.random().toString(36).slice(2, 8)}-${safeName}`);
      writeFileSync(tempPath, file.data);
      tempPaths.push(tempPath);
    }

    try {
      const params: Record<string, unknown> = { attachments: tempPaths };
      if (config.account) params.account = config.account;
      if (platformId.startsWith('group:')) {
        params.groupId = platformId.slice('group:'.length);
      } else {
        params.recipient = [platformId];
      }
      await tcp.rpc('send', params);
      log.info('Signal attachments sent', { platformId, count: files.length, filenames: files.map((f) => f.filename) });
    } catch (err) {
      log.error('Signal: attachment send failed', { platformId, count: files.length, err });
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
      log.warn('Signal: approval card skipped — not connected', { platformId });
      return;
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
        if (textStyles.length > 0) {
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
      log.error('Signal: approval card send failed', { platformId, questionId: content.questionId, err });
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
        // Signal the adapter that the daemon dropped us. No auto-reconnect yet
        // — subsequent deliver/setTyping calls short-circuit on `connected`
        // and log rather than throw into the retry loop. Operators see this in
        // logs/nanoclaw.log and can restart the service.
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
