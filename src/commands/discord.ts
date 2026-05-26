import { ensureProjectClaudeMd, run, runUserMessage, compactCurrentSession, stopRunningSession } from "../runner";
import { getSettings, loadSettings, reloadSettings } from "../config";
import { resetSession, peekSession } from "../sessions";
import { listThreadSessions, removeThreadSession, peekThreadSession } from "../sessionManager";
import { readFile, unlink, writeFile } from "node:fs/promises";
import { mkdir as mkdirFs } from "node:fs/promises";
import { existsSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { transcribeAudioToText } from "../whisper";
import { resolveSkillPrompt } from "../skills";
import { mkdir } from "node:fs/promises";
import { extname, join } from "node:path";
import { loadJobs } from "../jobs";
import * as XLSX from "xlsx";

// --- Discord API constants ---

const DISCORD_API = "https://discord.com/api/v10";
const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";

const GatewayOp = {
  DISPATCH: 0,
  HEARTBEAT: 1,
  IDENTIFY: 2,
  RECONNECT: 7,
  INVALID_SESSION: 9,
  HELLO: 10,
  HEARTBEAT_ACK: 11,
} as const;

// Intents bitfield
const INTENTS =
  (1 << 0) |   // GUILDS
  (1 << 9) |   // GUILD_MESSAGES
  (1 << 10) |  // GUILD_MESSAGE_REACTIONS
  (1 << 12) |  // DIRECT_MESSAGES
  (1 << 15);   // MESSAGE_CONTENT (privileged)

// --- Type interfaces ---

interface DiscordUser {
  id: string;
  username: string;
  discriminator: string;
  bot?: boolean;
}

interface DiscordAttachment {
  id: string;
  filename: string;
  content_type?: string;
  url: string;
  proxy_url: string;
  size: number;
  flags?: number;
}

interface DiscordMessage {
  id: string;
  channel_id: string;
  guild_id?: string;
  author: DiscordUser;
  content: string;
  attachments: DiscordAttachment[];
  mentions: DiscordUser[];
  referenced_message?: DiscordMessage | null;
  flags?: number;
  type: number;
}

interface DiscordInteraction {
  id: string;
  type: number; // 2=APPLICATION_COMMAND, 3=MESSAGE_COMPONENT
  data?: {
    name?: string;
    custom_id?: string;
  };
  channel_id?: string;
  guild_id?: string;
  member?: { user: DiscordUser };
  user?: DiscordUser;
  token: string;
  message?: DiscordMessage;
}

interface DiscordGuild {
  id: string;
  name: string;
  system_channel_id?: string | null;
  joined_at?: string;
}

interface GatewayPayload {
  op: number;
  d: any;
  s: number | null;
  t: string | null;
}

// --- Gateway state ---

let ws: WebSocket | null = null;
let heartbeatIntervalMs = 0;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let heartbeatJitterTimer: ReturnType<typeof setTimeout> | null = null;
let threadSubscriptionTimer: ReturnType<typeof setInterval> | null = null;
let lastSequence: number | null = null;
let gatewaySessionId: string | null = null;
let heartbeatAcked = true;
let running = true;
let discordDebug = false;

// Bot identity (populated from READY)
let botUserId: string | null = null;
let botUsername: string | null = null;
let applicationId: string | null = null;

// Track guilds we were already in before this session to avoid duplicate welcome messages
let readyGuildIds: Set<string> | null = null;

// Ensure startup message is sent only once per fresh connect
let startupMessageSent = false;

// Track known thread channel IDs and their parent channel IDs for multi-session support
const knownThreads = new Map<string, { parentId: string }>();

// Dedup set to prevent double-processing gateway duplicates
const processedMessageIds = new Set<string>();

// Dedup map for task threads: `${parentChannelId}:${taskId}` → threadId
// Populated from active threads on GUILD_CREATE and on every successful postTaskAnnouncement
const taskIdToThread = new Map<string, string>();

// --- Debug ---

function debugLog(message: string): void {
  if (!discordDebug) return;
  console.log(`[Discord][debug] ${message}`);
}

// --- REST API helper ---

async function discordApi<T>(
  token: string,
  method: string,
  endpoint: string,
  body?: unknown,
): Promise<T> {
  let res = await fetch(`${DISCORD_API}${endpoint}`, {
    method,
    headers: {
      Authorization: `Bot ${token}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  // Rate limit handling — loop instead of recursion, cap retry at 30s
  while (res.status === 429) {
    const data = (await res.json()) as { retry_after: number };
    const retryMs = Math.min(Math.ceil(data.retry_after * 1000), 30_000);
    debugLog(`Rate limited on ${method} ${endpoint}, retrying in ${retryMs}ms`);
    await Bun.sleep(retryMs);
    res = await fetch(`${DISCORD_API}${endpoint}`, {
      method,
      headers: {
        Authorization: `Bot ${token}`,
        "Content-Type": "application/json",
      },
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Discord API ${method} ${endpoint}: ${res.status} ${res.statusText} ${text}`);
  }

  // 204 No Content (reactions, etc.)
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

// --- Message sending ---

function formatTablesForDiscord(text: string): string {
  const lines = text.split("\n");
  const result: string[] = [];
  let inTable = false;
  let tableLines: string[] = [];

  for (const line of lines) {
    if (line.trimStart().startsWith("|")) {
      if (!inTable) {
        inTable = true;
        tableLines = [];
      }
      tableLines.push(line);
    } else {
      if (inTable) {
        result.push("```");
        result.push(...tableLines);
        result.push("```");
        inTable = false;
        tableLines = [];
      }
      result.push(line);
    }
  }
  if (inTable) {
    result.push("```");
    result.push(...tableLines);
    result.push("```");
  }
  return result.join("\n");
}


async function sendMessage(
  token: string,
  channelId: string,
  text: string,
  components?: unknown[],
): Promise<string | null> {
  const normalized = text.replace(/\[react:[^\]\r\n]+\]/gi, "").trim();
  if (!normalized) return null;
  const formatted = formatTablesForDiscord(normalized);
  const MAX_LEN = 2000;
  let lastMessageId: string | null = null;
  for (let i = 0; i < formatted.length; i += MAX_LEN) {
    const chunk = formatted.slice(i, i + MAX_LEN);
    const body: Record<string, unknown> = { content: chunk };
    // Attach components only to the last chunk
    if (components && i + MAX_LEN >= formatted.length) {
      body.components = components;
    }
    const msg = await discordApi<{ id: string }>(token, "POST", `/channels/${channelId}/messages`, body);
    lastMessageId = msg.id;
  }
  return lastMessageId;
}

async function sendMessageToUser(
  token: string,
  userId: string,
  text: string,
): Promise<void> {
  // Discord requires creating a DM channel before sending
  const channel = await discordApi<{ id: string }>(
    token,
    "POST",
    "/users/@me/channels",
    { recipient_id: userId },
  );
  await sendMessage(token, channel.id, text);
}

async function sendTyping(token: string, channelId: string): Promise<void> {
  await discordApi(token, "POST", `/channels/${channelId}/typing`).catch(() => {});
}

async function createThreadFromMessage(
  token: string,
  channelId: string,
  messageId: string,
  name: string,
): Promise<string> {
  const thread = await discordApi<{ id: string }>(
    token,
    "POST",
    `/channels/${channelId}/messages/${messageId}/threads`,
    { name: name.slice(0, 100), auto_archive_duration: 1440 },
  );
  return thread.id;
}

export async function postTaskAnnouncement(
  token: string,
  channelId: string,
  taskId: string,
  taskTitle: string,
): Promise<{ announcementMessageId: string; threadId: string }> {
  const dedupKey = `${channelId}:${taskId}`;
  const existing = taskIdToThread.get(dedupKey);
  if (existing) {
    console.log(`[Discord] Task thread reused: ${existing} for ${taskId} in ${channelId} (dedup)`);
    return { announcementMessageId: "", threadId: existing };
  }
  const text = `📋 **${taskId}: ${taskTitle}**\nСтатус: розпочато`;
  const msgId = await sendMessage(token, channelId, text);
  if (!msgId) throw new Error(`postTaskAnnouncement: sendMessage returned null for ${taskId}`);
  const threadId = await createThreadFromMessage(token, channelId, msgId, `${taskId}: ${taskTitle}`);
  knownThreads.set(threadId, { parentId: channelId });
  taskIdToThread.set(dedupKey, threadId);
  console.log(`[Discord] Task thread created: ${threadId} for ${taskId} in ${channelId}`);
  return { announcementMessageId: msgId, threadId };
}
export async function sendFileToChannel(
  token: string,
  channelId: string,
  filePath: string,
): Promise<void> {
  const file = Bun.file(filePath);
  if (!(await file.exists())) {
    console.error(`[Discord] sendFile: file not found: ${filePath}`);
    return;
  }

  const fileName = filePath.split("/").pop() ?? "file";
  const formData = new FormData();
  formData.append("files[0]", file, fileName);

  const res = await fetch(`${DISCORD_API}/channels/${channelId}/messages`, {
    method: "POST",
    headers: { Authorization: `Bot ${token}` },
    body: formData,
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Discord sendFile failed: ${res.status} ${body}`);
  }
}

export function extractSendFileDirectives(text: string): {
  cleanedText: string;
  filePaths: string[];
} {
  const filePaths: string[] = [];
  const cleanedText = text
    .replace(/\[send-file:([^\]\r\n]+)\]/gi, (_match, raw) => {
      const candidate = String(raw).trim();
      if (candidate) filePaths.push(candidate);
      return "";
    })
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  return { cleanedText, filePaths };
}

async function sendReaction(
  token: string,
  channelId: string,
  messageId: string,
  emoji: string,
): Promise<void> {
  const encoded = encodeURIComponent(emoji);
  await fetch(
    `${DISCORD_API}/channels/${channelId}/messages/${messageId}/reactions/${encoded}/@me`,
    {
      method: "PUT",
      headers: { Authorization: `Bot ${token}` },
    },
  ).catch(() => {});
}

// --- Reaction directive extraction (same as telegram.ts) ---

function extractReactionDirective(text: string): { cleanedText: string; reactionEmoji: string | null } {
  let reactionEmoji: string | null = null;
  const cleanedText = text
    .replace(/\[react:([^\]\r\n]+)\]/gi, (_match, raw) => {
      const candidate = String(raw).trim();
      if (!reactionEmoji && candidate) reactionEmoji = candidate;
      return "";
    })
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  return { cleanedText, reactionEmoji };
}

interface JobSaveDirective {
  name: string;
  frontmatter: string;
  body: string;
}

function extractJobDirectives(text: string): {
  cleanedText: string;
  deletions: string[];
  saves: JobSaveDirective[];
} {
  const deletions: string[] = [];
  const saves: JobSaveDirective[] = [];

  let cleaned = text.replace(
    /\[savejob:([^\]\r\n]+)\]\s*\n---\s*\n([\s\S]*?)\n---\s*\n([\s\S]*?)\[\/savejob\]/gi,
    (_m, name, frontmatter, body) => {
      saves.push({ name: String(name).trim(), frontmatter: String(frontmatter).trim(), body: String(body).trim() });
      return "";
    }
  );

  cleaned = cleaned.replace(/\[deljob:([^\]\r\n]+)\]/gi, (_m, raw) => {
    const name = String(raw).trim();
    if (name) deletions.push(name);
    return "";
  });

  cleaned = cleaned.replace(/[ \t]+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
  return { cleanedText: cleaned, deletions, saves };
}

async function applyJobDirectives(
  deletions: string[],
  saves: JobSaveDirective[]
): Promise<string[]> {
  const feedback: string[] = [];
  const JOBS_DIR = join(process.cwd(), ".claude", "claudeclaw", "jobs");
  await mkdirFs(JOBS_DIR, { recursive: true });

  for (const name of deletions) {
    const filePath = join(JOBS_DIR, `${name}.md`);
    try {
      await unlink(filePath);
      feedback.push(`🗑 Deleted job: **${name}**`);
    } catch (err: any) {
      if (err.code === "ENOENT") feedback.push(`Job not found: \`${name}\``);
      else feedback.push(`Failed to delete ${name}: ${err.message}`);
    }
  }

  for (const job of saves) {
    const safeName = job.name.replace(/[^a-zA-Z0-9_-]/g, "-");
    const filePath = join(JOBS_DIR, `${safeName}.md`);
    const fileContent = `---\n${job.frontmatter}\n---\n${job.body}\n`;
    try {
      await writeFile(filePath, fileContent, "utf8");
      feedback.push(`✅ Saved job: **${safeName}**`);
    } catch (err: any) {
      feedback.push(`Failed to save ${safeName}: ${err.message}`);
    }
  }

  return feedback;
}

// --- Thread rejoin helper ---
async function rejoinThreads(token: string): Promise<void> {
  const threadSessions = await listThreadSessions();
  for (const ts of threadSessions) {
    try {
      await discordApi(token, "PUT", `/channels/${ts.threadId}/thread-members/@me`);
      if (!knownThreads.has(ts.threadId)) {
        const ch = await discordApi<{ parent_id?: string }>(token, "GET", `/channels/${ts.threadId}`);
        if (ch.parent_id) {
          knownThreads.set(ts.threadId, { parentId: ch.parent_id });
        }
      }
      console.log(`[Discord] Rejoined thread: ${ts.threadId}`);
    } catch (err) {
      console.error(`[Discord] Failed to rejoin thread ${ts.threadId}: ${err}`);
    }
  }
  if (threadSessions.length > 0) {
    console.log(`[Discord] Rejoined ${threadSessions.length} thread(s) from sessions.json`);
  }
}

// --- Guild trigger logic ---

function guildTriggerReason(message: DiscordMessage): string | null {
  // Reply to bot
  if (botUserId && message.referenced_message?.author?.id === botUserId) return "reply_to_bot";

  // Mention via mentions array
  if (botUserId && message.mentions.some((m) => m.id === botUserId)) return "mention";

  // Mention in content (fallback)
  if (botUserId && message.content.includes(`<@${botUserId}>`)) return "mention_in_content";

  // Listen channel (respond to all messages, no mention needed)
  const config = getSettings().discord;
  if (config.listenChannels.includes(message.channel_id)) return "listen_channel";

  // Thread whose parent channel is a listen channel
  const threadInfo = knownThreads.get(message.channel_id);
  if (threadInfo && config.listenChannels.includes(threadInfo.parentId)) return "listen_channel_thread";

  return null;
}

// --- Attachment handling ---

// --- AI-powered thread intent classifier (uses Sonnet via Claude OAuth) ---
interface ThreadIntent {
  action: "hire" | "fire";
  names: string[];
}

async function classifyThreadIntent(text: string): Promise<ThreadIntent | null> {
  const systemPrompt = `You classify user messages into thread management intents.

If the user wants to CREATE/SPAWN/DEPLOY threads (e.g. "hire X", "派出 X", "叫 X 出來", "派 X 去打", "開 X", "建立 X"):
Return: {"action":"hire","names":["name1","name2"]}

If the user wants to DELETE/REMOVE threads (e.g. "fire X", "撤回 X", "把 X 叫回來", "刪 X", "關 X"):
Return: {"action":"fire","names":["name1","name2"]}

If the message is NOT about thread management, return: null

Rules:
- Extract individual names. "桃園三結義" = ["劉備","關羽","張飛"]. "五虎將" = ["關羽","張飛","趙雲","馬超","黃忠"].
- Common patterns: 派/派出/出征/上陣/迎戰/出戰 = hire. 撤/撤回/收回/叫回來/滾 = fire.
- Return ONLY valid JSON or the word null. No explanation.`;

  try {
    const input = `${systemPrompt}\n\n---\nUser message: ${text}`;
    const proc = Bun.spawn(
      ["claude", "--model", "claude-sonnet-4-20250514", "--print", "--output-format", "text"],
      {
        stdin: new Response(input).body!,
        stdout: "pipe",
        stderr: "pipe",
        env: { ...process.env, HOME: homedir() },
      },
    );
    const result = (await new Response(proc.stdout).text()).trim();
    await proc.exited;

    if (!result || result === "null") return null;
    const jsonMatch = result.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;
    return JSON.parse(jsonMatch[0]) as ThreadIntent;
  } catch (err) {
    console.error(`[Discord] Intent classifier error: ${err instanceof Error ? err.message : String(err)}`);
    return null;
  }
}

// --- Attachment handling ---

const BINARY_EXTENSIONS = new Set([
  ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".ico", ".svg",
  ".mp3", ".mp4", ".ogg", ".wav", ".m4a", ".flac", ".webm", ".avi", ".mov",
  ".zip", ".tar", ".gz", ".bz2", ".7z", ".rar",
  ".exe", ".dll", ".so", ".dylib", ".bin",
  ".woff", ".woff2", ".ttf", ".eot",
  ".pptx", ".ppt", ".pptm",
  ".odt", ".odp",
  ".pdf",
  ".db", ".sqlite", ".sqlite3", ".pyc", ".class", ".o", ".a",
]);

function isImageAttachment(a: DiscordAttachment): boolean {
  return Boolean(a.content_type?.startsWith("image/"));
}

function isVoiceAttachment(a: DiscordAttachment): boolean {
  // IS_VOICE_MESSAGE flag
  if ((a.flags ?? 0) & (1 << 13)) return true;
  return Boolean(a.content_type?.startsWith("audio/"));
}

function isTextFileAttachment(a: DiscordAttachment): boolean {
  if (isImageAttachment(a) || isVoiceAttachment(a)) return false;
  const ct = a.content_type ?? "";
  if (ct.startsWith("image/") || ct.startsWith("audio/") || ct.startsWith("video/")) return false;
  const ext = extname(a.filename).toLowerCase();
  if (BINARY_EXTENSIONS.has(ext)) return false;
  return true;
}

function isPdfAttachment(a: DiscordAttachment): boolean {
  if (a.content_type === "application/pdf") return true;
  return extname(a.filename).toLowerCase() === ".pdf";
}

function isBinaryFileAttachment(a: DiscordAttachment): boolean {
  if (isImageAttachment(a) || isVoiceAttachment(a) || isTextFileAttachment(a) || isPdfAttachment(a)) return false;
  return true;
}

function getBinaryFileHint(filename: string): string {
  const ext = extname(filename).toLowerCase();
  if ([".docx", ".doc", ".docm"].includes(ext)) return "Copy-paste the text or export as plain text.";
  if ([".pptx", ".ppt", ".pptm"].includes(ext)) return "Export as PDF or describe the slide content.";
  if (ext === ".pdf") return "Copy-paste the relevant sections directly into chat.";
  return "Try exporting as plain text or CSV.";
}

async function downloadDiscordAttachment(
  attachment: DiscordAttachment,
  type: "image" | "voice" | "pdf",
): Promise<string | null> {
  const dir = join(process.cwd(), ".claude", "claudeclaw", "inbox", "discord");
  await mkdir(dir, { recursive: true });

  const response = await fetch(attachment.url);
  if (!response.ok) throw new Error(`Discord attachment download failed: ${response.status}`);

  const defaultExt = type === "voice" ? ".ogg" : type === "pdf" ? ".pdf" : ".jpg";
  const ext = extname(attachment.filename) || defaultExt;
  const filename = `${attachment.id}-${Date.now()}${ext}`;
  const localPath = join(dir, filename);

  const bytes = new Uint8Array(await response.arrayBuffer());
  await Bun.write(localPath, bytes);
  debugLog(`Attachment downloaded: ${localPath} (${bytes.length} bytes)`);
  return localPath;
}

// --- Slash command registration ---

async function registerSlashCommands(token: string): Promise<void> {
  if (!applicationId) return;

  const commands = [
    {
      name: "start",
      description: "Show welcome message and usage instructions",
      type: 1,
    },
    {
      name: "reset",
      description: "Reset the global session for a fresh start",
      type: 1,
    },
    {
      name: "compact",
      description: "Compact session to reduce context size",
      type: 1,
    },
    {
      name: "status",
      description: "Show current session status",
      type: 1,
    },
    {
      name: "context",
      description: "Show context window usage",
      type: 1,
    },
    {
      name: "stop",
      description: "Stop the currently running Claude session in this channel",
      type: 1,
    },
    {
      name: "jobs",
      description: "List scheduled cron jobs",
      type: 1,
    },
    {
      name: "deljob",
      description: "Delete a scheduled cron job by name",
      type: 1,
      options: [
        {
          name: "name",
          description: "Job name (filename without .md)",
          type: 3,
          required: true,
        },
      ],
    },
    {
      name: "monitor",
      description: "Show machine status: CPU, memory, disk, top processes",
      type: 1,
    },
  ];

  await discordApi(
    token,
    "PUT",
    `/applications/${applicationId}/commands`,
    commands,
  );
  debugLog("Slash commands registered");
}

// --- Interaction response helper ---

async function respondToInteraction(
  interaction: DiscordInteraction,
  data: { content: string; flags?: number; components?: unknown[] },
): Promise<void> {
  await fetch(
    `${DISCORD_API}/interactions/${interaction.id}/${interaction.token}/callback`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type: 4, // CHANNEL_MESSAGE_WITH_SOURCE
        data,
      }),
    },
  );
}

// --- Message handler ---

async function handleMessageCreate(token: string, message: DiscordMessage): Promise<void> {
  const config = getSettings().discord;

  // Ignore bot messages
  if (message.author.bot) return;

  // Dedup: skip if already processed (e.g. gateway + poll both fire)
  if (message.id) {
    if (processedMessageIds.has(message.id)) return;
    processedMessageIds.add(message.id);
    if (processedMessageIds.size > 2000) {
      for (const id of [...processedMessageIds].slice(0, 500)) {
        processedMessageIds.delete(id);
      }
    }
  }

  const userId = message.author.id;
  const channelId = message.channel_id;
  const isDM = !message.guild_id;
  const isGuild = !!message.guild_id;
  const content = message.content;

  // Recover unknown threads: if not in knownThreads and not a top-level channel,
  // fetch channel info from Discord API and register if parent is a managed channel.
  if (isGuild && !knownThreads.has(channelId) && !config.channelProjects?.[channelId]) {
    try {
      const ch = await discordApi<{ parent_id?: string }>(config.token, "GET", `/channels/${channelId}`);
      if (ch.parent_id && config.channelProjects?.[ch.parent_id]) {
        knownThreads.set(channelId, { parentId: ch.parent_id });
        // Also rejoin so future messages are delivered without another API call
        discordApi(config.token, "PUT", `/channels/${channelId}/thread-members/@me`).catch(() => {});
        console.log(`[Discord] Thread auto-registered: ${channelId} (parent: ${ch.parent_id})`);
      }
    } catch (err) {
      debugLog(`Thread discovery failed for ${channelId}: ${err}`);
    }
  }

  // Guild trigger check — auto-listen in channels with a project (all auto-setup channels)
  const parentId = knownThreads.get(channelId)?.parentId;
  const hasChannelProject = isGuild && (
    !!config.channelProjects?.[channelId] ||
    (!!parentId && !!config.channelProjects?.[parentId])
  );
  const triggerReason = isGuild ? (hasChannelProject ? "channel_project" : guildTriggerReason(message)) : "direct_message";
  if (isGuild && !triggerReason) {
    const threadInfo = knownThreads.get(channelId);
    console.log(`[Discord][DIAG] SKIP channel=${channelId} guild=${message.guild_id} inKnown=${knownThreads.has(channelId)} threadInfo=${JSON.stringify(threadInfo)} knownSize=${knownThreads.size} listenCh=${JSON.stringify(config.listenChannels)} text="${content.slice(0, 40)}"`);
    return;
  }
  debugLog(
    `Handle message channel=${channelId} from=${userId} reason=${triggerReason} text="${content.slice(0, 80)}"`,
  );

  // Authorization check
  if (config.allowedUserIds.length > 0 && !config.allowedUserIds.includes(userId)) {
    if (isDM) {
      await sendMessage(config.token, channelId, "Unauthorized.");
    } else {
      debugLog(`Skip guild message channel=${channelId} from=${userId} reason=unauthorized_user`);
    }
    return;
  }

  // Detect attachments
  const imageAttachments = message.attachments.filter(isImageAttachment);
  const voiceAttachments = message.attachments.filter(isVoiceAttachment);
  const pdfAttachments = message.attachments.filter(isPdfAttachment);
  const textFileAttachments = message.attachments.filter(isTextFileAttachment);
  const binaryFileAttachments = message.attachments.filter(isBinaryFileAttachment);
  const hasImage = imageAttachments.length > 0;
  const hasVoice = voiceAttachments.length > 0;
  const hasPdf = pdfAttachments.length > 0;
  const hasTextFile = textFileAttachments.length > 0;
  const hasBinaryFile = binaryFileAttachments.length > 0;

  if (!content.trim() && !hasImage && !hasVoice && !hasTextFile && !hasBinaryFile) return;

  // Strip bot mention from content for cleaner prompt
  let cleanContent = content;
  if (botUserId) {
    cleanContent = cleanContent.replace(new RegExp(`<@!?${botUserId}>`, "g"), "").trim();
  }

  const label = message.author.username;
  const mediaParts = [hasImage ? "image" : "", hasVoice ? "voice" : ""].filter(Boolean);
  const mediaSuffix = mediaParts.length > 0 ? ` [${mediaParts.join("+")}]` : "";
  console.log(
    `[${new Date().toLocaleTimeString()}] Discord ${label}${mediaSuffix}: "${cleanContent.slice(0, 60)}${cleanContent.length > 60 ? "..." : ""}"`,
  );

  // Typing indicator loop (Discord typing lasts 10s, fire every 8s)
  const typingInterval = setInterval(() => sendTyping(config.token, channelId), 8000);

  try {
    await sendTyping(config.token, channelId);

    const imagePaths: string[] = [];
    const pdfPaths: { path: string; filename: string }[] = [];
    let voicePath: string | null = null;
    let voiceTranscript: string | null = null;

    if (hasImage) {
      for (const att of imageAttachments) {
        try {
          const p = await downloadDiscordAttachment(att, "image");
          if (p) imagePaths.push(p);
        } catch (err) {
          console.error(`[Discord] Failed to download image for ${label}: ${err instanceof Error ? err.message : err}`);
        }
      }
    }

    if (hasPdf) {
      for (const att of pdfAttachments) {
        try {
          const p = await downloadDiscordAttachment(att, "pdf");
          if (p) pdfPaths.push({ path: p, filename: att.filename });
        } catch (err) {
          console.error(`[Discord] Failed to download PDF for ${label}: ${err instanceof Error ? err.message : err}`);
        }
      }
    }

    if (hasVoice) {
      try {
        voicePath = await downloadDiscordAttachment(voiceAttachments[0], "voice");
      } catch (err) {
        console.error(`[Discord] Failed to download voice for ${label}: ${err instanceof Error ? err.message : err}`);
      }

      if (voicePath) {
        try {
          debugLog(`Voice file saved: path=${voicePath}`);
          voiceTranscript = await transcribeAudioToText(voicePath, {
            debug: discordDebug,
            log: (msg) => debugLog(msg),
          });
        } catch (err) {
          console.error(`[Discord] Failed to transcribe voice for ${label}: ${err instanceof Error ? err.message : err}`);
        }
      }
    }

    // --- Process text file attachments ---
    const textFileContents: { filename: string; content: string }[] = [];
    for (const att of textFileAttachments) {
      const MAX_TEXT_FILE_SIZE = 200 * 1024;
      if (att.size > MAX_TEXT_FILE_SIZE) {
        textFileContents.push({ filename: att.filename, content: `(file too large to inline: ${(att.size / 1024).toFixed(0)} KB — ask user to paste the relevant section)` });
        continue;
      }
      try {
        const filePath = await downloadDiscordAttachment(att, "image");
        if (filePath) {
          const fileExt = extname(att.filename).toLowerCase();
          if ([".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"].includes(fileExt)) {
            const wb = XLSX.readFile(filePath);
            const csvParts: string[] = [];
            for (const sheetName of wb.SheetNames) {
              const csv = XLSX.utils.sheet_to_csv(wb.Sheets[sheetName]);
              if (csv.trim()) csvParts.push(`Sheet: ${sheetName}\n${csv}`);
            }
            textFileContents.push({ filename: att.filename, content: csvParts.join("\n\n") || "(empty spreadsheet)" });
          } else if ([".docx", ".doc", ".docm"].includes(fileExt)) {
            const unzipResult = Bun.spawnSync(["unzip", "-p", filePath, "word/document.xml"]);
            if (unzipResult.exitCode === 0) {
              const xml = unzipResult.stdout.toString();
              const text = xml
                .replace(/<w:br[^>]*\/>/g, "\n")
                .replace(/<\/w:p>/g, "\n")
                .replace(/<[^>]+>/g, "")
                .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, "\"").replace(/&apos;/g, "'")
                .replace(/[ \t]+/g, " ")
                .replace(/\n +/g, "\n")
                .trim();
              textFileContents.push({ filename: att.filename, content: text || "(empty document)" });
            } else {
              textFileContents.push({ filename: att.filename, content: "(failed to extract docx content)" });
            }
          } else {
            const text = (await Bun.file(filePath).text()).replace(/\0/g, "");
            const sample = text.slice(0, 1000);
            const nonPrintable = (sample.match(/[\x01-\x08\x0e-\x1f\x7f]/g) ?? []).length;
            if (nonPrintable / Math.max(sample.length, 1) > 0.3) {
              const hint = att.filename.match(/\.(docx?|docm)$/i) ? "Copy-paste the text or export as plain text." :
                           att.filename.match(/\.pdf$/i) ? "Copy-paste the relevant text directly into chat." :
                           "Try exporting as plain text or CSV.";
              textFileContents.push({ filename: att.filename, content: `(binary file — cannot read inline. ${hint})` });
            } else {
              textFileContents.push({ filename: att.filename, content: text });
            }
          }
        }
      } catch (err) {
        console.error(`[Discord] Failed to download file ${att.filename}: ${err instanceof Error ? err.message : err}`);
        textFileContents.push({ filename: att.filename, content: "(failed to download)" });
      }
    }

    // --- Thread management: AI-powered intent classification ---
    if (isGuild && cleanContent.length < 200) {
      const intent = await classifyThreadIntent(cleanContent);
      if (intent && intent.action === "hire" && intent.names.length > 0) {
        const results: string[] = [];
        for (const threadName of intent.names) {
          try {
            const thread = await discordApi<{ id: string; name: string }>(
              config.token,
              "POST",
              `/channels/${channelId}/threads`,
              {
                name: threadName,
                type: 11, // PUBLIC_THREAD
                auto_archive_duration: 4320, // 3 days
              },
            );
            knownThreads.set(thread.id, { parentId: channelId });
            // Don't pre-create session — let Claude CLI create it on first message
            // The real UUID will be captured and saved by runner.ts
            await sendMessage(config.token, thread.id, `🧵 Thread **${threadName}** created with independent session. Start chatting!`);
            results.push(`✅ **${threadName}** → <#${thread.id}>`);
            console.log(`[Discord] Thread created: ${thread.id} name="${threadName}" parent=${channelId} knownSize=${knownThreads.size}`);
          } catch (err) {
            results.push(`❌ **${threadName}** — ${err instanceof Error ? err.message : err}`);
          }
        }
        await sendMessage(config.token, channelId, results.join("\n"));
        return;
      }

      if (intent && intent.action === "fire" && intent.names.length > 0) {
        const results: string[] = [];
        for (const targetName of intent.names) {
          const targetLower = targetName.toLowerCase();
          let foundId: string | null = null;
          for (const [tid, info] of knownThreads.entries()) {
            if (info.parentId === channelId) {
              try {
                const ch = await discordApi<{ id: string; name: string }>(config.token, "GET", `/channels/${tid}`);
                if (ch.name.toLowerCase() === targetLower) {
                  foundId = tid;
                  break;
                }
              } catch { /* thread might be gone */ }
            }
          }
          if (foundId) {
            try {
              await removeThreadSession(foundId);
              await discordApi(config.token, "DELETE", `/channels/${foundId}`);
              knownThreads.delete(foundId);
              results.push(`🗑️ **${targetName}** — deleted`);
            } catch (err) {
              results.push(`❌ **${targetName}** — ${err instanceof Error ? err.message : err}`);
            }
          } else {
            results.push(`❌ **${targetName}** — not found`);
          }
        }
        await sendMessage(config.token, channelId, results.join("\n"));
        return;
      }
    }

    // Skill routing: detect slash commands and resolve to SKILL.md prompts
    const command = cleanContent.startsWith("/") ? cleanContent.trim().split(/\s+/, 1)[0].toLowerCase() : null;
    let skillContext: string | null = null;
    if (command) {
      try {
        skillContext = await resolveSkillPrompt(command);
        if (skillContext) {
          debugLog(`Skill resolved for ${command}: ${skillContext.length} chars`);
        }
      } catch (err) {
        debugLog(`Skill resolution failed for ${command}: ${err instanceof Error ? err.message : err}`);
      }
    }

    // Build prompt (same pattern as Telegram)
    const promptParts = [`[Discord from ${label}]`];
    if (skillContext) {
      const args = cleanContent.trim().slice(command!.length).trim();
      promptParts.push(`<command-name>${command}</command-name>`);
      promptParts.push(skillContext);
      if (args) promptParts.push(`User arguments: ${args}`);
    } else if (cleanContent.trim()) {
      promptParts.push(`Message: ${cleanContent}`);
    }
    if (imagePaths.length > 0) {
      for (const p of imagePaths) {
        promptParts.push(`Image path: ${p}`);
      }
      promptParts.push(
        imagePaths.length === 1
          ? "The user attached an image. Inspect this image file directly before answering."
          : `The user attached ${imagePaths.length} images. Inspect each image file directly before answering.`,
      );
    } else if (hasImage) {
      promptParts.push("The user attached an image, but downloading it failed. Respond and ask them to resend.");
    }
    if (pdfPaths.length > 0) {
      for (const { path, filename } of pdfPaths) {
        promptParts.push(`PDF path: ${path} (original filename: ${filename})`);
      }
      promptParts.push(
        pdfPaths.length === 1
          ? "The user attached a PDF. Use the Read tool on the PDF path to inspect its contents. For PDFs longer than 10 pages, pass a pages range (e.g., pages: \"1-5\")."
          : `The user attached ${pdfPaths.length} PDFs. Use the Read tool on each PDF path to inspect contents. For PDFs longer than 10 pages, pass a pages range (e.g., pages: \"1-5\").`,
      );
    } else if (hasPdf) {
      promptParts.push("The user attached a PDF, but downloading it failed. Respond and ask them to resend.");
    }
    if (voiceTranscript) {
      promptParts.push(`Voice transcript: ${voiceTranscript}`);
      promptParts.push("The user attached voice audio. Use the transcript as their spoken message.");
    } else if (hasVoice) {
      promptParts.push(
        "The user attached voice audio, but it could not be transcribed. Respond and ask them to resend a clearer clip.",
      );
    }
    for (const { filename, content: fileContent } of textFileContents) {
      const safeContent = fileContent.replace(/\0/g, "");
      promptParts.push(`The user attached a file: ${filename}\n\`\`\`\n${safeContent}\n\`\`\``);
    }
    for (const att of binaryFileAttachments) {
      const hint = getBinaryFileHint(att.filename);
      promptParts.push(`The user attached a binary file "${att.filename}" (${(att.size / 1024).toFixed(0)} KB) that cannot be read inline. Tell them: ${hint}`);
    }

    const prefixedPrompt = promptParts.join("\n");
    // Use channel-specific session for guild channels; DMs share the global session
    const threadId = isGuild ? channelId : undefined;
    // Use channel-specific project directory if configured
    const channelProjectDir = isGuild
      ? (getSettings().discord.channelProjects?.[channelId] ??
         (parentId ? getSettings().discord.channelProjects?.[parentId] : undefined))
      : undefined;
    const result = await runUserMessage("discord", prefixedPrompt, threadId, channelProjectDir);

    if (result.exitCode !== 0) {
      await sendMessage(config.token, channelId, `Error (exit ${result.exitCode}): ${result.stderr || result.stdout || "Unknown error"}`);
    } else {
      const { cleanedText: afterReact, reactionEmoji } = extractReactionDirective(result.stdout || "");
      const { cleanedText: afterFile, filePaths } = extractSendFileDirectives(afterReact);
      const { cleanedText, deletions, saves } = extractJobDirectives(afterFile);
      const jobFeedback = (deletions.length || saves.length) ? await applyJobDirectives(deletions, saves) : [];
      if (reactionEmoji) {
        await sendReaction(config.token, channelId, message.id, reactionEmoji).catch((err) => {
          console.error(`[Discord] Failed to send reaction for ${label}: ${err instanceof Error ? err.message : err}`);
        });
      }
      const finalText = [cleanedText, ...jobFeedback].filter(Boolean).join("\n\n");
      if (finalText) {
        await sendMessage(config.token, channelId, finalText);
      }
      for (const fp of filePaths) {
        try {
          await sendFileToChannel(config.token, channelId, fp);
        } catch (err) {
          console.error(`[Discord] Failed to send file for ${label}: ${err instanceof Error ? err.message : err}`);
          await sendMessage(config.token, channelId, `Failed to send file: ${fp.split("/").pop()}`);
        }
      }
      if (!cleanedText && filePaths.length === 0 && jobFeedback.length === 0) {
        await sendMessage(config.token, channelId, "(empty response)");
      }
    }
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    console.error(`[Discord] Error for ${label}: ${errMsg}`);
    await sendMessage(config.token, channelId, `Error: ${errMsg}`);
  } finally {
    clearInterval(typingInterval);
  }
}

// --- Interaction handler (slash commands + secretary buttons) ---

async function handleInteractionCreate(token: string, interaction: DiscordInteraction): Promise<void> {
  const config = getSettings().discord;
  const actorId = interaction.member?.user?.id ?? interaction.user?.id;

  if (config.allowedUserIds.length > 0 && (!actorId || !config.allowedUserIds.includes(actorId))) {
    await respondToInteraction(interaction, { content: "Unauthorized.", flags: 64 });
    return;
  }

  // Slash commands (type 2)
  if (interaction.type === 2 && interaction.data?.name) {
    if (interaction.data.name === "start") {
      await respondToInteraction(interaction, {
        content: "Hello! Send me a message and I'll respond using Claude.\nUse `/reset` to start a fresh session.",
      });
      return;
    }

    if (interaction.data.name === "reset") {
      const channelId = interaction.channel_id;
      const channelSession = channelId ? await peekThreadSession(channelId) : null;
      if (channelSession && channelId) {
        await removeThreadSession(channelId);
        await respondToInteraction(interaction, {
          content: `Channel session reset (was \`${channelSession.sessionId.slice(0, 8)}\`, ${channelSession.turnCount} turns). Next message starts fresh.`,
        });
      } else {
        await resetSession();
        await respondToInteraction(interaction, {
          content: "Global session reset. Next message starts fresh.",
        });
      }
      return;
    }

    if (interaction.data.name === "compact") {
      await respondToInteraction(interaction, { content: "⏳ Compacting session..." });
      const result = await compactCurrentSession();
      await fetch(
        `${DISCORD_API}/webhooks/${applicationId}/${interaction.token}/messages/@original`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ content: result.message }),
        },
      );
      return;
    }

    if (interaction.data.name === "status") {
      const settings = getSettings();
      const channelId = interaction.channel_id;
      // Look up session for the channel this command was used in
      const channelSession = channelId ? await peekThreadSession(channelId) : null;
      const globalSession = await peekSession();
      const session = channelSession ?? globalSession;
      if (!session) {
        await respondToInteraction(interaction, { content: "📊 No active session." });
        return;
      }

      const lines = ["📊 **Session Status**"];
      lines.push(`Session: \`${session.sessionId.slice(0, 8)}\``);
      lines.push(`Turns: ${(session as any).turnCount ?? 0}`);
      lines.push(`Model: ${settings.model || "default"}`);
      lines.push(`Security: ${settings.security.level}`);
      lines.push(`Created: ${session.createdAt}`);
      lines.push(`Last used: ${session.lastUsedAt}`);

      // Session file size
      const home = homedir();
      const channelProjectDir = channelId ? settings.discord?.channelProjects?.[channelId] : undefined;
      const projectSlug = (channelProjectDir ?? process.cwd()).replace(/\//g, "-");
      const jsonlPath = `${home}/.claude/projects/${projectSlug}/${session.sessionId}.jsonl`;
      if (existsSync(jsonlPath)) {
        const fileSizeMB = (statSync(jsonlPath).size / 1024 / 1024).toFixed(1);
        lines.push(`Session size: ${fileSizeMB} MB`);

        // Token usage from last turn
        try {
          const raw = await readFile(jsonlPath, "utf8");
          let lastUsage: any = null;
          let totalOutput = 0;
          for (const line of raw.trim().split("\n")) {
            try {
              const obj = JSON.parse(line);
              if (obj.message?.usage) lastUsage = obj.message.usage;
              if (obj.message?.usage?.output_tokens) totalOutput += obj.message.usage.output_tokens;
            } catch {}
          }
          if (lastUsage) {
            const input = lastUsage.input_tokens ?? 0;
            const cacheCreation = lastUsage.cache_creation_input_tokens ?? 0;
            const cacheRead = lastUsage.cache_read_input_tokens ?? 0;
            const totalContext = input + cacheCreation + cacheRead;
            const maxContext = 200000;
            const pct = ((totalContext / maxContext) * 100).toFixed(1);
            const filled = Math.round(Math.min(totalContext / maxContext, 1) * 20);
            const bar = "█".repeat(filled) + "░".repeat(20 - filled);
            lines.push(``, `**Context Window**`);
            lines.push(`${bar} ${pct}%`);
            lines.push(`Total: \`${totalContext.toLocaleString()}\` / \`${maxContext.toLocaleString()}\` tokens`);
            lines.push(`├ Input: \`${input.toLocaleString()}\``);
            lines.push(`├ Cache read: \`${cacheRead.toLocaleString()}\``);
            lines.push(`└ Output (cumulative): \`${totalOutput.toLocaleString()}\``);
          }
        } catch {}
      }

      // Memory files
      const memoryDir = channelProjectDir ? join(channelProjectDir, "memory") : null;
      if (memoryDir && existsSync(memoryDir)) {
        try {
          const { readdir: readdirAsync } = await import("node:fs/promises");
          const memFiles = (await readdirAsync(memoryDir)).filter((f: string) => f.endsWith(".md") && f !== "MEMORY.md");
          lines.push(``, `**Memory**`);
          lines.push(`Files: ${memFiles.length}`);
          for (const mf of memFiles.slice(0, 8)) {
            lines.push(`  • ${mf.replace(/\.md$/, "")}`);
          }
          if (memFiles.length > 8) lines.push(`  ... and ${memFiles.length - 8} more`);
        } catch {}
      }

      await respondToInteraction(interaction, { content: lines.join("\n") });
      return;
    }

    if (interaction.data.name === "context") {
      const session = await peekSession();
      if (!session) {
        await respondToInteraction(interaction, { content: "No active session." });
        return;
      }
      const home = homedir();
      const projectSlug = process.cwd().replace(/\//g, "-");
      const jsonlPath = `${home}/.claude/projects/${projectSlug}/${session.sessionId}.jsonl`;
      if (!existsSync(jsonlPath)) {
        await respondToInteraction(interaction, { content: "Conversation file not found." });
        return;
      }
      try {
        const raw = await readFile(jsonlPath, "utf8");
        const fileLines = raw.trim().split("\n");
        let lastUsage: any = null;
        let totalOutput = 0;
        for (const line of fileLines) {
          try {
            const obj = JSON.parse(line);
            if (obj.message?.usage) lastUsage = obj.message.usage;
            if (obj.message?.usage?.output_tokens) totalOutput += obj.message.usage.output_tokens;
          } catch {}
        }
        if (!lastUsage) {
          await respondToInteraction(interaction, { content: "No usage data found." });
          return;
        }
        const input = lastUsage.input_tokens ?? 0;
        const cacheCreation = lastUsage.cache_creation_input_tokens ?? 0;
        const cacheRead = lastUsage.cache_read_input_tokens ?? 0;
        const totalContext = input + cacheCreation + cacheRead;
        const maxContext = 200000;
        const pct = ((totalContext / maxContext) * 100).toFixed(1);
        const filled = Math.round((Math.min(totalContext / maxContext, 1)) * 20);
        const bar = "█".repeat(filled) + "░".repeat(20 - filled);
        const msg = [
          `📐 **Context Window**`,
          `${bar} ${pct}%`,
          ``,
          `Total: \`${totalContext.toLocaleString()}\` / \`${maxContext.toLocaleString()}\` tokens`,
          `├ Input: \`${input.toLocaleString()}\``,
          `├ Cache creation: \`${cacheCreation.toLocaleString()}\``,
          `├ Cache read: \`${cacheRead.toLocaleString()}\``,
          `└ Output (cumulative): \`${totalOutput.toLocaleString()}\``,
          ``,
          `Turns: ${(session as any).turnCount ?? 0}`,
        ];
        await respondToInteraction(interaction, { content: msg.join("\n") });
      } catch (err) {
        await respondToInteraction(interaction, {
          content: `Failed to read context: ${err instanceof Error ? err.message : err}`,
        });
      }
      return;
    }

    if (interaction.data.name === "stop") {
      const threadId = interaction.guild_id ? interaction.channel_id : undefined;
      const stopped = stopRunningSession(threadId);
      await respondToInteraction(interaction, {
        content: stopped ? "⏹️ Stopped." : "Nothing is running in this channel.",
      });
      return;
    }

    if (interaction.data.name === "jobs") {
      const jobs = await loadJobs();
      if (jobs.length === 0) {
        await respondToInteraction(interaction, { content: "No scheduled jobs." });
        return;
      }
      const lines = [`📋 **Cron Jobs** (${jobs.length})`];
      for (const job of jobs) {
        lines.push("");
        const recurring = job.recurring ? "recurring" : "one-shot";
        const channel = job.channelId ? `<#${job.channelId}>` : "global";
        lines.push(`**${job.name}** \`${job.schedule}\` (${recurring}) — ${channel}`);
        const promptSnippet = job.prompt.split("\n")[0].slice(0, 80);
        lines.push(promptSnippet + (job.prompt.length > 80 ? "…" : ""));
      }
      await respondToInteraction(interaction, { content: lines.join("\n") });
      return;
    }

    if (interaction.data.name === "deljob") {
      const jobName = (interaction.data as any).options?.[0]?.value as string;
      if (!jobName) {
        await respondToInteraction(interaction, { content: "Provide a job name." });
        return;
      }
      const JOBS_DIR = join(process.cwd(), ".claude", "claudeclaw", "jobs");
      const jobPath = join(JOBS_DIR, `${jobName}.md`);
      try {
        await unlink(jobPath);
        await respondToInteraction(interaction, { content: `✅ Deleted job: **${jobName}**` });
      } catch (err: any) {
        if (err.code === "ENOENT") {
          await respondToInteraction(interaction, { content: `Job not found: \`${jobName}\`\nUse \`/jobs\` to see available jobs.` });
        } else {
          await respondToInteraction(interaction, { content: `Failed to delete: ${err.message}` });
        }
      }
      return;
    }

    if (interaction.data.name === "monitor") {
      try {
        const monitorLines: string[] = [];

        // CPU
        const cpuRaw = await new Promise<string>((resolve) => {
          const proc = Bun.spawn(["top", "-l", "1", "-n", "0"], { stdout: "pipe", stderr: "pipe" });
          proc.stdout.text().then(t => resolve(t)).catch(() => resolve(""));
        });
        const cpuLine = cpuRaw.split("\n").find(l => l.includes("CPU usage")) ?? "";

        // Memory via vm_stat
        const vmRaw = await new Promise<string>((resolve) => {
          const proc = Bun.spawn(["vm_stat"], { stdout: "pipe", stderr: "pipe" });
          proc.stdout.text().then(t => resolve(t)).catch(() => resolve(""));
        });
        const pageSize = 4096;
        const parseVm = (key: string) => {
          const line = vmRaw.split("\n").find(l => l.includes(key));
          return line ? parseInt(line.replace(/[^0-9]/g, "")) * pageSize : 0;
        };
        const free = parseVm("Pages free");
        const active = parseVm("Pages active");
        const inactive = parseVm("Pages inactive");
        const wiredMem = parseVm("Pages wired");
        const totalMem = free + active + inactive + wiredMem;
        const usedMem = active + wiredMem;
        const usedGB = (usedMem / 1e9).toFixed(1);
        const totalGB = (totalMem / 1e9).toFixed(1);
        const freePct = totalMem > 0 ? ((free / totalMem) * 100).toFixed(0) : "?";

        // Disk
        const diskRaw = await new Promise<string>((resolve) => {
          const proc = Bun.spawn(["df", "-h", "/"], { stdout: "pipe", stderr: "pipe" });
          proc.stdout.text().then(t => resolve(t)).catch(() => resolve(""));
        });
        const diskLine = diskRaw.split("\n")[1] ?? "";
        const diskParts = diskLine.trim().split(/\s+/);
        const diskUsed = diskParts[2] ?? "?";
        const diskFree = diskParts[3] ?? "?";
        const diskPct = diskParts[4] ?? "?";

        // Top processes
        const psRaw = await new Promise<string>((resolve) => {
          const proc = Bun.spawn(["ps", "aux", "-r"], { stdout: "pipe", stderr: "pipe" });
          proc.stdout.text().then(t => resolve(t)).catch(() => resolve(""));
        });
        const topProcs = psRaw.split("\n").slice(1, 7)
          .filter(Boolean)
          .map(l => {
            const p = l.trim().split(/\s+/);
            const cpu = (p[2] ?? "0").padStart(5);
            const mem = (p[3] ?? "0").padStart(7);
            const name = (p[10] ?? "?").split("/").pop()!.slice(0, 18).padEnd(18);
            return `  \`${name}\` CPU ${cpu}%  MEM ${mem}`;
          }).join("\n");

        // Claude/browser procs
        const claudeRaw = await new Promise<string>((resolve) => {
          const proc = Bun.spawn(
            ["bash", "-c", "ps aux | grep -E 'claude|bun.*claudeclaw|playwright|chromium' | grep -v grep"],
            { stdout: "pipe", stderr: "pipe" }
          );
          proc.stdout.text().then(t => resolve(t)).catch(() => resolve(""));
        });
        const claudeProcs = claudeRaw.trim().split("\n").filter(Boolean)
          .map(l => {
            const p = l.trim().split(/\s+/);
            const cpu = p[2] ?? "0";
            const cmd = p.slice(10).join(" ").slice(0, 45);
            return `  \`${cmd}\` — CPU ${cpu}%`;
          });

        const lines = [
          "💻 **Machine Status**",
          "",
          `**CPU** ${cpuLine.trim() || "n/a"}`,
          `**RAM** ${usedGB} GB used / ${totalGB} GB total (${freePct}% free)`,
          `**Disk /** used: ${diskUsed}  free: ${diskFree}  (${diskPct})`,
          "",
          "**Top processes:**",
          topProcs || "  (none)",
        ];
        if (claudeProcs.length > 0) {
          lines.push("", "**Claude/browser:**");
          lines.push(...claudeProcs.slice(0, 5));
        }

        // --- Claude session usage ---
        try {
          const globalSession = await peekSession();
          const threadSessions = await listThreadSessions();
          const home = homedir();
          const projectSlug = process.cwd().replace(/\//g, "-");

          lines.push("", "**Claude sessions:**");
          lines.push(`  Global: ${globalSession ? `\`${globalSession.sessionId.slice(0,8)}\` (${(globalSession as any).turnCount ?? 0} turns)` : "none"}`);
          lines.push(`  Threads: ${threadSessions.length}`);

          // Aggregate token usage across all known sessions
          const allSessions: Array<{ id: string; label: string }> = [];
          if (globalSession) allSessions.push({ id: globalSession.sessionId, label: "global" });
          for (const ts of threadSessions) allSessions.push({ id: ts.sessionId, label: `thread ${ts.threadId.slice(0,8)}` });

          let totalInput = 0, totalOutput = 0, totalCacheRead = 0, totalCacheCreate = 0;
          const perSession: string[] = [];
          for (const s of allSessions.slice(0, 10)) {
            const jsonlPath = `${home}/.claude/projects/${projectSlug}/${s.id}.jsonl`;
            if (!existsSync(jsonlPath)) continue;
            try {
              const raw = await readFile(jsonlPath, "utf8");
              let sIn = 0, sOut = 0, sCR = 0, sCC = 0;
              for (const line of raw.trim().split("\n")) {
                try {
                  const obj = JSON.parse(line);
                  const u = obj.message?.usage;
                  if (!u) continue;
                  sIn += u.input_tokens ?? 0;
                  sOut += u.output_tokens ?? 0;
                  sCR += u.cache_read_input_tokens ?? 0;
                  sCC += u.cache_creation_input_tokens ?? 0;
                } catch {}
              }
              totalInput += sIn; totalOutput += sOut; totalCacheRead += sCR; totalCacheCreate += sCC;
              if (sIn + sOut > 0) {
                perSession.push(`  \`${s.label.slice(0,20).padEnd(20)}\` in: ${sIn.toLocaleString()} out: ${sOut.toLocaleString()}`);
              }
            } catch {}
          }
          // Sonnet 4.5 pricing approximation: $3/M input, $15/M output, $0.30/M cache read, $3.75/M cache write
          const cost = (totalInput / 1e6) * 3 + (totalOutput / 1e6) * 15 + (totalCacheRead / 1e6) * 0.30 + (totalCacheCreate / 1e6) * 3.75;

          lines.push("", "**Token usage (cumulative across sessions):**");
          lines.push(`  Input: ${totalInput.toLocaleString()}`);
          lines.push(`  Output: ${totalOutput.toLocaleString()}`);
          lines.push(`  Cache read: ${totalCacheRead.toLocaleString()}`);
          lines.push(`  Cache create: ${totalCacheCreate.toLocaleString()}`);
          lines.push(`  Estimated cost: $${cost.toFixed(2)}`);

          if (globalSession) {
            const globalPath = `${home}/.claude/projects/${projectSlug}/${globalSession.sessionId}.jsonl`;
            if (existsSync(globalPath)) {
              const raw = await readFile(globalPath, "utf8");
              let lastUsage: any = null;
              for (const line of raw.trim().split("\n")) {
                try {
                  const obj = JSON.parse(line);
                  if (obj.message?.usage) lastUsage = obj.message.usage;
                } catch {}
              }
              if (lastUsage) {
                const ctx = (lastUsage.input_tokens ?? 0) + (lastUsage.cache_creation_input_tokens ?? 0) + (lastUsage.cache_read_input_tokens ?? 0);
                const pct = ((ctx / 200000) * 100).toFixed(0);
                lines.push("", `**Global context:** ${ctx.toLocaleString()} / 200,000 tokens (${pct}%)`);
              }
            }
          }
        } catch (err) {
          lines.push("", `(session stats unavailable: ${err instanceof Error ? err.message : err})`);
        }

        await respondToInteraction(interaction, { content: lines.join("\n") });
      } catch (err) {
        await respondToInteraction(interaction, {
          content: `Monitor error: ${err instanceof Error ? err.message : err}`,
        });
      }
      return;
    }

    // Unknown command
    await respondToInteraction(interaction, { content: "Unknown command." });
    return;
  }

  // Button interactions (type 3) — secretary workflow
  if (interaction.type === 3 && interaction.data?.custom_id) {
    const customId = interaction.data.custom_id;

    // Secretary pattern: "sec_yes_<8hex>" or "sec_no_<8hex>"
    const secMatch = customId.match(/^sec_(yes|no)_([0-9a-f]{8})$/);
    if (secMatch) {
      const action = secMatch[1];
      const pendingId = secMatch[2];
      let responseText = "Server error";

      try {
        const resp = await fetch(`http://127.0.0.1:9999/confirm/${pendingId}/${action}`);
        const result = (await resp.json()) as { ok: boolean };
        responseText =
          action === "yes" && result.ok
            ? "Sent!"
            : result.ok
              ? "Dismissed"
              : "Not found";
      } catch {
        // server not running
      }

      await respondToInteraction(interaction, {
        content: responseText,
        flags: 64, // EPHEMERAL
      });
      return;
    }

    // Default button ack
    await respondToInteraction(interaction, { content: "OK", flags: 64 });
    return;
  }

  // Default ack for any other interaction type
  await respondToInteraction(interaction, { content: "OK", flags: 64 });
}

// --- Guild join handler ---

async function handleGuildCreate(token: string, guild: DiscordGuild): Promise<void> {
  const config = getSettings().discord;

  // Skip guilds we were already in at READY time
  if (readyGuildIds?.has(guild.id)) return;

  const channelId = guild.system_channel_id;
  if (!channelId) return;

  console.log(`[Discord] Joined guild: ${guild.name} (${guild.id})`);

  const eventPrompt =
    `[Discord system event] I was added to a guild.\n` +
    `Guild name: ${guild.name}\n` +
    `Guild id: ${guild.id}\n` +
    "Write a short first message for the server. Confirm I was added and explain how to trigger me (mention or reply).";

  try {
    const result = await run("discord", eventPrompt);
    if (result.exitCode !== 0) {
      await sendMessage(config.token, channelId, "I was added to this server. Mention me to start.");
      return;
    }
    await sendMessage(config.token, channelId, result.stdout || "I was added to this server.");
  } catch {
    await sendMessage(config.token, channelId, "I was added to this server. Mention me to start.");
  }
}

// --- Per-channel project auto-setup ---

const BASE_CHANNELS_DIR = join(process.cwd(), "channels");

function sanitizeChannelName(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9-_]/g, "-").replace(/-+/g, "-").slice(0, 40);
}

async function ensureChannelProject(channel: { id: string; name?: string; type?: number }): Promise<string | null> {
  // Only text channels (type 0), announcement channels (type 5), or unknown type
  if (channel.type !== undefined && channel.type !== 0 && channel.type !== 5) return null;

  const channelName = channel.name ? sanitizeChannelName(channel.name) : channel.id;
  const projectDir = join(BASE_CHANNELS_DIR, `${channelName}-${channel.id}`);

  await mkdir(projectDir, { recursive: true });

  const claudeMdPath = join(projectDir, "CLAUDE.md");
  const { existsSync } = await import("node:fs");
  if (!existsSync(claudeMdPath)) {
    const displayName = channel.name ?? channel.id;
    const jobsDir = join(process.cwd(), ".claude", "claudeclaw", "jobs");
    const content = `# Channel: ${displayName}

This is the project workspace for Discord channel **#${displayName}** (ID: ${channel.id}).

Work here is scoped to this channel's context and history.

## Cron Jobs

You can create scheduled jobs for this channel. Jobs live at:

\`\`\`
${jobsDir}
\`\`\`

### Job file format

Create a \`.md\` file in that directory:

\`\`\`markdown
---
schedule: "0 9 * * *"
recurring: true
channelId: ${channel.id}
projectDir: ${projectDir}
notify: true
---

Your prompt here. This runs in the #${displayName} channel session.
\`\`\`

- \`schedule\`: standard cron expression (minute hour day month weekday)
- \`recurring: true\` — repeats; \`false\` — runs once then clears schedule
- \`notify: true\` — post result to channel; \`false\` — silent; \`"error"\` — only on failure
- \`channelId\` and \`projectDir\` make the job run in this channel's session

### Managing jobs

- **List:** \`ls ${jobsDir}\`
- **View:** read the \`.md\` file

### Creating/deleting jobs via directives

**IMPORTANT: The \`.claude/\` directory is BLOCKED — you cannot use Write, Edit, Bash, or any tool to create or delete files there. Any attempt will fail. The ONLY way to manage jobs is via these directives in your reply:**

**Delete a job:**
\`\`\`
[deljob:job-name]
\`\`\`

**Create or replace a job:**
\`\`\`
[savejob:job-name]
---
schedule: "0 9 * * *"
recurring: true
channelId: ${channel.id}
projectDir: ${projectDir}
notify: true
---
Your prompt here.
[/savejob]
\`\`\`

Directives are stripped from the reply; the user sees a confirmation like "Saved job: **name**" or "Deleted job: **name**". Only use these when explicitly asked to create/delete jobs.

**Send a file to the user:**
\`\`\`
[send-file:/absolute/path/to/file.pdf]
\`\`\`
Use this whenever the user asks you to send, export, or share a file. Generate or locate the file first, then include the directive in your reply. The daemon uploads it directly to Discord.
`;
    await Bun.write(claudeMdPath, content);
    console.log(`[Discord] Created project for channel #${displayName}: ${projectDir}`);
  }

  // Update settings.json with channel → projectDir mapping
  const settingsPath = join(process.cwd(), ".claude", "claudeclaw", "settings.json");
  try {
    const raw = await Bun.file(settingsPath).json();
    if (!raw.discord) raw.discord = {};
    if (!raw.discord.channelProjects) raw.discord.channelProjects = {};
    if (!raw.discord.channelProjects[channel.id]) {
      raw.discord.channelProjects[channel.id] = projectDir;
      await Bun.write(settingsPath, JSON.stringify(raw, null, 2) + "\n");
      await reloadSettings();
    }
  } catch (e) {
    console.error(`[Discord] Failed to update settings for channel ${channel.id}:`, e);
  }

  return projectDir;
}

// --- Gateway WebSocket ---

function sendWs(data: unknown): void {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function sendHeartbeat(): void {
  sendWs({ op: GatewayOp.HEARTBEAT, d: lastSequence });
  heartbeatAcked = false;
}

function startHeartbeat(): void {
  stopHeartbeat();
  // First heartbeat with jitter per Discord spec
  heartbeatJitterTimer = setTimeout(() => {
    heartbeatJitterTimer = null;
    sendHeartbeat();
  }, Math.random() * heartbeatIntervalMs);
  heartbeatTimer = setInterval(() => {
    if (!heartbeatAcked) {
      debugLog("Heartbeat not acked, reconnecting");
      ws?.close(4000, "Heartbeat timeout");
      return;
    }
    sendHeartbeat();
  }, heartbeatIntervalMs);
}

function stopHeartbeat(): void {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = null;
  if (heartbeatJitterTimer) clearTimeout(heartbeatJitterTimer);
  heartbeatJitterTimer = null;
}

async function sendStartupMessage(token: string): Promise<void> {
  if (startupMessageSent) return;
  startupMessageSent = true;
  const filePath = join(process.cwd(), ".claude", "claudeclaw", "startup-message.txt");
  let text: string;
  try {
    text = (await readFile(filePath, "utf-8")).trim();
  } catch {
    return; // no startup message
  }
  try {
    await unlink(filePath);
  } catch {
    // best-effort delete
  }
  if (!text) return;
  const config = getSettings().discord;
  // Try listenChannels first, then fall back to channelProjects keys
  const candidates = [
    ...(config.listenChannels ?? []),
    ...Object.keys(config.channelProjects ?? {}),
  ];
  for (const channelId of candidates) {
    try {
      await sendMessage(token, channelId, text);
      return;
    } catch {
      // try next
    }
  }
}

function startThreadSubscriptionHeartbeat(token: string): void {
  if (threadSubscriptionTimer) return;
  threadSubscriptionTimer = setInterval(() => {
    const config = getSettings().discord;
    for (const [threadId, info] of knownThreads.entries()) {
      if (config.channelProjects?.[info.parentId]) {
        discordApi(token, "PUT", `/channels/${threadId}/thread-members/@me`).catch(() => {});
      }
    }
  }, 10 * 60 * 1000);
}

function stopThreadSubscriptionHeartbeat(): void {
  if (threadSubscriptionTimer) clearInterval(threadSubscriptionTimer);
  threadSubscriptionTimer = null;
}


function resetGatewayState(): void {
  heartbeatIntervalMs = 0;
  heartbeatAcked = true;
  lastSequence = null;
  gatewaySessionId = null;
  readyGuildIds = null;
  startupMessageSent = false;
  botUserId = null;
  botUsername = null;
  applicationId = null;
  knownThreads.clear();
}

function sendIdentify(token: string): void {
  sendWs({
    op: GatewayOp.IDENTIFY,
    d: {
      token,
      intents: INTENTS,
      properties: {
        os: process.platform,
        browser: "claudeclaw",
        device: "claudeclaw",
      },
    },
  });
}

// Non-recoverable close codes that should not trigger reconnection
const FATAL_CLOSE_CODES = new Set([4004, 4010, 4011, 4012, 4013, 4014]);

function handleDispatch(token: string, eventName: string, data: any): void {
  debugLog(`Dispatch: ${eventName}`);

  // Diagnostic: log every event that touches a known thread
  const eventChannelId = data?.channel_id || data?.id;
  if (eventChannelId && knownThreads.has(eventChannelId)) {
    const author = data?.author?.username || data?.user_id || "?";
    console.log(`[Discord][THREAD-EVT] ${eventName} ch=${eventChannelId} author=${author}`);
  }

  switch (eventName) {
    case "READY":
      gatewaySessionId = data.session_id;
      botUserId = data.user.id;
      botUsername = data.user.username;
      applicationId = data.application.id;
      // Track existing guilds so we don't send welcome messages on reconnect
      readyGuildIds = new Set((data.guilds ?? []).map((g: { id: string }) => g.id));
      console.log(`[Discord] Ready as ${data.user.username} (${data.user.id})`);
      registerSlashCommands(token).catch((err) =>
        console.error(`[Discord] Failed to register slash commands: ${err}`),
      );
      break;

    case "MESSAGE_CREATE":
      console.log(`[Discord][GW] MESSAGE_CREATE ch=${data.channel_id} author=${data.author?.username} guild=${data.guild_id || 'DM'}`);
      handleMessageCreate(token, data).catch((err) =>
        console.error(`[Discord] MESSAGE_CREATE unhandled:`, err),
      );
      break;

    case "INTERACTION_CREATE":
      handleInteractionCreate(token, data).catch((err) =>
        console.error(`[Discord] INTERACTION_CREATE unhandled: ${err}`),
      );
      break;

    case "GUILD_CREATE":
      // Cache active threads and explicitly rejoin each one so MESSAGE_CREATE is delivered
      if (data.threads) {
        console.log(`[Discord] GUILD_CREATE: ${data.threads.length} active threads in guild ${data.id}`);
        for (const thread of data.threads) {
          knownThreads.set(thread.id, { parentId: thread.parent_id });
          // Populate task dedup cache from thread name (e.g. "TASK-109: Перейменувати...")
          const taskMatch = typeof thread.name === "string" ? thread.name.match(/^(TASK-\d+):/) : null;
          if (taskMatch) {
            const key = `${thread.parent_id}:${taskMatch[1]}`;
            if (!taskIdToThread.has(key)) taskIdToThread.set(key, thread.id);
          }
          // Rejoin unconditionally — Discord only delivers MESSAGE_CREATE to thread members
          discordApi(token, "PUT", `/channels/${thread.id}/thread-members/@me`).catch((err) =>
            console.error(`[Discord] Failed to rejoin thread ${thread.id}: ${err}`)
          );
          console.log(`[Discord]   thread: ${thread.id} name="${thread.name}" parent=${thread.parent_id}`);
        }
      } else {
        console.log(`[Discord] GUILD_CREATE: no active threads in guild ${data.id}`);
      }
      // Auto-setup project directories for all text channels
      if (Array.isArray(data.channels)) {
        for (const ch of data.channels) {
          ensureChannelProject(ch).catch((err) =>
            console.error(`[Discord] Failed to setup channel project ${ch.id}: ${err}`),
          );
        }
      }
      // Rejoin all known threads from sessions.json so gateway sends MESSAGE_CREATE
      rejoinThreads(token).catch((err) =>
        console.error(`[Discord] Failed to rejoin threads: ${err}`),
      );
      handleGuildCreate(token, data).catch((err) =>
        console.error(`[Discord] GUILD_CREATE unhandled: ${err}`),
      );
      // Send startup message once after fresh connect (file deleted after send)
      sendStartupMessage(token).catch((err) =>
        console.error(`[Discord] Failed to send startup message: ${err}`),
      );
      break;

    case "CHANNEL_CREATE":
      if (data.id && data.guild_id) {
        ensureChannelProject(data).catch((err) =>
          console.error(`[Discord] Failed to setup new channel project ${data.id}: ${err}`),
        );
      }
      break;

    case "CHANNEL_DELETE":
      if (data.id && data.guild_id) {
        // Remove from settings mapping and session — folder stays on disk (history preserved)
        removeThreadSession(data.id).catch((err) =>
          console.error(`[Discord] Failed to remove session for deleted channel ${data.id}: ${err}`),
        );
        (async () => {
          const settingsPath = join(process.cwd(), ".claude", "claudeclaw", "settings.json");
          try {
            const raw = await Bun.file(settingsPath).json();
            if (raw.discord?.channelProjects?.[data.id]) {
              console.log(`[Discord] Channel deleted, unmapping project: ${data.id} → ${raw.discord.channelProjects[data.id]}`);
              delete raw.discord.channelProjects[data.id];
              await Bun.write(settingsPath, JSON.stringify(raw, null, 2) + "\n");
            }
          } catch (e) {
            console.error(`[Discord] Failed to unmap deleted channel ${data.id}:`, e);
          }
        })();
      }
      break;

    case "THREAD_CREATE":
      if (data.id && data.parent_id) {
        knownThreads.set(data.id, { parentId: data.parent_id });
        const taskMatch = typeof data.name === "string" ? data.name.match(/^(TASK-\d+):/) : null;
        if (taskMatch) {
          const key = `${data.parent_id}:${taskMatch[1]}`;
          if (!taskIdToThread.has(key)) taskIdToThread.set(key, data.id);
        }
        debugLog(`Thread tracked: ${data.id} (parent: ${data.parent_id})`);
      }
      break;

    case "THREAD_DELETE":
      if (data.id) {
        knownThreads.delete(data.id);
        // Drop dedup cache entries that point to this thread
        for (const [key, tid] of taskIdToThread) {
          if (tid === data.id) taskIdToThread.delete(key);
        }
        removeThreadSession(data.id).catch((err) =>
          console.error(`[Discord] Failed to cleanup thread session: ${err}`),
        );
        debugLog(`Thread removed: ${data.id}`);
      }
      break;

    case "THREAD_UPDATE":
      if (data.id && data.parent_id) {
        if (data.thread_metadata?.archived) {
          knownThreads.delete(data.id);
          removeThreadSession(data.id).catch((err) =>
            console.error(`[Discord] Failed to cleanup archived thread session: ${err}`),
          );
          debugLog(`Thread archived and cleaned up: ${data.id}`);
        } else {
          knownThreads.set(data.id, { parentId: data.parent_id });
        }
      }
      break;

    case "THREAD_LIST_SYNC":
      if (data.threads) {
        console.log(`[Discord] THREAD_LIST_SYNC: ${data.threads.length} thread(s)`);
        for (const thread of data.threads) {
          knownThreads.set(thread.id, { parentId: thread.parent_id });
          discordApi(token, "PUT", `/channels/${thread.id}/thread-members/@me`).catch((err) =>
            debugLog(`Failed to rejoin thread ${thread.id} on THREAD_LIST_SYNC: ${err}`)
          );
        }
      }
      break;
  }
}

function handleGatewayPayload(token: string, payload: GatewayPayload): void {
  if (payload.s !== null) lastSequence = payload.s;

  switch (payload.op) {
    case GatewayOp.HELLO:
      heartbeatIntervalMs = payload.d.heartbeat_interval;
      startHeartbeat();
      sendIdentify(token);
      break;

    case GatewayOp.HEARTBEAT_ACK:
      heartbeatAcked = true;
      break;

    case GatewayOp.HEARTBEAT:
      // Server-requested heartbeat
      sendHeartbeat();
      break;

    case GatewayOp.RECONNECT:
      debugLog("Gateway requested reconnect");
      ws?.close(4000, "Reconnect requested");
      break;

    case GatewayOp.INVALID_SESSION:
      debugLog(`Invalid session`);
      gatewaySessionId = null;
      lastSequence = null;
      setTimeout(() => sendIdentify(token), 1000 + Math.random() * 4000);
      break;

    case GatewayOp.DISPATCH:
      handleDispatch(token, payload.t!, payload.d);
      break;
  }
}

function connectGateway(token: string, url?: string): void {
  const gatewayUrl = url || GATEWAY_URL;
  debugLog(`Connecting to gateway: ${gatewayUrl}`);

  ws = new WebSocket(gatewayUrl);

  ws.onopen = () => {
    debugLog("Gateway WebSocket opened");
  };

  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(String(event.data)) as GatewayPayload;
      handleGatewayPayload(token, payload);
    } catch (err) {
      console.error(`[Discord] Failed to parse gateway payload: ${err}`);
    }
  };

  ws.onclose = (event) => {
    debugLog(`Gateway closed: code=${event.code} reason=${event.reason}`);
    stopHeartbeat();
    if (!running) return;

    // Fatal close codes — do not reconnect
    if (FATAL_CLOSE_CODES.has(event.code)) {
      console.error(`[Discord] Fatal close code ${event.code}: ${event.reason}. Not reconnecting.`);
      return;
    }

    // Always reconnect fresh — session resume skips GUILD_CREATE which breaks thread event delivery
    gatewaySessionId = null;
    lastSequence = null;
    setTimeout(() => connectGateway(token), 3000 + Math.random() * 4000);
  };

  ws.onerror = () => {
    // onclose will fire after onerror, reconnection handled there
  };
}

// --- Exports ---

/** Send a message to a specific channel (used by heartbeat forwarding) */
export { sendMessage, sendMessageToUser };

/** Stop gateway connection and clear runtime state (used for token rotation/hot reload). */
export function stopGateway(): void {
  running = false;
  stopHeartbeat();
  stopThreadSubscriptionHeartbeat();
  if (ws) {
    try {
      ws.close(1000, "Gateway stop requested");
    } catch {
      // best-effort
    }
    ws = null;
  }
  resetGatewayState();
}

process.on("SIGTERM", () => {
  stopGateway();
});
process.on("SIGINT", () => {
  stopGateway();
});

/** Start gateway connection in-process (called by start.ts when token is configured) */
export function startGateway(debug = false): void {
  discordDebug = debug;
  const config = getSettings().discord;
  if (ws) stopGateway();
  running = true;
  console.log("Discord bot started (gateway)");
  console.log(`  Allowed users: ${config.allowedUserIds.length === 0 ? "all" : config.allowedUserIds.join(", ")}`);
  if (config.listenChannels.length > 0) {
    console.log(`  Listen channels: ${config.listenChannels.join(", ")}`);
  }
  if (discordDebug) console.log("  Debug: enabled");

  (async () => {
    await ensureProjectClaudeMd();
    connectGateway(config.token);
    startThreadSubscriptionHeartbeat(config.token);
  })().catch((err) => {
    console.error(`[Discord] Fatal: ${err}`);
  });
}

/** Standalone entry point (bun run src/index.ts discord) */
export async function discord() {
  await loadSettings();
  await ensureProjectClaudeMd();
  const config = getSettings().discord;

  if (!config.token) {
    console.error("Discord token not configured. Set discord.token in .claude/claudeclaw/settings.json");
    process.exit(1);
  }

  console.log("Discord bot started (gateway, standalone)");
  console.log(`  Allowed users: ${config.allowedUserIds.length === 0 ? "all" : config.allowedUserIds.join(", ")}`);
  if (discordDebug) console.log("  Debug: enabled");

  connectGateway(config.token);
  // Keep process alive
  await new Promise(() => {});
}
