import asyncio
import config
import time
import hashlib
import logging
import signal
import sys
import aiosqlite
from collections import OrderedDict
from telegram import Update, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import RetryAfter, TelegramError

# --- WINDOWS CONSOLE FIX: reconfigure stdout/stderr to UTF-8 so emoji log lines
# never trigger UnicodeEncodeError on cp1252 terminals ("replace" = worst case a '?').
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# --- LOGGING SETUP (replaces silent except: pass) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("vault_bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
# --- TOKEN LEAK FIX: httpx logs full request URLs (which CONTAIN BOT TOKENS) at INFO.
# Silence it so vault_bot.log never stores your tokens.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# --- INDEPENDENT BOT MEMORY SYSTEM ---
bot_states = {}
# Shared persistent DB connection (opened once at startup, not per-message)
_db_conn: aiosqlite.Connection = None

# --- BOT LABELS: token -> "Bot 1", "Bot 2"... (token order in config.py) ---
# Filled once at startup in run_multiple_bots(). Used for the bot_name column.
TOKEN_LABELS = {}

def bot_label(bot) -> str:
    return TOKEN_LABELS.get(bot.token, "Unknown")

def is_announcer(bot) -> bool:
    """Only Bot 1 (first token in config) posts 'Grouping media...' status.
    Prevents 9 bots from each posting their own status message."""
    return bool(config.BOT_TOKENS) and bot.token == config.BOT_TOKENS[0]

# --- GP SMART GROUPING THRESHOLD ---
# Incoming albums with >= this many items are passed as-is (they already look
# good as a big album). Albums smaller than this join the auto-group queue.
GP_ALBUM_SKIP_MIN = 6

# --- ARMED INSPECT MODE (shared by ALL bots in this process) ---
# /dbfind or /dbdel sent WITHOUT a reply arms the mode: the next media you
# forward is intercepted by EVERY bot BEFORE the vault pipeline sees it —
# no insert, no duplicate count, no grouping, no autodelete. Only ONE bot
# answers (responder), the rest silently swallow their copies (grace window).
INSPECT = {
    "mode": None,          # None | "find" | "del"
    "task": None,          # auto-disarm timeout task
    "responder": None,     # bot id that answers
    "grace_until": 0.0,    # swallow window covering all bots' update copies
    "albums": {},          # media_group_id -> {'m': [], 'ids': [], 'task': None}
}
INSPECT_TIMEOUT = 60.0   # armed mode auto-cancels after this many seconds
INSPECT_GRACE   = 5.0    # seconds all bots keep swallowing copies after a hit

class QueueState:
    def __init__(self):
        self.media, self.message_ids = [], []
        self.timer_task = None
        self.chat_id = None
        self.processing_msg_ids = []   # ALL status msg ids — list, so none get orphaned

def get_state(bot_id):
    if bot_id not in bot_states:
        bot_states[bot_id] = {
            "settings": config.DEFAULT_SETTINGS.copy(),
            "queue": QueueState(),
            "off_mode_albums": {},
            "gp_albums": {},             # GP smart grouping: per-album collector
            "album_lock": asyncio.Lock(),
            "db_enabled": config.DEFAULT_SETTINGS.get("db_check", True),  # /db toggle — default from config.py
            "last_warn": 0.0
        }
    return bot_states[bot_id]

# --- SECURITY FILTER: Only your ADMIN_ID can trigger anything ---
class AdminFilter(filters.MessageFilter):
    def filter(self, message):
        return message.from_user and message.from_user.id == config.ADMIN_ID

admin_filter = AdminFilter()

# --- DATABASE: Open once, reuse forever (fixes per-message open/close lag) ---
async def init_db():
    global _db_conn
    _db_conn = await aiosqlite.connect(config.DB_NAME)
    await _db_conn.execute("PRAGMA journal_mode=WAL")      # Prevents DB corruption on crash
    await _db_conn.execute("PRAGMA synchronous=NORMAL")    # Faster writes, still safe
    await _db_conn.execute("PRAGMA busy_timeout=5000")     # Waits instead of erroring if DB momentarily locked
    await _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS media_vault (
            file_hash TEXT PRIMARY KEY,
            bot_name TEXT,
            created_at REAL,
            duplicate_count INTEGER NOT NULL DEFAULT 0,
            last_duplicate_at REAL
        )
    """)
    # --- SAFE MIGRATION: only ADDS missing columns. Existing rows NEVER modified. ---
    cursor = await _db_conn.execute("PRAGMA table_info(media_vault)")
    existing_cols = {row[1] for row in await cursor.fetchall()}
    new_columns = {
        "bot_name":          "ALTER TABLE media_vault ADD COLUMN bot_name TEXT",
        "created_at":        "ALTER TABLE media_vault ADD COLUMN created_at REAL",
        "duplicate_count":   "ALTER TABLE media_vault ADD COLUMN duplicate_count INTEGER NOT NULL DEFAULT 0",
        "last_duplicate_at": "ALTER TABLE media_vault ADD COLUMN last_duplicate_at REAL",
    }
    for col, stmt in new_columns.items():
        if col not in existing_cols:
            await _db_conn.execute(stmt)
            log.info(f"DB migration: added column '{col}' (old data untouched)")
    await _db_conn.commit()
    log.info("Database initialized successfully.")

async def close_db():
    global _db_conn
    if _db_conn:
        await _db_conn.close()
        log.info("Database closed cleanly.")

# --- Tiny DB fetch helpers (used by /dbstats) ---
async def db_fetchone(sql, params=()):
    cursor = await _db_conn.execute(sql, params)
    return await cursor.fetchone()

async def db_fetchall(sql, params=()):
    cursor = await _db_conn.execute(sql, params)
    return await cursor.fetchall()

def fmt_time(ts):
    """Human time or dash for NULL (old rows have no timestamp)."""
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts)) if ts else "—"

def short_hash(h):
    return f"{h[:12]}…" if h else "—"

def hash_id(unique_id: str) -> str:
    """SHA-256 hash of file ID — KEPT: matches your existing 185K-hash media.db."""
    return hashlib.sha256(unique_id.encode()).hexdigest()

def get_unique_id(msg):
    if msg.photo: return msg.photo[-1].file_unique_id
    if msg.video: return msg.video.file_unique_id
    if msg.document: return msg.document.file_unique_id
    return None

# --- SAFE DELETE with proper error logging ---
async def delete_msg(msg, delay=0):
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        await msg.delete()
    except RetryAfter as e:
        log.warning(f"Rate limit on delete — waiting {e.retry_after}s")
        await asyncio.sleep(e.retry_after + 0.5)
        try:
            await msg.delete()
        except TelegramError as e2:
            log.debug(f"Delete retry failed (likely already deleted): {e2}")
    except TelegramError as e:
        log.debug(f"Delete failed (likely already deleted): {e}")

# --- MEDIA CONSTRUCTOR ---
def get_media_obj(msg, cap=None, cap_entities=None, parse_mode=None):
    kwargs = {"caption": cap}
    if cap_entities and not parse_mode:
        kwargs["caption_entities"] = cap_entities
    elif parse_mode:
        kwargs["parse_mode"] = parse_mode
    if msg.photo: return InputMediaPhoto(msg.photo[-1].file_id, **kwargs)
    if msg.video: return InputMediaVideo(msg.video.file_id, **kwargs)
    if msg.document: return InputMediaDocument(msg.document.file_id, **kwargs)
    return None

# --- ADMIN COMMANDS ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    m = await update.message.reply_text("🚀 **Vault Active**.", parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 5))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    text = (
        "⚙️ **Commands**:\n"
        "/gp — Toggle Grouping\n"
        "/autodelete — Toggle Auto-Delete\n"
        "/addcaption [text] — Set Caption\n"
        "/removecaption — Clear Caption\n"
        "/db — Toggle Duplicate-Check\n"
        "/dbstats — Database Report\n"
        "/dbclear — Reset Duplicate Stats (keeps hashes)\n"
        "/dbfind — (reply) Info • or alone, then forward\n"
        "/dbdel — (reply) Delete • or alone, then forward\n"
        "/settings — Show Status"
    )
    m = await update.message.reply_text(text, parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 15))

async def gp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["auto_group"] = not state["settings"]["auto_group"]
    status = "ON" if state["settings"]["auto_group"] else "OFF"
    m = await update.message.reply_text(f"Auto-grouper: **{status}**", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

async def autodelete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["autodelete"] = not state["settings"]["autodelete"]
    status = "ON" if state["settings"]["autodelete"] else "OFF"
    m = await update.message.reply_text(f"Auto-delete: **{status}**", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

# --- NEW: /db — toggle duplicate-check per bot (default ON) ---
# When OFF: media is still RECORDED + duplicates still COUNTED, but nothing is blocked.
async def db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["db_enabled"] = not state["db_enabled"]
    status = "ON" if state["db_enabled"] else "OFF"
    note = "" if state["db_enabled"] else " (still recording stats)"
    m = await update.message.reply_text(f"Duplicate-check: **{status}**{note}", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

# --- NEW: /dbstats — full database report ---
async def dbstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    try:
        total_files = (await db_fetchone("SELECT COUNT(*) FROM media_vault"))[0]
        total_dups = (await db_fetchone(
            "SELECT COALESCE(SUM(duplicate_count), 0) FROM media_vault"
        ))[0]

        per_bot = await db_fetchall("""
            SELECT COALESCE(bot_name, 'Unknown'),
                   COUNT(*),
                   COALESCE(SUM(duplicate_count), 0)
            FROM media_vault
            GROUP BY COALESCE(bot_name, 'Unknown')
            ORDER BY COUNT(*) DESC
        """)

        top_dup = await db_fetchone("""
            SELECT file_hash, duplicate_count FROM media_vault
            WHERE duplicate_count > 0
            ORDER BY duplicate_count DESC, last_duplicate_at DESC
            LIMIT 1
        """)

        recent = await db_fetchall("""
            SELECT file_hash, COALESCE(bot_name, 'Unknown'), created_at
            FROM media_vault
            WHERE created_at IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 3
        """)
    except Exception as e:
        log.error(f"/dbstats query failed: {e}")
        m = await update.message.reply_text("❌ DB error — check vault_bot.log.")
        asyncio.create_task(delete_msg(m, 8))
        return

    lines = [
        "📊 **DATABASE REPORT**",
        f"📦 Files saved: `{total_files}`",
        f"🚫 Duplicates blocked: `{total_dups}`",
        "",
        "🤖 **Per-bot:**",
    ]
    for name, files, dups in per_bot:
        lines.append(f"• {name} — `{files}` files / `{dups}` dups")

    if top_dup:
        lines += ["", f"🔁 Most duplicated: `{short_hash(top_dup[0])}` — **{top_dup[1]}×**"]

    if recent:
        lines += ["", "🕒 **Latest 3:**"]
        for h, bn, ts in recent:
            lines.append(f"• `{short_hash(h)}` — {bn} — {fmt_time(ts)}")

    m = await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 60))

# --- NEW: /dbclear — reset stats only, KEEP all hashes (blocking stays active) ---
async def dbclear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    try:
        await _db_conn.execute(
            "UPDATE media_vault SET duplicate_count = 0, last_duplicate_at = NULL"
        )
        await _db_conn.commit()
        m = await update.message.reply_text("🧹 Stats reset. Hash vault untouched — blocking still active.")
    except Exception as e:
        log.error(f"/dbclear failed: {e}")
        m = await update.message.reply_text("❌ DB error — check vault_bot.log.")
    asyncio.create_task(delete_msg(m, 8))

# --- /dbfind — reply = instant lookup • no reply = ARM then forward (no side effects) ---
async def dbfind_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    reply = update.message.reply_to_message
    uid = get_unique_id(reply) if reply else None

    # MODE 1: reply to media → instant lookup (unchanged behavior)
    if uid:
        try:
            f_hash, row = await inspect_lookup(uid)
        except Exception as e:
            log.error(f"/dbfind query failed: {e}")
            m = await update.message.reply_text("❌ DB error — check vault_bot.log.")
        else:
            m = await update.message.reply_text(format_find_text(uid, f_hash, row), parse_mode="Markdown")
        asyncio.create_task(delete_msg(m, 20))
        return

    # MODE 2: no reply → arm "find" for the next forwarded media
    if reply is not None:
        m = await update.message.reply_text("⚠️ Reply to a photo/video — or send /dbfind alone, then forward the media.")
        asyncio.create_task(delete_msg(m, 8))
        return
    arm_inspect("find", update.message.chat_id, context)
    m = await update.message.reply_text(
        "📥 **Find mode ON** — forward the media now (60s).\n"
        "It gets checked WITHOUT being saved, counted, grouped or deleted.",
        parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 20))

# --- /dbdel — reply = instant delete • no reply = ARM then forward (no side effects) ---
async def dbdel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    reply = update.message.reply_to_message
    uid = get_unique_id(reply) if reply else None

    # MODE 1: reply to media → instant delete (unchanged behavior)
    if uid:
        try:
            f_hash, row = await inspect_lookup(uid)
            if row and await delete_record(f_hash):
                log.info(f"/dbdel removed {short_hash(f_hash)} from vault.")
                m = await update.message.reply_text(
                    f"🗑 **Deleted from vault:** `{short_hash(f_hash)}`\n"
                    f"This file will now pass as NEW if sent again.",
                    parse_mode="Markdown")
            else:
                m = await update.message.reply_text("❌ Not found in vault — nothing deleted.")
        except Exception as e:
            log.error(f"/dbdel failed: {e}")
            m = await update.message.reply_text("❌ DB error — check vault_bot.log.")
        asyncio.create_task(delete_msg(m, 10))
        return

    # MODE 2: no reply → arm "del" for the next forwarded media
    if reply is not None:
        m = await update.message.reply_text("⚠️ Reply to a photo/video — or send /dbdel alone, then forward the media.")
        asyncio.create_task(delete_msg(m, 8))
        return
    arm_inspect("del", update.message.chat_id, context)
    m = await update.message.reply_text(
        "🗑 **Delete mode ON** — forward the media now (60s).\n"
        "Only its vault RECORD is deleted — the media itself is never processed.",
        parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 20))

# --- INSPECT MODE HELPERS ---
def arm_inspect(mode, chat_id, context):
    """Arm inspect mode globally (all bots). Resets any previous arming."""
    if INSPECT["task"]:
        INSPECT["task"].cancel()
    INSPECT["mode"] = mode
    INSPECT["responder"] = None
    INSPECT["grace_until"] = 0.0
    INSPECT["albums"].clear()
    INSPECT["task"] = asyncio.create_task(inspect_timeout(chat_id, context))
    log.info(f"Inspect mode armed: {mode}")

def disarm_inspect():
    """Turn mode off. grace_until is KEPT so other bots still swallow their
    copies of the just-inspected media for a few seconds."""
    INSPECT["mode"] = None
    INSPECT["responder"] = None
    if INSPECT["task"]:
        INSPECT["task"].cancel()
        INSPECT["task"] = None

async def inspect_timeout(chat_id, context):
    try:
        await asyncio.sleep(INSPECT_TIMEOUT)
        INSPECT["mode"] = None
        INSPECT["responder"] = None
        INSPECT["task"] = None
        INSPECT["grace_until"] = 0.0
        INSPECT["albums"].clear()
        m = await context.bot.send_message(chat_id, "⌛ Inspect mode timed out — nothing was touched.")
        asyncio.create_task(delete_msg(m, 5))
    except asyncio.CancelledError:
        pass

async def inspect_lookup(uid):
    f_hash = hash_id(uid)
    row = await db_fetchone(
        "SELECT bot_name, created_at, duplicate_count, last_duplicate_at "
        "FROM media_vault WHERE file_hash = ?", (f_hash,)
    )
    return f_hash, row

def format_find_text(uid, f_hash, row):
    if row:
        bot_name, created_at, dup_count, last_dup = row
        return (f"🔍 **FOUND IN VAULT**\n"
                f"🆔 `file_unique_id`:\n`{uid}`\n"
                f"🔑 hash: `{short_hash(f_hash)}`\n"
                f"🤖 First saved by: `{bot_name or 'Unknown'}`\n"
                f"📅 First seen: `{fmt_time(created_at)}`\n"
                f"🔁 Duplicates blocked: `{dup_count}`\n"
                f"🕒 Last duplicate: `{fmt_time(last_dup)}`")
    return (f"❌ **NOT in vault** — this file is new.\n"
            f"🆔 `file_unique_id`:\n`{uid}`")

async def delete_record(f_hash) -> bool:
    cursor = await _db_conn.execute("DELETE FROM media_vault WHERE file_hash = ?", (f_hash,))
    await _db_conn.commit()
    return bool(cursor.rowcount and cursor.rowcount > 0)

async def run_inspect_single(msg, context):
    """Armed mode, single media: find/del WITHOUT the vault pipeline touching it."""
    uid = get_unique_id(msg)
    mode = INSPECT["mode"]
    try:
        f_hash, row = await inspect_lookup(uid)
        if mode == "find":
            m = await msg.reply_text(format_find_text(uid, f_hash, row), parse_mode="Markdown")
            asyncio.create_task(delete_msg(m, 20))
            return
        if row and await delete_record(f_hash):
            log.info(f"/dbdel (armed) removed {short_hash(f_hash)} from vault.")
            m = await msg.reply_text(
                f"🗑 **Deleted from vault:** `{short_hash(f_hash)}`\n"
                f"This file will now pass as NEW if sent again.",
                parse_mode="Markdown")
        else:
            m = await msg.reply_text("❌ Not found in vault — nothing deleted.")
        asyncio.create_task(delete_msg(m, 10))
    except Exception as e:
        log.error(f"armed inspect (single) failed: {e}")

async def run_inspect_album(mg_id, chat_id, context):
    """Armed mode + a whole album was forwarded: act on every item at once."""
    try:
        await asyncio.sleep(config.ALBUM_BATCH_DELAY)
        data = INSPECT["albums"].pop(mg_id, None)
        if not data or not INSPECT["mode"]:
            return
        msgs = data['m']
        mode = INSPECT["mode"]
        try:
            if mode == "find":
                lines, found = [], 0
                for i, m2 in enumerate(msgs, 1):
                    uid = get_unique_id(m2)
                    _, row = await inspect_lookup(uid)
                    found += 1 if row else 0
                    lines.append(f"{i}. {'✅' if row else '❌'} `{uid}`")
                text = (f"🔍 **Album check ({len(msgs)} items):** "
                        f"{found} in vault / {len(msgs) - found} new\n" + "\n".join(lines))
            else:
                deleted = 0
                for m2 in msgs:
                    uid = get_unique_id(m2)
                    f_hash, row = await inspect_lookup(uid)
                    if row and await delete_record(f_hash):
                        deleted += 1
                log.info(f"/dbdel (armed album) removed {deleted}/{len(msgs)} records.")
                text = (f"🗑 **Album delete done:** {deleted}/{len(msgs)} records removed.\n"
                        f"They will pass as NEW if sent again.")
            m = await context.bot.send_message(chat_id, text, parse_mode="Markdown")
            asyncio.create_task(delete_msg(m, 25))
        except Exception as e:
            log.error(f"armed inspect (album) failed: {e}")
    except asyncio.CancelledError:
        pass
    finally:
        disarm_inspect()   # grace window remains until it expires

async def addcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    text = update.message.text.replace("/addcaption", "").strip()
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["custom_caption"] = text if text else None
    m = await update.message.reply_text("✅ Caption updated." if text else "🗑 Caption cleared.")
    asyncio.create_task(delete_msg(m, 5))

async def removecaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["custom_caption"] = None
    m = await update.message.reply_text("🗑 Caption cleared.")
    asyncio.create_task(delete_msg(m, 5))

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    text = (f"**Current Settings**:\n"
            f"- GP: `{state['settings']['auto_group']}`\n"
            f"- Delete: `{state['settings']['autodelete']}`\n"
            f"- DB check: `{state['db_enabled']}`\n"
            f"- Caption: `{state['settings']['custom_caption']}`")
    m = await update.message.reply_text(text, parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 10))

# --- SAFE SEND HELPER: RetryAfter handled in one place ---
async def safe_send_media_group(bot, chat_id, media):
    """Sends a media group, retries once on flood wait."""
    try:
        await bot.send_media_group(chat_id=chat_id, media=media)
        return True
    except RetryAfter as e:
        log.warning(f"FloodWait on send_media_group — waiting {e.retry_after}s")
        await asyncio.sleep(e.retry_after + 1.5)
        try:
            await bot.send_media_group(chat_id=chat_id, media=media)
            return True
        except TelegramError as e2:
            log.error(f"send_media_group failed after retry: {e2}")
            return False
    except TelegramError as e:
        log.error(f"send_media_group failed: {e}")
        return False

async def safe_delete_messages(bot, chat_id, ids):
    """Deletes messages in chunks, handles flood wait."""
    for i in range(0, len(ids), config.MAX_DELETE_CHUNK):
        chunk = ids[i:i + config.MAX_DELETE_CHUNK]
        try:
            await bot.delete_messages(chat_id=chat_id, message_ids=chunk)
        except RetryAfter as e:
            log.warning(f"FloodWait on delete_messages — waiting {e.retry_after}s")
            await asyncio.sleep(e.retry_after + 0.5)
            try:
                await bot.delete_messages(chat_id=chat_id, message_ids=chunk)
            except TelegramError as e2:
                log.debug(f"Bulk delete retry failed: {e2}")
        except TelegramError as e:
            log.debug(f"Bulk delete failed: {e}")

# --- GP ON: QUEUE FLUSHER ---
async def flush_queue_delayed(context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    await asyncio.sleep(config.ALBUM_BATCH_DELAY)
    async with state["album_lock"]:
        if not state["queue"].media:
            return
        m_list = state["queue"].media[:]
        ids = state["queue"].message_ids[:]
        c_id = state["queue"].chat_id
        state["queue"].media.clear()
        state["queue"].message_ids.clear()
        state["queue"].timer_task = None

        v = [m for m in m_list if not isinstance(m, InputMediaDocument)]
        d = [m for m in m_list if isinstance(m, InputMediaDocument)]
        chunks = [v[i:i+10] for i in range(0, len(v), 10)] + \
                 [d[i:i+10] for i in range(0, len(d), 10)]

        for chunk in chunks:
            if not chunk:
                continue
            if state["settings"]["custom_caption"]:
                for i, m in enumerate(chunk):
                    chunk[i] = type(m)(
                        media=m.media,
                        caption=state["settings"]["custom_caption"] if i == 0 else None,
                        parse_mode="HTML" if i == 0 else None
                    )
            await safe_send_media_group(context.bot, c_id, chunk)
            await asyncio.sleep(config.QUEUE_COOLDOWN)

        if state["settings"]["autodelete"]:
            await safe_delete_messages(context.bot, c_id, ids)

        # Delete ALL queued status messages — fixes orphaned "Grouping media..." leftovers
        for pm_id in state["queue"].processing_msg_ids:
            try:
                await context.bot.delete_message(chat_id=c_id, message_id=pm_id)
            except TelegramError:
                pass
        state["queue"].processing_msg_ids.clear()

# --- GP ON: SMART ALBUM SORTER ---
# Collects each incoming album, then decides by size:
#   1-5 items  -> feed into the group queue (merged into albums of 10, old behavior)
#   6-10 items -> skip grouping, pass through as its own album (GP-OFF style)
async def route_gp_album_delayed(mg_id, chat_id, context):
    state = get_state(context.bot.id)
    try:
        await asyncio.sleep(config.ALBUM_BATCH_DELAY)
        if mg_id not in state["gp_albums"]:
            return
        data = state["gp_albums"].pop(mg_id)
        msgs, ids = data['m'], data['ids']

        # BIG ALBUM (6+): send as-is, don't regroup
        if len(msgs) >= GP_ALBUM_SKIP_MIN:
            final = []
            for i, m in enumerate(msgs):
                if state["settings"]["custom_caption"]:
                    cap = state["settings"]["custom_caption"] if i == 0 else None
                    pm = "HTML" if i == 0 else None
                    ent = None
                else:
                    cap, ent, pm = m.caption, m.caption_entities, None
                obj = get_media_obj(m, cap, ent, pm)
                if obj:
                    final.append(obj)
            if final:
                await safe_send_media_group(context.bot, chat_id, final)
                if state["settings"]["autodelete"]:
                    await safe_delete_messages(context.bot, chat_id, ids)
            return

        # SMALL ALBUM (1-5): feed items into the group queue (old behavior)
        for m in msgs:
            obj = get_media_obj(m, m.caption, m.caption_entities)
            if obj:
                state["queue"].chat_id = chat_id
                state["queue"].media.append(obj)
                state["queue"].message_ids.append(m.message_id)
        if state["queue"].media:
            if state["queue"].timer_task:
                state["queue"].timer_task.cancel()
            elif is_announcer(context.bot):
                try:
                    p = await context.bot.send_message(chat_id, "⏳ Grouping media...")
                    state["queue"].processing_msg_ids.append(p.message_id)
                except TelegramError:
                    pass
            state["queue"].timer_task = asyncio.create_task(flush_queue_delayed(context))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"gp_album router error: {e}")

# --- GP OFF: ALBUM DEBOUNCE ---
async def send_off_mode_album_delayed(mg_id, chat_id, context):
    state = get_state(context.bot.id)
    try:
        await asyncio.sleep(config.ALBUM_BATCH_DELAY)
        if mg_id not in state["off_mode_albums"]:
            return
        data = state["off_mode_albums"].pop(mg_id)
        msgs, ids = data['m'], data['ids']
        final = []
        for i, m in enumerate(msgs):
            if state["settings"]["custom_caption"]:
                cap = state["settings"]["custom_caption"] if i == 0 else None
                pm = "HTML" if i == 0 else None
                ent = None
            else:
                cap, ent, pm = m.caption, m.caption_entities, None
            obj = get_media_obj(m, cap, ent, pm)
            if obj:
                final.append(obj)
        if final:
            await safe_send_media_group(context.bot, chat_id, final)
            if state["settings"]["autodelete"]:
                await safe_delete_messages(context.bot, chat_id, ids)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"off_mode_album error: {e}")

# --- CENTRAL MESSAGE INTAKE ---
async def process_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    state = get_state(context.bot.id)
    chat_id = msg.chat_id

    # --- ARMED INSPECT INTERCEPT — runs BEFORE the vault pipeline touches anything ---
    now = time.time()
    is_media = bool(get_unique_id(msg))
    if is_media and now < INSPECT["grace_until"]:
        return  # swallow window: another bot's copy of the media being inspected
    if INSPECT["mode"]:
        if not is_media:
            return  # armed: ignore non-media noise until timeout
        if INSPECT["responder"] is None:
            INSPECT["responder"] = context.bot.id
            INSPECT["grace_until"] = now + INSPECT_GRACE
        if context.bot.id != INSPECT["responder"]:
            return  # other bots swallow silently — only ONE bot answers
        if msg.media_group_id:
            # Forwarded a whole album: collect it, then act on ALL items at once
            mg_id = msg.media_group_id
            if mg_id not in INSPECT["albums"]:
                INSPECT["albums"][mg_id] = {'m': [], 'ids': [], 'task': None}
            INSPECT["albums"][mg_id]['m'].append(msg)
            INSPECT["albums"][mg_id]['ids'].append(msg.message_id)
            INSPECT["grace_until"] = now + config.ALBUM_BATCH_DELAY + INSPECT_GRACE
            if INSPECT["albums"][mg_id]['task']:
                INSPECT["albums"][mg_id]['task'].cancel()
            INSPECT["albums"][mg_id]['task'] = asyncio.create_task(
                run_inspect_album(mg_id, chat_id, context)
            )
            return
        await run_inspect_single(msg, context)
        disarm_inspect()   # grace window stays active for the other bots' copies
        return

    # --- DUPLICATE CHECK via persistent DB connection (hash KEPT for 185K compatibility) ---
    unique_id = get_unique_id(msg)
    if unique_id:
        f_hash = hash_id(unique_id)
        try:
            cursor = await _db_conn.execute(
                "INSERT OR IGNORE INTO media_vault (file_hash, bot_name, created_at) VALUES (?, ?, ?)",
                (f_hash, bot_label(context.bot), time.time())
            )
            if cursor.rowcount == 0:
                # Already in vault → count the duplicate (bot_name/created_at never overwritten)
                await _db_conn.execute(
                    "UPDATE media_vault SET duplicate_count = duplicate_count + 1, "
                    "last_duplicate_at = ? WHERE file_hash = ?",
                    (time.time(), f_hash)
                )
            await _db_conn.commit()

            if cursor.rowcount == 0 and state["db_enabled"]:
                # /db ON → block it (original behavior, unchanged)
                current_time = time.time()
                if current_time - state["last_warn"] > 5.0:
                    state["last_warn"] = current_time
                    try:
                        warn_msg = await context.bot.send_message(
                            chat_id, "🗑️ **Duplicate — skipped.**", parse_mode="Markdown"
                        )
                        asyncio.create_task(delete_msg(warn_msg, 3))
                    except TelegramError:
                        pass
                if state["settings"]["autodelete"]:
                    await delete_msg(msg)
                return
            # If /db OFF → duplicate was recorded & counted above, but passes through.
        except Exception as e:
            log.error(f"DB error during duplicate check: {e}")
            # On DB error, continue processing rather than silently dropping

    # --- GROUPING MODE (with smart album sorter) ---
    if state["settings"]["auto_group"]:
        # Album item -> collect per media_group_id, size decides group vs. skip
        if msg.media_group_id:
            mg_id = msg.media_group_id
            if mg_id not in state["gp_albums"]:
                state["gp_albums"][mg_id] = {'m': [], 'ids': [], 'task': None}
            state["gp_albums"][mg_id]['m'].append(msg)
            state["gp_albums"][mg_id]['ids'].append(msg.message_id)
            if state["gp_albums"][mg_id]['task']:
                state["gp_albums"][mg_id]['task'].cancel()
            state["gp_albums"][mg_id]['task'] = asyncio.create_task(
                route_gp_album_delayed(mg_id, chat_id, context)
            )
            return
        # Single media (album of 1) -> straight into the group queue (unchanged)
        obj = get_media_obj(msg, msg.caption, msg.caption_entities)
        if not obj:
            return
        state["queue"].chat_id = chat_id
        state["queue"].media.append(obj)
        state["queue"].message_ids.append(msg.message_id)
        if state["queue"].timer_task:
            state["queue"].timer_task.cancel()
        elif is_announcer(context.bot):
            try:
                p = await context.bot.send_message(chat_id, "⏳ Grouping media...")
                state["queue"].processing_msg_ids.append(p.message_id)
            except TelegramError:
                pass
        state["queue"].timer_task = asyncio.create_task(flush_queue_delayed(context))
        return

    # --- ALBUM PASSTHROUGH (GP OFF) ---
    if msg.media_group_id:
        mg_id = msg.media_group_id
        if mg_id not in state["off_mode_albums"]:
            state["off_mode_albums"][mg_id] = {'m': [], 'ids': [], 'task': None}
        state["off_mode_albums"][mg_id]['m'].append(msg)
        state["off_mode_albums"][mg_id]['ids'].append(msg.message_id)
        if state["off_mode_albums"][mg_id]['task']:
            state["off_mode_albums"][mg_id]['task'].cancel()
        state["off_mode_albums"][mg_id]['task'] = asyncio.create_task(
            send_off_mode_album_delayed(mg_id, chat_id, context)
        )
    else:
        # --- SINGLE MEDIA: copy_message strips forward tag (no footprint) ---
        try:
            kwargs = {"chat_id": chat_id, "from_chat_id": chat_id, "message_id": msg.message_id}
            if state["settings"]["custom_caption"]:
                kwargs.update({"caption": state["settings"]["custom_caption"], "parse_mode": "HTML"})
            await context.bot.copy_message(**kwargs)
            if state["settings"]["autodelete"]:
                await delete_msg(msg)
            await asyncio.sleep(0.1)
        except RetryAfter as e:
            log.warning(f"FloodWait on copy_message — waiting {e.retry_after}s")
            await asyncio.sleep(e.retry_after + 1.5)
            try:
                await context.bot.copy_message(**kwargs)
                if state["settings"]["autodelete"]:
                    await delete_msg(msg)
            except TelegramError as e2:
                log.error(f"copy_message failed after retry: {e2}")
        except TelegramError as e:
            log.error(f"copy_message failed: {e}")

# --- BOT STARTUP ---
async def start_single_bot(app):
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

# --- GRACEFUL SHUTDOWN: Ctrl+C or VS Code stop won't corrupt the DB ---
async def shutdown(apps):
    log.info("Shutdown signal received — stopping bots cleanly...")
    for app in apps:
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception as e:
            log.error(f"Error stopping bot: {e}")
    await close_db()
    log.info("All bots stopped. DB closed. Safe to exit.")

async def run_multiple_bots():
    global TOKEN_LABELS
    TOKEN_LABELS = {token: f"Bot {i+1}" for i, token in enumerate(config.BOT_TOKENS)}

    await init_db()

    apps = []
    for token in config.BOT_TOKENS:
        app = (
            Application.builder()
            .token(token)
            .read_timeout(30.0)
            .write_timeout(30.0)
            .connect_timeout(30.0)
            .build()
        )
        app.add_handler(CommandHandler("start", start_command, filters=admin_filter))
        app.add_handler(CommandHandler("help", help_command, filters=admin_filter))
        app.add_handler(CommandHandler("gp", gp_command, filters=admin_filter))
        app.add_handler(CommandHandler("autodelete", autodelete_command, filters=admin_filter))
        app.add_handler(CommandHandler("db", db_command, filters=admin_filter))
        app.add_handler(CommandHandler("dbstats", dbstats_command, filters=admin_filter))
        app.add_handler(CommandHandler("dbclear", dbclear_command, filters=admin_filter))
        app.add_handler(CommandHandler("dbfind", dbfind_command, filters=admin_filter))
        app.add_handler(CommandHandler("dbdel", dbdel_command, filters=admin_filter))
        app.add_handler(CommandHandler("addcaption", addcaption_command, filters=admin_filter))
        app.add_handler(CommandHandler("removecaption", removecaption_command, filters=admin_filter))
        app.add_handler(CommandHandler("settings", settings_command, filters=admin_filter))
        app.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, process_message))
        apps.append(app)

    log.info(f"Connecting {len(apps)} bots to Telegram...")
    await asyncio.gather(*[start_single_bot(app) for app in apps])
    log.info(f"✅ {len(apps)} Vault Bots active.")

    # Graceful shutdown on Ctrl+C or system signal
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler — Ctrl+C still works

    # Cross-platform clean exit:
    # Linux/Termux → signal sets stop_event; Windows → Ctrl+C cancels the wait.
    # finally: guarantees clean DB close on BOTH paths (fixes Windows Ctrl+C traceback).
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        log.info("Ctrl+C received — running clean shutdown...")
    finally:
        await shutdown(apps)

if __name__ == "__main__":
    try:
        asyncio.run(run_multiple_bots())
    except KeyboardInterrupt:
        print("Bot stopped by user (Ctrl+C). Database closed safely.")
