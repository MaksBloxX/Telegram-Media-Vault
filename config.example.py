# ─────────────────────────────────────────────────────────────
#  COPY THIS FILE TO  config.py  AND FILL IN YOUR OWN VALUES.
#  config.py is git-ignored — NEVER commit real tokens.
#    Windows : Copy-Item config.example.py config.py
#    Termux  : cp config.example.py config.py
# ─────────────────────────────────────────────────────────────

# --- Bot tokens from @BotFather. Order matters: the FIRST bot is "Bot 1",
#     the only one that posts "Grouping media..." status messages. ---
BOT_TOKENS = [
    "1111111111:AA-REPLACE-WITH-YOUR-REAL-TOKEN-1",   # Bot 1 (announcer)
    # "2222222222:AA-REPLACE-WITH-YOUR-REAL-TOKEN-2", # Bot 2 (optional)
]

# --- Your numeric Telegram user id (get it from @userinfobot). ---
ADMIN_ID = 0

# --- Database file name ---
DB_NAME = "media.db"

# --- Timers & limits ---
ALBUM_BATCH_DELAY = 3.5   # seconds to wait before flushing an album/queue
QUEUE_COOLDOWN    = 2.0   # pause between album chunks (flood protection)
MAX_DELETE_CHUNK  = 100   # max messages deleted in one batch
MAX_SEEN_MEDIA    = 5000  # RAM cache size for no-DB mode

# --- Default settings (per bot, toggleable at runtime) ---
DEFAULT_SETTINGS = {
    "autodelete"     : True,   # delete original messages after reposting
    "custom_caption" : None,   # custom caption (None = off)
    "auto_group"     : True,   # smart auto-grouping (/gp)
    "db_check"       : True    # duplicate blocking (/db)
}
