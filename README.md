# 🗄️ Telegram Media Vault Bot (Multi-Bot Edition)

A single Python process that runs **multiple Telegram bots at once** on one shared SQLite "vault".
It de-duplicates media by hash, re-groups loose photos/videos into clean albums, strips forward
tags, applies a custom caption, and auto-deletes the originals — so your channel/saved chat stays
tidy with zero manual work.

Built with `python-telegram-bot` (async) + `aiosqlite`. Runs on **Windows (PowerShell)**,
**Linux**, and **Android (Termux)**.

> **Private tool.** Every handler is locked to one `ADMIN_ID`. Anyone else messaging the bot is
> ignored completely.

---

## 📑 Table of Contents
- [Features](#-features)
- [Why this design (advantages)](#-why-this-design-advantages)
- [How it works (logic flow)](#-how-it-works-logic-flow)
- [Commands](#-commands)
- [Safety Rules — read before uploading to GitHub](#-safety-rules--read-before-uploading-to-github)
- [Setup — Windows / VS Code / PowerShell](#-setup--windows--vs-code--powershell)
- [Setup — Termux (Android)](#-setup--termux-android)
- [Configuration reference](#-configuration-reference)
- [Files created at runtime](#-files-created-at-runtime)
- [Troubleshooting](#-troubleshooting)
- [Ideas / roadmap](#-ideas--roadmap)
- [Disclaimer](#-disclaimer)

---

## ✨ Features

| # | Feature | What it does |
|---|---------|--------------|
| 1 | **Multi-bot, one process** | Put 1–N tokens in `config.py`; all of them poll concurrently in a single `asyncio` loop. Bot 1 is the "announcer" so you don't get N status messages. |
| 2 | **SHA-256 duplicate vault** | Every photo/video/document's `file_unique_id` is hashed and stored. Re-sends are detected instantly, even months later. |
| 3 | **Duplicate statistics** | Per-hash `duplicate_count` + `last_duplicate_at`, plus which bot first saved it (`bot_name`). |
| 4 | **Smart auto-grouping (`/gp`)** | Loose singles are buffered for `ALBUM_BATCH_DELAY` seconds and re-sent as neat 10-item albums. Incoming albums with ≥ 6 items pass through untouched. |
| 5 | **Forward-tag stripping** | Uses `copy_message` / `send_media_group`, so the re-posted media has **no "Forwarded from"** header. |
| 6 | **Auto-delete (`/autodelete`)** | The original messages are removed after the clean copy is posted. |
| 7 | **Custom caption** | `/addcaption <HTML>` applies a caption to everything; `/removecaption` clears it. |
| 8 | **Armed Inspect Mode** | `/dbfind` or `/dbdel` with **no reply** arms a 60s window: the next forwarded media is *only* looked up / removed from the vault — never saved, counted, grouped, or deleted. All other bots silently swallow their copy so you get exactly one answer. |
| 9 | **Safe DB migration** | On startup, missing columns are `ALTER TABLE`-added. Existing rows are never rewritten — an old 185k-hash `media.db` keeps working. |
| 10 | **WAL + busy_timeout** | `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000` → crash-resistant and no "database is locked" spam. |
| 11 | **Flood-wait aware** | `RetryAfter` is caught on send *and* delete, sleeps `retry_after + margin`, then retries once. |
| 12 | **Graceful shutdown** | `SIGINT`/`SIGTERM` (and Windows Ctrl+C) stop pollers, then close the DB cleanly in a `finally:` block. |
| 13 | **Token-leak-proof logging** | `httpx`/`httpcore` loggers are forced to `WARNING`, because their INFO lines contain the full API URL **including your bot token**. |
| 14 | **Windows UTF-8 fix** | `stdout/stderr` reconfigured to UTF-8 so emoji log lines never crash a `cp1252` console. |

---

## 🚀 Why this design (advantages)

- **One connection, not one per message.** The SQLite handle is opened once in `init_db()` and
  reused — this removes the open/close latency that made per-message checks laggy.
- **Per-bot isolated state.** `get_state(bot.id)` gives each bot its own settings, queue, album
  buffers and lock, so one bot's `/gp` toggle never affects another's — while the *vault* stays
  shared and global.
- **Global inspect mode, single responder.** A `responder` id + `grace_until` timestamp guarantees
  that when N bots receive the same forwarded media, exactly one replies and N−1 stay quiet.
- **Idempotent duplicate write.** `INSERT OR IGNORE` + `cursor.rowcount == 0` does insert-or-detect
  in a single round trip; no `SELECT` then `INSERT` race.
- **Non-destructive by default.** `/db OFF` still records and counts duplicates, it just stops
  blocking. `/dbclear` resets stats but keeps every hash.
- **Fails open, not silent.** DB errors are logged and processing continues, instead of the media
  disappearing into a bare `except: pass`.
- **Cross-platform on purpose.** Signal handlers, console encoding and Ctrl+C paths are all handled
  for both Linux/Termux and Windows.

---

## 🔁 How it works (logic flow)

```
Message from ADMIN_ID
        │
        ├─ command?  ──► CommandHandler (admin_filter)
        │
        └─ media ────────────────────────────────────────────────┐
                                                                 │
   1. INSPECT intercept                                          │
      • grace window active?          → swallow, return          │
      • mode armed?  → one responder answers /dbfind or /dbdel   │
        (no insert, no count, no group, no delete) → return      │
                                                                 │
   2. DUPLICATE CHECK                                            │
      hash = SHA256(file_unique_id)                              │
      INSERT OR IGNORE …                                         │
        ├─ new row       → continue                              │
        └─ already exists→ duplicate_count += 1                  │
             ├─ /db ON   → warn (rate-limited 5s) + delete, stop │
             └─ /db OFF  → continue                              │
                                                                 │
   3. ROUTING                                                    │
      /gp ON                                                     │
        ├─ album ≥ 6 items → pass through as-is                  │
        ├─ album < 6 items → merge into auto-group queue         │
        └─ single          → queue, flush after ALBUM_BATCH_DELAY│
      /gp OFF                                                    │
        ├─ album  → rebuilt & re-sent as one album               │
        └─ single → copy_message (forward tag stripped)          │
                                                                 │
   4. AUTODELETE ON → originals deleted, chunked ≤ MAX_DELETE_CHUNK
```

---

## 🎛️ Commands

All commands are **admin-only** and self-destruct after a few seconds to keep the chat clean.

| Command | Description |
|---|---|
| `/start` | Health check — "Vault Active". |
| `/help` | Command list. |
| `/settings` | Show current toggles for this bot. |
| `/gp` | Toggle smart auto-grouping. |
| `/autodelete` | Toggle deletion of originals. |
| `/addcaption <text>` | Set a custom caption (HTML parse mode). |
| `/removecaption` | Clear the custom caption. |
| `/db` | Toggle duplicate **blocking** (stats keep recording either way). |
| `/dbstats` | Report: total files, total duplicates, per-bot breakdown, most-duplicated hash, latest 3. |
| `/dbclear` | Reset duplicate counters — **hashes are kept**, blocking stays active. |
| `/dbfind` | Reply to media → instant vault lookup. Alone → arm 60s, then forward the media. |
| `/dbdel` | Reply to media → delete its vault record. Alone → arm 60s, then forward the media. |

---

## 🔐 Safety Rules — read before uploading to GitHub

**Short answer: yes, this is safe to publish — *after* you strip your secrets.**
The code itself contains no credentials; the risk is entirely in what you accidentally commit
alongside it.

### ❌ Never commit these

| File | Why |
|---|---|
| `config.py` | Contains **real bot tokens** + your `ADMIN_ID`. A leaked token = full control of your bot by anyone. |
| `media.db`, `media.db-wal`, `media.db-shm` | Your private media fingerprint history. |
| `vault_bot.log` | Runtime log — may contain chat ids and error context. |
| `.venv/`, `__pycache__/` | Junk. |

### ✅ Do this instead

1. Ship **`config.example.py`** with placeholder values and add `config.py` to `.gitignore`.
   (Both files are included in this repo — copy the example to `config.py` and fill it in.)
2. Add the provided **`.gitignore`** *before* your first commit.
3. If a token was **ever** pushed, even once, even in a deleted commit:
   → open BotFather → `/revoke` → generate a new token. Git history is forever; assume it's burned.
4. Keep `ADMIN_ID` out of the repo. It's not catastrophic, but it's your account id.
5. Don't publish sample screenshots that show tokens, chat ids, or private media.
6. Optional but recommended: enable **GitHub Secret Scanning / Push Protection** on the repo.

### ⚖️ Usage rules

- This is a **personal automation tool** for chats/channels **you own or admin**.
- The bot only obeys `ADMIN_ID` — do not remove `admin_filter` to "share" it.
- Respect Telegram's [Terms of Service](https://telegram.org/tos) and Bot API rate limits.
  Do not use it to mass-mirror or redistribute content you don't have the rights to.
- Deleting is irreversible. `/dbdel` removes a vault record; the media is *not* re-downloadable
  by the bot afterwards. Back up `media.db` before bulk experiments.

### 🩺 Pre-push checklist

```bash
git status --porcelain          # config.py / *.db / *.log must NOT be listed
grep -rIn "[0-9]\{8,10\}:AA" .  # should match ONLY config.example.py placeholders
```

---

## 💻 Setup — Windows / VS Code / PowerShell

> No `git clone` required — click **Code → Download ZIP** on GitHub and extract it,
> or use VS Code: `File → Open Folder…` on the extracted folder.

```powershell
# 1) Go into the extracted folder
cd "$HOME\Downloads\telegram-media-vault"

# 2) (If Python is missing) install it once, then reopen PowerShell
winget install -e --id Python.Python.3.12

# 3) Create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1
# If blocked: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# 4) Install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# 5) Create your private config
Copy-Item config.example.py config.py
notepad config.py          # paste your BotFather tokens + your ADMIN_ID

# 6) Run
python bot.py
```

Stop with **Ctrl+C** — the DB closes cleanly.

**VS Code tip:** press `Ctrl+Shift+P → Python: Select Interpreter → .venv`, then just hit ▶ Run.

---

## 📱 Setup — Termux (Android)

```bash
# 1) Update Termux and install Python
pkg update -y && pkg upgrade -y
pkg install -y python

# 2) Allow access to your phone storage (one-time, accept the popup)
termux-setup-storage

# 3) Unzip the downloaded folder (e.g. saved in Downloads)
pkg install -y unzip
cd ~/storage/downloads
unzip telegram-media-vault.zip -d ~/vaultbot
cd ~/vaultbot/telegram-media-vault

# 4) Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 5) Create your private config
cp config.example.py config.py
nano config.py        # paste tokens + ADMIN_ID, then Ctrl+O, Enter, Ctrl+X

# 6) Run
python bot.py
```

**Keep it alive in the background**

```bash
pkg install -y termux-services tmux
termux-wake-lock            # stop Android from sleeping the process

tmux new -s vault           # start a session
python bot.py
# detach: Ctrl+B then D      • reattach: tmux attach -t vault
```

Also disable battery optimisation for Termux in Android settings, otherwise polling dies.

---

## ⚙️ Configuration reference

`config.py` (copy from `config.example.py`):

| Key | Default | Meaning |
|---|---|---|
| `BOT_TOKENS` | *(list)* | One or more BotFather tokens. **Order matters** — the first is "Bot 1", the announcer. |
| `ADMIN_ID` | *(int)* | Your numeric Telegram user id (get it from `@userinfobot`). |
| `DB_NAME` | `media.db` | SQLite vault path. |
| `ALBUM_BATCH_DELAY` | `3.5` | Seconds to wait before flushing a collected album/queue. |
| `QUEUE_COOLDOWN` | `2.0` | Pause between sending album chunks (flood protection). |
| `MAX_DELETE_CHUNK` | `100` | Max messages deleted per batch. |
| `MAX_SEEN_MEDIA` | `5000` | RAM-cache size for no-DB mode. |
| `DEFAULT_SETTINGS.autodelete` | `True` | Delete originals. |
| `DEFAULT_SETTINGS.custom_caption` | `None` | Caption applied to reposts. |
| `DEFAULT_SETTINGS.auto_group` | `True` | Smart grouping on start. |
| `DEFAULT_SETTINGS.db_check` | `True` | Duplicate blocking on start. |

In `bot.py`: `GP_ALBUM_SKIP_MIN = 6` (albums this size or larger pass through),
`INSPECT_TIMEOUT = 60.0`, `INSPECT_GRACE = 5.0`.

**Tuning tips:** slow network → raise `ALBUM_BATCH_DELAY` to `5.0`.
Hitting flood waits → raise `QUEUE_COOLDOWN` to `3.0`+.

---

## 📂 Files created at runtime

```
media.db            SQLite vault  (+ .db-wal / .db-shm while running)
vault_bot.log       rotating-free plain log, UTF-8
```

Table `media_vault`: `file_hash` (PK) · `bot_name` · `created_at` · `duplicate_count` · `last_duplicate_at`

---

## 🧯 Troubleshooting

| Symptom | Fix |
|---|---|
| `Unauthorized` on start | Token wrong/revoked → new token from BotFather. |
| Bot ignores you | `ADMIN_ID` mismatch. Check with `@userinfobot`. |
| `Conflict: terminated by other getUpdates` | The same token is running twice (another terminal, or duplicated in `BOT_TOKENS`). |
| `database is locked` | Another process has `media.db` open. Close DB Browser / second instance. |
| Frequent `FloodWait` in log | Increase `QUEUE_COOLDOWN`, decrease bot count. |
| Emoji crash on Windows | Already handled; if it persists run `chcp 65001` first. |
| Termux stops when screen off | `termux-wake-lock` + disable battery optimisation. |

---

## 💡 Ideas / roadmap

- `.env` support via `python-dotenv` instead of a Python config file.
- Perceptual hashing (pHash) to catch *re-encoded* duplicates, not just identical files.
- `/dbexport` → CSV dump of the vault.
- Auto-rotating log files (`RotatingFileHandler`).
- Optional destination chat id, so media is re-posted to a different channel.
- Systemd / `termux-services` unit files for autostart.

---

## ⚠️ Disclaimer

Provided as-is, for personal use. You are responsible for your own bot tokens, your data, and for
complying with Telegram's Terms of Service and any applicable law. The authors accept no liability
for lost media, deleted messages, or banned bots.

---

## 📄 License

MIT — see `LICENSE`.
