import re
import os
import asyncio
import logging
import aiofiles
import sqlite3
from datetime import datetime, timedelta
from urllib.parse import urlparse
from hashlib import sha256
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram import Client, filters
from pyrogram.errors import (
    UserAlreadyParticipant,
    InviteHashExpired,
    InviteHashInvalid,
    PeerIdInvalid,
    InviteRequestSent,
    FloodWait
)
from config import (
    API_ID,
    API_HASH,
    BOT_TOKEN,
    SESSION_STRING,
    ADMIN_LIMIT,
    ADMIN_IDS,
    DEFAULT_LIMIT
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Clients
app = Client("app_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=1000)
user = Client("user_session", session_string=SESSION_STRING, workers=1000)

# Database Setup for Persistent Storage
DB_NAME = "cc_scraper.db"

def init_db():
    """Initialize SQLite database to store seen cards and last message IDs."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # Table for storing unique card hashes (prevents duplicates across all time)
    c.execute('''CREATE TABLE IF NOT EXISTS seen_cards
                 (card_hash TEXT PRIMARY KEY, source TEXT, first_seen TIMESTAMP)''')
    # Table for last scraped message ID per channel (for incremental scraping)
    c.execute('''CREATE TABLE IF NOT EXISTS last_scraped
                 (channel_id INTEGER PRIMARY KEY, last_msg_id INTEGER, last_scrape TIMESTAMP)''')
    conn.commit()
    conn.close()
    logger.info("Database initialized.")

def is_card_seen(card_number, source):
    """Check if a card number (hashed) is already in the database for any source."""
    card_hash = sha256(card_number.encode()).hexdigest()
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT 1 FROM seen_cards WHERE card_hash = ?", (card_hash,))
    exists = c.fetchone() is not None
    conn.close()
    return exists, card_hash

def mark_card_seen(card_hash, source):
    """Add a new card hash to the database."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO seen_cards (card_hash, source, first_seen) VALUES (?, ?, ?)",
                  (card_hash, source, datetime.utcnow()))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # Already exists
    finally:
        conn.close()

def get_last_msg_id(channel_id):
    """Retrieve the last scraped message ID for a channel."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT last_msg_id FROM last_scraped WHERE channel_id = ?", (channel_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def update_last_msg_id(channel_id, last_msg_id):
    """Update the last scraped message ID for a channel."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO last_scraped (channel_id, last_msg_id, last_scrape)
                 VALUES (?, ?, ?)''', (channel_id, last_msg_id, datetime.utcnow()))
    conn.commit()
    conn.close()

async def scrape_new_messages(client, channel_username, limit, start_number=None, bank_name=None):
    """Scrape ONLY NEW credit cards from a channel, starting after the last seen message."""
    messages_found = []
    count = 0
    pattern = r'\d{16}\D*\d{2}\D*\d{2,4}\D*\d{3,4}'
    bin_pattern = re.compile(r'^\d{6}') if start_number else None

    try:
        chat = await user.get_chat(channel_username)
        channel_id = chat.id
        channel_title = chat.title
    except Exception as e:
        logger.error(f"Failed to get chat {channel_username}: {e}")
        return [], "Invalid Channel", 0

    last_msg_id = get_last_msg_id(channel_id)
    logger.info(f"Scraping channel: {channel_title} (ID: {channel_id}). Last known msg ID: {last_msg_id}")

    # We will collect the newest message ID from this scrape
    newest_msg_id_in_this_run = last_msg_id

    try:
        # Iterate through messages. If last_msg_id is known, we use offset_id to start after it.
        async for message in user.search_messages(chat_id=channel_id, query="", limit=limit*5):  # Fetch more to filter
            if count >= limit:
                break

            # Update the newest message ID we've encountered
            if newest_msg_id_in_this_run is None or message.id > newest_msg_id_in_this_run:
                newest_msg_id_in_this_run = message.id

            # If we have a last_msg_id and this message is older or equal, skip it (we've seen it)
            if last_msg_id and message.id <= last_msg_id:
                continue

            text = message.text or message.caption
            if not text:
                continue

            # Bank name filter
            if bank_name and bank_name.lower() not in text.lower():
                continue

            # Find all card patterns in the message
            matched_strings = re.findall(pattern, text)
            for matched in matched_strings:
                extracted = re.findall(r'\d+', matched)
                if len(extracted) == 4:
                    card_number, mo, year, cvv = extracted
                    year = year[-2:]

                    # BIN filter
                    if start_number and not card_number.startswith(start_number[:6]):
                        continue

                    # Check if card is NEW (not in database)
                    seen, card_hash = is_card_seen(card_number, channel_title)
                    if not seen:
                        formatted = f"{card_number}|{mo}|{year}|{cvv}"
                        messages_found.append(formatted)
                        mark_card_seen(card_hash, channel_title)
                        count += 1
                        logger.debug(f"New card found: {card_number}")
                    else:
                        logger.debug(f"Duplicate card skipped: {card_number}")

    except Exception as e:
        logger.error(f"Error during scraping of {channel_title}: {e}")

    # Update the last message ID for the channel to the newest we saw
    if newest_msg_id_in_this_run and newest_msg_id_in_this_run != last_msg_id:
        update_last_msg_id(channel_id, newest_msg_id_in_this_run)
        logger.info(f"Updated last_msg_id for {channel_title} to {newest_msg_id_in_this_run}")

    logger.info(f"Scraped {len(messages_found)} NEW cards from {channel_title}")
    return messages_found, channel_title, newest_msg_id_in_this_run or last_msg_id

async def send_results(client, message, unique_messages, source_name, last_msg_id, bin_filter=None, bank_filter=None):
    """Send the results file."""
    if unique_messages:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"NEW_x{len(unique_messages)}_{source_name.replace(' ', '_')}_{timestamp}.txt"
        async with aiofiles.open(file_name, 'w') as f:
            await f.write("\n".join(unique_messages))

        async with aiofiles.open(file_name, 'rb') as f:
            user_link = await get_user_link(message)
            caption = (
                f"<b>🆕 ADVANCED CC SCRAPER ✅</b>\n"
                f"<b>━━━━━━━━━━━━━━━━</b>\n"
                f"<b>🔰 Source:</b> <code>{source_name}</code>\n"
                f"<b>📈 New Cards:</b> <code>{len(unique_messages)}</code>\n"
                f"<b>📌 Last Msg ID:</b> <code>{last_msg_id}</code>\n"
            )
            if bin_filter:
                caption += f"<b>🔢 BIN Filter:</b> <code>{bin_filter}</code>\n"
            if bank_filter:
                caption += f"<b>🏦 Bank Filter:</b> <code>{bank_filter}</code>\n"
            caption += (
                f"<b>━━━━━━━━━━━━━━━━</b>\n"
                f"<b>✅ Cards are GUARANTEED NEW (not in database)</b>\n"
                f"<b>👤 Scrapped By: {user_link}</b>\n"
            )
            await message.delete()
            await client.send_document(message.chat.id, f, caption=caption, file_name=file_name)
        os.remove(file_name)
    else:
        await message.edit_text("<b>❌ No NEW Credit Cards Found Since Last Scrape.</b>")

async def get_user_link(message):
    """Generate a user mention link."""
    if not message.from_user:
        return '<a href="https://t.me/ItsSmartToolBot">Smart Tool</a>'
    name = f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip()
    return f'<a href="tg://user?id={message.from_user.id}">{name}</a>'

async def join_private_chat(client, invite_link):
    """Join a private channel via invite link."""
    try:
        await client.join_chat(invite_link)
        return True
    except UserAlreadyParticipant:
        return True
    except (InviteHashExpired, InviteHashInvalid, InviteRequestSent):
        return False

# COMMAND HANDLERS
def setup_handlers(app):
    @app.on_message(filters.command(["advscr", "newscr"], prefixes=["/", ".", ",", "!"]) & (filters.group | filters.private))
    async def adv_scr_cmd(client, message):
        args = message.text.split()[1:]
        if len(args) < 2:
            await message.reply_text("<b>⚠️ Format: /advscr channel limit [BIN or BankName]</b>")
            return

        user_id = message.from_user.id if message.from_user else None
        channel_identifier = args[0]

        try:
            limit = int(args[1])
            max_lim = ADMIN_LIMIT if user_id in ADMIN_IDS else DEFAULT_LIMIT
            if limit > max_lim:
                await message.reply_text(f"<b>Limit exceeded. Max: {max_lim}</b>")
                return
        except ValueError:
            await message.reply_text("<b>Invalid limit.</b>")
            return

        start_number = None
        bank_name = None
        if len(args) > 2:
            if args[2].isdigit():
                start_number = args[2]
            else:
                bank_name = " ".join(args[2:])

        temp_msg = await message.reply_text("<b>🔍 Scanning for NEW cards...</b>")
        new_cards, source_name, last_msg_id = await scrape_new_messages(user, channel_identifier, limit, start_number, bank_name)

        bin_filter = start_number[:6] if start_number else None
        await send_results(client, temp_msg, new_cards, source_name, last_msg_id, bin_filter, bank_name)

    @app.on_message(filters.command("scr_stats", prefixes=["/", ".", ",", "!"]) & (filters.group | filters.private))
    async def stats_cmd(client, message):
        """Show database statistics."""
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM seen_cards")
        total_cards = c.fetchone()[0]
        c.execute("SELECT COUNT(DISTINCT source) FROM seen_cards")
        total_sources = c.fetchone()[0]
        conn.close()
        await message.reply_text(
            f"<b>📊 Scraper Database Stats</b>\n"
            f"<b>Total Unique Cards:</b> {total_cards}\n"
            f"<b>Tracked Sources:</b> {total_sources}\n"
            f"<b>Database File:</b> <code>{DB_NAME}</code>"
        )

    @app.on_message(filters.command("scr_clear", prefixes=["/", ".", ",", "!"]) & filters.user(ADMIN_IDS))
    async def clear_db_cmd(client, message):
        """(Admin) Clear database for a fresh start."""
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("DELETE FROM seen_cards")
        c.execute("DELETE FROM last_scraped")
        conn.commit()
        conn.close()
        await message.reply_text("<b>✅ Database cleared. All cards will be considered NEW next scrape.</b>")

    @app.on_message(filters.command("start"))
    async def start(client, message):
        buttons = [
            [InlineKeyboardButton("Update Channel", url="https://t.me/Modvip_rm"),
             InlineKeyboardButton("My Dev👨‍💻", user_id=7303810912)]
        ]
        start_text = """
<b>🆕 Advanced CC Scraper Bot</b>

I scrape ONLY NEW credit cards that haven't been seen before.

<code>/advscr @channel 100</code> - Scrape new cards from a channel.
<code>/advscr @channel 100 515462</code> - With BIN filter.
<code>/advscr @channel 100 BankName</code> - With bank filter.
<code>/scr_stats</code> - Show database stats.
<code>/scr_clear</code> - Clear history (Admin).

I track last message IDs and card hashes to ensure no duplicates.
        """
        await client.send_message(message.chat.id, start_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

# MAIN
if __name__ == "__main__":
    init_db()  # Initialize database before starting
    setup_handlers(app)
    logger.info("Starting Advanced Scraper Bot...")
    user.start()
    app.run()