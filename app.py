"""
app.py - Telegram Deal Reposting Bot (ready-to-paste)

Features:
- Two output channels, separate source lists.
- Repost media first (if present), then formatted caption with affiliate links.
- Expand short URLs, enforce Amazon affiliate tag, EarnKaro optional.
- Per-channel dedupe (canonical URL + text hash) with DEDUPE_SECONDS.
- Skip pending/stale messages after restart (START_TIME).
- Night pause (NIGHT_PAUSE_ENABLED, NIGHT_START_HOUR, NIGHT_END_HOUR) using TIMEZONE.
- Rate limit + jitter (MIN_INTERVAL_SECONDS, MIN_JITTER, MAX_JITTER).
- Render API redeploy when idle > 30 minutes with backoff and daily cap.
- Flask health endpoints.
- Supports STRING_SESSION or BOT_TOKEN login.
"""

import os
import re
import io
import time
import json
import math
import random
import logging
import atexit
import asyncio
import hashlib
import requests
import threading
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import aiohttp
from flask import Flask, jsonify, request

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

# Optional Pillow for promo overlays
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False

# ---------------- Load env ---------------- #
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=dotenv_path)

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

CHANNEL_ID = os.getenv("CHANNEL_ID")  # required: destination channel 1 (username or ID)
CHANNEL_ID_2 = os.getenv("CHANNEL_ID_2", "")  # optional second channel

USE_CHANNEL_1 = os.getenv("USE_CHANNEL_1", "true").lower() == "true"
USE_CHANNEL_2 = os.getenv("USE_CHANNEL_2", "true").lower() == "true"

# Source lists: comma-separated telegram chat IDs or usernames
SOURCE_IDS_CHANNEL_1 = [s.strip() for s in os.getenv("SOURCE_IDS_CHANNEL_1", "").split(",") if s.strip()]
SOURCE_IDS_CHANNEL_2 = [s.strip() for s in os.getenv("SOURCE_IDS_CHANNEL_2", "").split(",") if s.strip()]

AFFILIATE_TAG = os.getenv("AFFILIATE_TAG", "lootfastdeals-21")
USE_EARNKARO = os.getenv("USE_EARNKARO", "false").lower() == "true"
EARNKARO_API_KEY = os.getenv("EARNKARO_API_KEY", "").strip()

DEDUPE_SECONDS = int(os.getenv("DEDUPE_SECONDS", "3600"))
MAX_MSG_LEN = int(os.getenv("MAX_MSG_LEN", "700"))
PREVIEW_LEN = int(os.getenv("PREVIEW_LEN", "500"))

# Rate limiting settings
MIN_INTERVAL_SECONDS = int(os.getenv("MIN_INTERVAL_SECONDS", "60"))
MIN_JITTER = int(os.getenv("MIN_JITTER", "3"))
MAX_JITTER = int(os.getenv("MAX_JITTER", "12"))

# Night pause
NIGHT_PAUSE_ENABLED = os.getenv("NIGHT_PAUSE_ENABLED", "true").lower() == "true"
NIGHT_START_HOUR = int(os.getenv("NIGHT_START_HOUR", "2"))
NIGHT_END_HOUR = int(os.getenv("NIGHT_END_HOUR", "6"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Calcutta")  # India default

# Admin notifications
ADMIN_NOTIFY = os.getenv("ADMIN_NOTIFY", "").strip()  # e.g. @kr_cool
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]

# Render redeploy options
RENDER_API_KEY = os.getenv("RENDER_API_KEY", "").strip()
RENDER_SERVICE_ID = os.getenv("RENDER_SERVICE_ID", "").strip()
RENDER_DEPLOY_HOOK = os.getenv("RENDER_DEPLOY_HOOK", "").strip()

# Keep-alive external URL to ping (your Render service)
EXTERNAL_URL = os.getenv("EXTERNAL_URL", "").strip()

# Misc
PORT = int(os.getenv("PORT", "10000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_REDEPLOYS_PER_DAY = int(os.getenv("MAX_REDEPLOYS_PER_DAY", "3"))
REDEPLOY_BACKOFF_BASE = int(os.getenv("REDEPLOY_BACKOFF_BASE", "60"))
MAX_MESSAGE_AGE_SECONDS = int(os.getenv("MAX_MESSAGE_AGE_SECONDS", "120"))  # skip messages older than this after start

# Hashtags sets
HASHTAG_SETS = [
    "#LootDeals #Discount #OnlineShopping",
    "#Free #Offer #Sale",
    "#TopDeals #BigSale #BestPrice",
    "#PriceDrop #FlashSale #DealAlert",
]

# Setup logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("dealbot")

# ---------------- Runtime state ---------------- #
seen_urls = set()
seen_channel_1 = {}
seen_channel_2 = {}
last_msg_time = time.time()
last_sent_channel = {}
redeploy_count_today = 0
last_redeploy_time = 0

# Startup marker: used to skip pending backlog messages posted while bot was down
START_TIME = time.time()

# Create Telethon client
if STRING_SESSION:
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    logger.info("Using STRING_SESSION auth")
else:
    # Use a file session name, but if BOT_TOKEN provided we'll start as bot later
    client = TelegramClient("session", API_ID, API_HASH)
    if BOT_TOKEN:
        logger.info("Bot token provided (will start as bot)")

app = Flask(__name__)

# ---------------- Helpers ---------------- #
def choose_hashtags():
    return random.choice(HASHTAG_SETS)

async def notify_admins(message: str):
    """Send message to admin username or admin numeric IDs."""
    try:
        targets = []
        if ADMIN_IDS:
            targets.extend([str(x) for x in ADMIN_IDS])
        if ADMIN_NOTIFY:
            targets.append(ADMIN_NOTIFY)
        # de-duplicate and send
        for t in set(targets):
            try:
                name = t.lstrip("@")
                await client.send_message(name, message)
                logger.info(f"Admin notified: {t}")
            except Exception as e:
                logger.warning(f"Failed to notify admin {t}: {e}")
    except Exception as e:
        logger.exception(f"notify_admins error: {e}")

SHORT_PATTERNS = [
    r"(https?://fkrt\.cc/\S+)", r"(https?://myntr\.it/\S+)",
    r"(https?://dl\.flipkart\.com/\S+)", r"(https?://ajio\.me/\S+)",
    r"(https?://amzn\.to/\S+)", r"(https?://amzn\.in/\S+)",
    r"(https?://bit\.ly/\S+)", r"(https?://tinyurl\.com/\S+)",
    r"(https?://fktt\.co/\S+)", r"(https?://bitly\.cx/\S+)",
    r"(https?://fkt\.co/\S+)"
]

async def expand_all_async(text: str) -> str:
    """Async expansion of short URLs using aiohttp (follow redirects)."""
    urls = sum((re.findall(p, text) for p in SHORT_PATTERNS), [])
    if not urls:
        return text
    timeout = aiohttp.ClientTimeout(total=6)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        for u in urls:
            try:
                async with s.head(u, allow_redirects=True, timeout=5) as r:
                    expanded = str(r.url)
                    text = text.replace(u, expanded)
                    logger.debug(f"Expanded {u} -> {expanded}")
            except Exception as e:
                logger.debug(f"URL expand failed for {u}: {e}")
    return text

def convert_amazon(text: str) -> str:
    """Ensure Amazon links include single affiliate tag (avoid duplicates)."""
    # Replace existing tag param with ours
    text = re.sub(r'([?&])tag=[^&\s]+', r'\1tag=' + AFFILIATE_TAG, text, flags=re.I)
    # For amazon dp/gp links without tag add one
    pat = r'(https?://(?:www\.)?amazon\.(?:in|com)/(?:.*?/)?(?:dp|gp/product)/([A-Z0-9]{10}))(?:[^\s]*)?'
    def repl(m):
        url = m.group(1)
        asin = m.group(2)
        if '?' in url:
            return f"{url}&tag={AFFILIATE_TAG}"
        else:
            return f"https://www.amazon.in/dp/{asin}/?tag={AFFILIATE_TAG}"
    text = re.sub(pat, repl, text, flags=re.I)
    return text

async def convert_earnkaro_async(text: str) -> str:
    """Optional EarnKaro conversion for Flipkart/Myntra/Ajio links (async)."""
    if not USE_EARNKARO:
        return text
    urls = re.findall(r"(https?://\S+)", text)
    if not urls:
        return text
    timeout = aiohttp.ClientTimeout(total=6)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        for u in urls:
            if any(x in u for x in ["flipkart", "myntra", "ajio"]):
                try:
                    # EarnKaro endpoint - optional; adapt if their API requires auth differently
                    async with s.post("https://api.earnkaro.com/api/deeplink", json={"url": u}, timeout=6) as r:
                        if r.status == 200:
                            js = await r.json()
                            ek = js.get("data", {}).get("link")
                            if ek:
                                text = text.replace(u, ek)
                                logger.debug(f"EarnKaro converted {u} -> {ek}")
                except Exception as e:
                    logger.debug(f"EarnKaro conversion error for {u}: {e}")
    return text

async def process_text_pipeline(text: str) -> str:
    t = await expand_all_async(text)
    t = convert_amazon(t)
    t = await convert_earnkaro_async(t)
    return t

def extract_product_name(text: str) -> Optional[str]:
    text_no_urls = re.sub(r'https?://\S+', '', text)
    patterns = [
        r"(?:Samsung|iPhone|OnePlus|Realme|Xiaomi|Redmi|Poco|Motorola|Nokia|LG|Sony|HP|Dell|Lenovo|Asus|Acer|Boat|JBL|Noise|Mi)\s+[^@\n]+?(?=@|₹|http|$)",
        r"[A-Z][a-z]+(?:\s+[A-Za-z0-9]+)+?(?:\s+\d+(?:cm|inch|GB|TB|MB|mAh|MP|Hz))+(?=@|₹|http|$)",
        r"([A-Za-z][^@\n]{10,}?(?=@|₹|http|\n|$))",
    ]
    for p in patterns:
        m = re.search(p, text_no_urls, re.IGNORECASE)
        if m:
            prod = m.group(0).strip()
            if len(prod) > 10:
                return prod
    return None

def canonicalize(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r'amazon\.(?:com|in)/(?:.*?/)?(?:dp|gp/product)/([A-Z0-9]{10})', url, flags=re.I)
    if m:
        return f"amazon:{m.group(1)}"
    if "flipkart.com" in url:
        pid_match = re.search(r'/p/([a-zA-Z0-9]+)', url) or re.search(r'/itm/([a-zA-Z0-9]+)', url)
        if pid_match:
            return f"flipkart:{pid_match.group(1)}"
    for dom in ["myntra.com", "ajio.com"]:
        if dom in url:
            path = url.split("?")[0].rstrip("/")
            return dom + ":" + path.split("/")[-1] if "/" in path else path
    return None

def hash_text(msg: str) -> str:
    product_name = extract_product_name(msg)
    if product_name:
        clean = re.sub(r"\s+", " ", product_name.lower())
        clean = re.sub(r"[^\w\s]", "", clean)
        if len(clean) > 15:
            return hashlib.md5(clean.encode()).hexdigest()
    clean = re.sub(r"\s+", " ", msg.lower())
    urls = re.findall(r'https?://\S+', clean)
    url_part = "".join(urls)
    text_part = clean[:100]
    combined = url_part + text_part
    return hashlib.md5(combined.encode()).hexdigest()

def truncate_message(msg: str) -> str:
    if len(msg) <= MAX_MSG_LEN:
        return msg
    urls = re.findall(r"https?://\S+", msg)
    more_link = urls[0] if urls else ""
    return msg[:PREVIEW_LEN] + "...\n👉 More: " + more_link

async def fetch_image_bytes(url: str, timeout_sec: int = 8) -> Optional[bytes]:
    """Fetch image bytes from URL (async)."""
    if not url:
        return None
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        try:
            async with s.get(url, timeout=timeout) as r:
                if r.status == 200:
                    ct = r.headers.get("Content-Type", "")
                    if "image" in ct:
                        return await r.read()
                    else:
                        return await r.read()  # might be image anyway
        except Exception as e:
            logger.debug(f"fetch_image_bytes failed for {url}: {e}")
    return None

async def get_og_image_from_page(url: str, timeout_sec: int = 6) -> Optional[str]:
    """Fetch page and extract og:image or twitter:image or first img src."""
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, timeout=timeout) as r:
                if r.status != 200:
                    return None
                text = await r.text()
                m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', text, flags=re.I)
                if m:
                    return m.group(1)
                m2 = re.search(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']', text, flags=re.I)
                if m2:
                    return m2.group(1)
                m3 = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', text, flags=re.I)
                if m3:
                    return m3.group(1)
    except Exception as e:
        logger.debug(f"get_og_image_from_page error: {e}")
    return None

async def extract_image_from_msg_obj(msg_obj) -> Optional[bytes]:
    """Priority: 1) Telegram media -> download, 2) OG image via first product URL."""
    # 1) Download media if present
    try:
        if getattr(msg_obj, "media", None):
            b = io.BytesIO()
            try:
                await client.download_media(msg_obj.media, file=b)
                b.seek(0)
                data = b.read()
                if data and len(data) > 1000:
                    return data
            except Exception as e:
                logger.debug(f"download_media failed: {e}")
    except Exception as e:
        logger.debug(f"extract_image_from_msg media check error: {e}")

    # 2) OG fallback from first matching URL
    try:
        text = msg_obj.message or ""
        urls = re.findall(r"(https?://\S+)", text)
        for u in urls:
            if any(k in u for k in ["amazon", "amzn.to", "flipkart", "myntra", "ajio", "fkrt", "bit.ly"]):
                img_url = await get_og_image_from_page(u)
                if img_url:
                    if img_url.startswith("//"):
                        img_url = "https:" + img_url
                    img_bytes = await fetch_image_bytes(img_url)
                    if img_bytes:
                        return img_bytes
    except Exception as e:
        logger.debug(f"OG fallback error: {e}")

    return None

def build_promotional_image(product_bytes: bytes, badge_text: str = "🔥 Hot Deal", price_text: Optional[str] = None) -> bytes:
    """Optional Pillow overlay; returns bytes. If Pillow unavailable, returns original bytes."""
    if not PIL_AVAILABLE:
        return product_bytes
    try:
        im = Image.open(io.BytesIO(product_bytes)).convert("RGBA")
        max_width = 900
        if im.width > max_width:
            ratio = max_width / im.width
            im = im.resize((max_width, int(im.height * ratio)), Image.LANCZOS)
        draw = ImageDraw.Draw(im)
        try:
            font = ImageFont.truetype("DejaVuSans-Bold.ttf", 28)
        except Exception:
            font = ImageFont.load_default()
        draw.rectangle([(10, 10), (230, 10 + 40)], fill=(255, 69, 0, 220))
        draw.text((18, 14), badge_text, font=font, fill="white")
        if price_text:
            h = im.height
            draw.rectangle([(0, h - 44), (im.width, h)], fill=(0, 0, 0, 200))
            draw.text((12, h - 36), price_text, font=font, fill="white")
        out = io.BytesIO()
        im.save(out, format="PNG")
        out.seek(0)
        return out.read()
    except Exception as e:
        logger.debug(f"build_promotional_image error: {e}")
        return product_bytes

async def send_to_channel_with_media_and_caption(target_channel, msg_obj, processed_text, label, channel_name, seen_dict):
    """Send media if available, then caption (with rate-limit)."""
    global last_msg_time
    # Rate limiting per channel
    now_ts = time.time()
    last = last_sent_channel.get(channel_name, 0)
    need_wait = MIN_INTERVAL_SECONDS + random.randint(MIN_JITTER, MAX_JITTER)
    if (now_ts - last) < need_wait:
        logger.info(f"Rate limit: skipping post to {channel_name}, need {int(need_wait - (now_ts - last))}s more")
        return False

    # Try to obtain image bytes
    image_bytes = await extract_image_from_msg_obj(msg_obj)

    # Clean/ensure affiliate tag for first url in processed_text
    cleaned = re.sub(r'([?&])tag=[^&\s]+', '', processed_text, flags=re.I)
    urls = re.findall(r"(https?://\S+)", cleaned)
    if urls:
        first = urls[0]
        # ensure single tag for amazon
        if "amazon." in first:
            if '?' in first:
                first = first + "&tag=" + AFFILIATE_TAG
            else:
                first = first + "/?tag=" + AFFILIATE_TAG
            cleaned = cleaned.replace(urls[0], first, 1)

    caption = label + truncate_message(cleaned) + f"\n\n{choose_hashtags()}"

    # If we have image bytes, send file first
    try:
        if image_bytes:
            try:
                promo = build_promotional_image(image_bytes, badge_text="🔥 Deal", price_text=None)
                bio = io.BytesIO(promo)
                bio.name = "deal.png"
                await client.send_file(target_channel, file=bio, caption=caption, link_preview=False)
                logger.info(f"Sent image + caption to {channel_name}")
            except Exception as e:
                logger.debug(f"send_file failed: {e}; falling back to text send")
                await client.send_message(target_channel, caption, link_preview=False)
        else:
            await client.send_message(target_channel, caption, link_preview=False)
        last_msg_time = time.time()
        last_sent_channel[channel_name] = time.time()
        return True
    except Exception as ex:
        logger.exception(f"Error sending to {channel_name}: {ex}")
        return False

# ---------------- Main Processing ---------------- #
async def process_and_send(raw_text: str, msg_obj, target_channel, channel_name, seen_dict):
    """
    Full pipeline: validate, process text, dedupe, label, send (media+caption).
    msg_obj required to access media and timestamp.
    """
    if not raw_text and not getattr(msg_obj, "media", None):
        return False

    # Basic sanity: ignore messages that are too short and have no URLs
    urls_in_raw = re.findall(r"https?://\S+", raw_text or "")
    if (not raw_text or len(raw_text.strip()) < 10) and not urls_in_raw and not getattr(msg_obj, "media", None):
        logger.info(f"[{channel_name}] Skipping: too short & no URLs & no media")
        return False

    try:
        processed = await process_text_pipeline(raw_text or "")
        urls = re.findall(r"https?://\S+", processed)
        if not urls and not getattr(msg_obj, "media", None):
            logger.info(f"[{channel_name}] Skipping: no URLs and no media after processing")
            return False

        now = time.time()
        dedupe_keys = []

        # canonical URL dedupe
        for u in urls:
            c = canonicalize(u)
            if c:
                last_seen = seen_dict.get(c)
                if not last_seen or (now - last_seen) > DEDUPE_SECONDS:
                    dedupe_keys.append(c)
                    logger.debug(f"[{channel_name}] URL dedupe key: {c}")
                else:
                    logger.info(f"[{channel_name}] Duplicate URL skipped: {c}")

        # text hash dedupe
        text_key = hash_text(processed)
        last_seen = seen_dict.get(text_key)
        if not last_seen or (now - last_seen) > DEDUPE_SECONDS:
            dedupe_keys.append(text_key)
            logger.debug(f"[{channel_name}] Text dedupe key: {text_key}")
        else:
            logger.info(f"[{channel_name}] Duplicate text skipped: {text_key}")

        # If both URL and text were duplicates -> skip
        if not dedupe_keys:
            logger.info(f"[{channel_name}] Skipped: dedupe triggered both URL & text")
            return False

        # Save dedupe keys for channel
        for k in dedupe_keys:
            seen_dict[k] = now
        for u in urls:
            seen_urls.add(u)

        # Choose platform label
        label = ""
        if any("amazon" in u for u in urls):
            label = "🔥 Amazon Deal:\n"
        elif any("flipkart" in u for u in urls):
            label = "⚡ Flipkart Deal:\n"
        elif any("myntra" in u for u in urls):
            label = "✨ Myntra Deal:\n"
        elif any("ajio" in u for u in urls):
            label = "🛍️ Ajio Deal:\n"
        else:
            label = "🎯 Fast Deal:\n"

        # Send media + caption
        success = await send_to_channel_with_media_and_caption(target_channel, msg_obj, processed, label, channel_name, seen_dict)
        return success

    except Exception as ex:
        logger.exception(f"[{channel_name}] Error in process_and_send: {ex}")
        # notify admin for non-conflict errors
        if "two different IP addresses" not in str(ex):
            await notify_admins(f"❌ Error processing message for {channel_name}: {ex}")
        return False

# ---------------- Bot main & event handlers ---------------- #
async def bot_main():
    global START_TIME, last_msg_time

    # Start client (user string session or bot token)
    try:
        if STRING_SESSION:
            await client.start()
        elif BOT_TOKEN:
            await client.start(bot_token=BOT_TOKEN)
        else:
            # start normally (file session)
            await client.start()
        me = await client.get_me()
        logger.info(f"Logged in as: {getattr(me, 'first_name', '')} (id:{getattr(me, 'id', '')})")
        # reset START_TIME here to avoid processing backlog
        START_TIME = time.time()
        logger.info(f"START_TIME set to {START_TIME}")
    except Exception as e:
        logger.exception(f"Session start failed: {e}")
        await notify_admins(f"CRITICAL: Session start failed: {e}")
        return

    # Notify admin once
    try:
        await notify_admins("🤖 Bot started successfully and monitoring sources.")
    except Exception:
        pass

    # Resolve source entities to IDs (best-effort)
    sources_ch1 = []
    sources_ch2 = []
    if USE_CHANNEL_1:
        for s in SOURCE_IDS_CHANNEL_1:
            try:
                ent = await client.get_entity(s)
                sources_ch1.append(ent.id)
                logger.info(f"Connected source (ch1): {getattr(ent, 'title', s)} ({s})")
            except Exception as e:
                logger.warning(f"Failed to get entity for source {s}: {e}")
    if USE_CHANNEL_2 and CHANNEL_ID_2:
        for s in SOURCE_IDS_CHANNEL_2:
            try:
                ent = await client.get_entity(s)
                sources_ch2.append(ent.id)
                logger.info(f"Connected source (ch2): {getattr(ent, 'title', s)} ({s})")
            except Exception as e:
                logger.warning(f"Failed to get entity for source {s}: {e}")

    logger.info(f"Channel config: CH1={CHANNEL_ID} enabled={USE_CHANNEL_1} sources={len(sources_ch1)}")
    logger.info(f"Channel config: CH2={CHANNEL_ID_2} enabled={USE_CHANNEL_2} sources={len(sources_ch2)}")

    # Event handlers
    if USE_CHANNEL_1 and sources_ch1:
        @client.on(events.NewMessage(chats=sources_ch1))
        async def handler_ch1(ev):
            msg = ev.message
            # skip pending: message older than START_TIME
            try:
                msg_ts = getattr(msg.date, "timestamp", lambda: time.time())()
            except Exception:
                msg_ts = time.time()
            if msg_ts < (START_TIME - 1):
                logger.debug("Skipped pending message (ch1)")
                return
            # skip stale messages older than MAX_MESSAGE_AGE_SECONDS
            if time.time() - msg_ts > MAX_MESSAGE_AGE_SECONDS:
                logger.debug("Skipped stale message (ch1)")
                return
            # night pause
            if NIGHT_PAUSE_ENABLED:
                try:
                    from zoneinfo import ZoneInfo
                    now_local = datetime.now(ZoneInfo(TIMEZONE))
                    h = now_local.hour
                    if NIGHT_START_HOUR < NIGHT_END_HOUR:
                        if NIGHT_START_HOUR <= h < NIGHT_END_HOUR:
                            logger.debug("Night pause active (ch1) -> skip")
                            return
                    else:
                        if h >= NIGHT_START_HOUR or h < NIGHT_END_HOUR:
                            logger.debug("Night pause active (ch1) -> skip")
                            return
                except Exception as e:
                    logger.debug(f"ZoneInfo error: {e}")
            await process_and_send(msg.message or "", msg, CHANNEL_ID, "Channel 1", seen_channel_1)

    if USE_CHANNEL_2 and sources_ch2:
        @client.on(events.NewMessage(chats=sources_ch2))
        async def handler_ch2(ev):
            msg = ev.message
            try:
                msg_ts = getattr(msg.date, "timestamp", lambda: time.time())()
            except Exception:
                msg_ts = time.time()
            if msg_ts < (START_TIME - 1):
                logger.debug("Skipped pending message (ch2)")
                return
            if time.time() - msg_ts > MAX_MESSAGE_AGE_SECONDS:
                logger.debug("Skipped stale message (ch2)")
                return
            if NIGHT_PAUSE_ENABLED:
                try:
                    from zoneinfo import ZoneInfo
                    now_local = datetime.now(ZoneInfo(TIMEZONE))
                    h = now_local.hour
                    if NIGHT_START_HOUR < NIGHT_END_HOUR:
                        if NIGHT_START_HOUR <= h < NIGHT_END_HOUR:
                            logger.debug("Night pause active (ch2) -> skip")
                            return
                    else:
                        if h >= NIGHT_START_HOUR or h < NIGHT_END_HOUR:
                            logger.debug("Night pause active (ch2) -> skip")
                            return
                except Exception as e:
                    logger.debug(f"ZoneInfo error: {e}")
            await process_and_send(msg.message or "", msg, CHANNEL_ID_2, "Channel 2", seen_channel_2)

    logger.info("Bot is monitoring sources...")
    await client.run_until_disconnected()

# ---------------- Redeploy & Monitor ---------------- #
def redeploy_via_hook():
    if not RENDER_DEPLOY_HOOK:
        return False
    try:
        requests.post(RENDER_DEPLOY_HOOK, timeout=10)
        logger.info("Redeploy hook fired")
        return True
    except Exception as e:
        logger.warning(f"Redeploy hook failed: {e}")
        return False

async def trigger_render_deploy_async():
    if not RENDER_API_KEY or not RENDER_SERVICE_ID:
        logger.debug("Render API not configured")
        return False
    url = f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/deploys"
    headers = {"Authorization": f"Bearer {RENDER_API_KEY}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=headers, json={}, timeout=15) as r:
                text = await r.text()
                if r.status in (200, 201):
                    logger.info("Render API deploy triggered")
                    return True
                else:
                    logger.warning(f"Render API deploy failed {r.status}: {text}")
                    return False
    except Exception as e:
        logger.warning(f"Render deploy error: {e}")
        return False

def monitor_health_background():
    """Runs in thread; triggers redeploy when idle too long with backoff/daily cap."""
    global redeploy_count_today, last_redeploy_time
    while True:
        time.sleep(300)  # every 5 minutes
        idle = time.time() - last_msg_time
        if idle > 1800:  # 30 minutes
            now = time.time()
            if redeploy_count_today >= MAX_REDEPLOYS_PER_DAY:
                logger.info("Max redeploys today reached")
                continue
            wait = REDEPLOY_BACKOFF_BASE * (2 ** redeploy_count_today)
            if (now - last_redeploy_time) < wait:
                logger.debug(f"Waiting backoff: {int(wait - (now - last_redeploy_time))}s left")
                continue
            logger.warning("Idle >30min -> attempting redeploy")
            triggered = False
            if RENDER_API_KEY and RENDER_SERVICE_ID:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    triggered = loop.run_until_complete(trigger_render_deploy_async())
                    loop.close()
                except Exception as e:
                    logger.warning(f"Async render deploy error: {e}")
            if not triggered and RENDER_DEPLOY_HOOK:
                triggered = redeploy_via_hook()
            if triggered:
                redeploy_count_today += 1
                last_redeploy_time = time.time()

# ---------------- Keep-alive ping loop ---------------- #
def keep_alive_loop():
    while True:
        time.sleep(300)
        try:
            requests.get(f"http://127.0.0.1:{PORT}/ping", timeout=5)
            logger.debug("Internal ping ok")
        except Exception as e:
            logger.debug(f"Internal ping failed: {e}")
        if EXTERNAL_URL:
            try:
                r = requests.get(f"{EXTERNAL_URL}/ping", timeout=8)
                logger.debug(f"External ping {r.status_code}")
            except Exception as e:
                logger.debug(f"External ping failed: {e}")

# ---------------- Flask endpoints ---------------- #
@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "channel_1": CHANNEL_ID,
        "channel_2": CHANNEL_ID_2,
        "use_channel_1": USE_CHANNEL_1,
        "use_channel_2": USE_CHANNEL_2,
        "sources_ch1": len(SOURCE_IDS_CHANNEL_1),
        "sources_ch2": len(SOURCE_IDS_CHANNEL_2)
    })

@app.route("/ping")
def ping():
    return "pong"

@app.route("/health")
def health():
    return jsonify({
        "time_since_last_message": int(time.time() - last_msg_time),
        "unique_links_seen": len(seen_urls),
        "status": "healthy" if (time.time() - last_msg_time) < 3600 else "idle"
    })

@app.route("/stats")
def stats():
    return jsonify({
        "last_message_time": last_msg_time,
        "unique_links": len(seen_urls),
        "dedupe_ch1": len(seen_channel_1),
        "dedupe_ch2": len(seen_channel_2)
    })

@app.route("/redeploy", methods=["POST"])
def redeploy_endpoint():
    triggered = False
    try:
        if RENDER_API_KEY and RENDER_SERVICE_ID:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            triggered = loop.run_until_complete(trigger_render_deploy_async())
            loop.close()
        if not triggered and RENDER_DEPLOY_HOOK:
            triggered = redeploy_via_hook()
    except Exception as e:
        logger.warning(f"redeploy endpoint error: {e}")
    return ("ok", 200) if triggered else ("fail", 500)

# ---------------- Entrypoint ---------------- #
def start_bot_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(bot_main())

if __name__ == "__main__":
    # Start Telethon bot in background thread
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_bot_loop, args=(loop,), daemon=True)
    t.start()

    # Start keepalive + monitor threads
    Thread_keep = threading.Thread(target=keep_alive_loop, daemon=True)
    Thread_keep.start()
    Thread_monitor = threading.Thread(target=monitor_health_background, daemon=True)
    Thread_monitor.start()

    # Start Flask
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False, threaded=True)
