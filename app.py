# app.py - FASTDEALS Bot (final, ready-to-paste)
# - ASIN validator + fallback (verify short link; fallback to original with tag if invalid)
# - Night pause removed
# - Keeps multi-channel sources, dedupe, EarnKaro, media fallback, render ping, etc.
import os
import re
import time
import io
import hashlib
import random
import asyncio
import atexit
import requests
from threading import Thread
from flask import Flask, jsonify, request
import aiohttp
from collections import Counter
from urllib.parse import urlparse, parse_qs, unquote

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from dotenv import load_dotenv

# Optional Pillow for overlays; NOT required (we will NOT use overlay in send flow)
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False

# ---------------- Load env ---------------- #
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=dotenv_path)

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "session")

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or 0)
CHANNEL_ID_2 = int(os.getenv("CHANNEL_ID_2", "0") or 0)

# Channel control
USE_CHANNEL_1 = os.getenv("USE_CHANNEL_1", "true").lower() == "true"
USE_CHANNEL_2 = os.getenv("USE_CHANNEL_2", "true").lower() == "true"

AFFILIATE_TAG = os.getenv("AFFILIATE_TAG", "lootfastdeals-21")
USE_EARNKARO = os.getenv("USE_EARNKARO", "false").lower() == "true"

DEDUPE_SECONDS = int(os.getenv("DEDUPE_SECONDS", "3600"))
MAX_MSG_LEN = int(os.getenv("MAX_MSG_LEN", "700"))
PREVIEW_LEN = int(os.getenv("PREVIEW_LEN", "500"))

ADMIN_NOTIFY = os.getenv("ADMIN_NOTIFY", "").strip()  # e.g. @kr_cool
RENDER_DEPLOY_HOOK = os.getenv("RENDER_DEPLOY_HOOK", "").strip()

# Render API credentials (you provided these values)
RENDER_API_KEY = os.getenv("RENDER_API_KEY", "").strip()
RENDER_SERVICE_ID = os.getenv("RENDER_SERVICE_ID", "").strip()

PORT = int(os.getenv("PORT", "10000"))
EXTERNAL_URL = os.getenv("EXTERNAL_URL", "").strip()

# Rate limiting & backoff
MIN_INTERVAL_SECONDS = int(os.getenv("MIN_INTERVAL_SECONDS", "60"))  # default 60s
# Per-channel overrides (optional)
MIN_INTERVAL_SECONDS_CHANNEL_1 = int(os.getenv("MIN_INTERVAL_SECONDS_CHANNEL_1", str(MIN_INTERVAL_SECONDS)))
MIN_INTERVAL_SECONDS_CHANNEL_2 = int(os.getenv("MIN_INTERVAL_SECONDS_CHANNEL_2", str(MIN_INTERVAL_SECONDS)))

MIN_JITTER = int(os.getenv("MIN_JITTER", "3"))
MAX_JITTER = int(os.getenv("MAX_JITTER", "12"))
MAX_REDEPLOYS_PER_DAY = int(os.getenv("MAX_REDEPLOYS_PER_DAY", "3"))
REDEPLOY_BACKOFF_BASE = int(os.getenv("REDEPLOY_BACKOFF_BASE", "60"))

# Admin notify cooldown (seconds)
_admin_notify_cache = {}
_ADMIN_NOTIFY_COOLDOWN = int(os.getenv("ADMIN_NOTIFY_COOLDOWN", "600"))  # default 10 minutes

# ---------------- DIFFERENT SOURCES ---------------- #
SOURCE_IDS_CHANNEL_1 = [
    -1001448358487,  # Yaha Everything
    -1001767957702,  # Transparent Deals
    -1001387180060,  # Crazy Offers Deals - COD
    -1001378801949   # UNIVERSAL DEALS
]

SOURCE_IDS_CHANNEL_2 = [
    -1001505338947,  # Online Dealz Broadcast
    -1001561964907,  # 2.0 LCBD Loot Deals
    -1001450755585,  # Trending Loot Deals
    -1001820593092,  # Steadfast Deal
    -1001351555431   # LOOT लो!! Deals Offers
]

SHORT_PATTERNS = [
    r"(https?://fkrt\.cc/\S+)", r"(https?://myntr\.it/\S+)",
    r"(https?://dl\.flipkart\.com/\S+)", r"(https?://ajio\.me/\S+)",
    r"(https?://amzn\.to/\S+)", r"(https?://amzn\.in/\S+)",
    r"(https?://bit\.ly/\S+)", r"(https?://tinyurl\.com/\S+)",
    r"(https?://fktt\.co/\S+)", r"(https?://bitly\.cx/\S+)",
    r"(https?://fkt\.co/\S+)"
]

HASHTAG_SETS = [
    "#LootDeals #Discount #OnlineShopping",
    "#FAST #Offer #Sale",
    "#TopDeals #BigSale #BestPrice",
    "#PriceDrop #FlashSale #DealAlert",
    "#Amazondeals #Promo #Shoppingree",
    "#ShopSmart #SaleAlert #clusiveDeal",
    "#TrendingOffers #SaveMoreHotOffer",
    "#DesiDeals #BestBuy #Discount",
]

# ---------------- Runtime in-memory state (no Redis) ---------------- #
seen_urls = set()
seen_products = {}
seen_channel_1 = {}
seen_channel_2 = {}
last_msg_time = time.time()
last_sent_channel = {}   # channel_name -> timestamp
redeploy_count_today = 0
last_redeploy_time = 0

# ---------------- Session handling ---------------- #
if STRING_SESSION:
    print("Using STRING_SESSION from env")
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
else:
    print(f"Using file session: {SESSION_NAME}.session")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

app = Flask(__name__)

# ---------------- Graceful shutdown ---------------- #
def cleanup():
    try:
        if client.is_connected():
            client.disconnect()
            print("✅ Session properly closed")
    except Exception:
        pass

atexit.register(cleanup)

# ---------------- Helpers ---------------- #
async def notify_admin(message, error_key=None):
    """Send notification to admin (via user session) with cooldown.
    error_key is normalized by hashing so similar messages collapse to same cooldown bucket.
    """
    if not ADMIN_NOTIFY:
        return
    now = time.time()
    if error_key:
        # normalize error_key to a fixed-size hash to avoid near-unique keys
        try:
            key_norm = hashlib.sha1(error_key.encode('utf-8')).hexdigest()[:24]
        except Exception:
            key_norm = str(error_key)[:24]
        last = _admin_notify_cache.get(key_norm, 0)
        if (now - last) < _ADMIN_NOTIFY_COOLDOWN:
            print("ℹ️ Admin notify suppressed by cooldown for key:", key_norm)
            return
        _admin_notify_cache[key_norm] = now
    try:
        target = ADMIN_NOTIFY[1:] if ADMIN_NOTIFY.startswith("@") else ADMIN_NOTIFY
        await client.send_message(target, message)
        print(f"📢 Admin notified: {message}")
    except Exception as e:
        print(f"⚠️ Admin notify failed: {e}")

async def expand_all(text):
    """Expand short URLs like fkrt.cc, amzn.to etc (async).
    - Try HEAD first; if blocked (405) or fails, try GET.
    - Strip trailing punctuation from matched short URL.
    """
    # find matches for all short patterns (strip trailing punctuation)
    found = []
    for p in SHORT_PATTERNS:
        for m in re.findall(p, text):
            u = m.rstrip(').,;:]}')
            found.append(u)
    urls = list(dict.fromkeys(found))
    if not urls:
        return text
    timeout = aiohttp.ClientTimeout(total=7)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        for u in urls:
            try:
                expanded_url = None
                try:
                    async with s.head(u, allow_redirects=True) as r:
                        expanded_url = str(r.url)
                except Exception:
                    # HEAD failed for this shortener; try GET
                    try:
                        async with s.get(u, allow_redirects=True) as r2:
                            expanded_url = str(r2.url)
                    except Exception as e2:
                        print(f"⚠️ GET expansion failed for {u}: {e2}")
                        expanded_url = None
                if expanded_url and expanded_url != u:
                    # replace occurrences of the short url in the original text
                    text = text.replace(u, expanded_url)
                    print(f"🔗 Expanded {u} → {expanded_url}")
            except Exception as e:
                print(f"⚠️ Expansion failed for {u}: {e}")
    return text

# ---------------- Improved Amazon canonicalizer (async with verification) ---------------- #
def _find_asins_in_string(s):
    """Return list of all 10-char alnum sequences that look like ASINs (uppercased)."""
    tokens = re.findall(r'([A-Z0-9]{10})', s, flags=re.I)
    return [t.upper() for t in tokens]

def _first_asin_from_qs(qs):
    for k in ("asin", "ASIN"):
        if k in qs and qs[k]:
            val = qs[k][0]
            if re.match(r'^[A-Z0-9]{10}$', val, flags=re.I):
                return val.upper()
    # look for asinlist/asins-like keys
    for k, v in qs.items():
        if "asin" in k.lower() and v:
            first = re.split(r'[|,]', v[0])[0]
            if re.match(r'^[A-Z0-9]{10}$', first, flags=re.I):
                return first.upper()
    return None

async def convert_amazon_async(text):
    """
    Async version: Replace Amazon/store URLs with short affiliate product links.
    - Extract ASINs from many patterns.
    - If multiple ASINs exist, choose the most frequent ASIN.
    - Build short link https://www.amazon.in/dp/<ASIN>/?tag=<AFFILIATE_TAG>.
    - Verify short link by requesting it; if verification fails, fall back to original URL with tag appended.
    - Does NOT append 'More (full list)' — single short link only (or fallback original+tag).
    """
    if not text:
        return text

    # First replace existing tag=... occurrences globally to our tag (quick)
    try:
        text = re.sub(r'([?&])tag=[^&\s&]+', r'\1tag=' + AFFILIATE_TAG, text)
    except Exception:
        pass

    # Find amazon-like URLs
    urls = re.findall(r'https?://[^\s)]+amazon\.[a-z\.]{2,6}[^\s)\]\}]*', text, flags=re.I)
    if not urls:
        return text

    new_text = text
    timeout = aiohttp.ClientTimeout(total=6)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for u in sorted(set(urls), key=lambda x: -len(x)):  # longer first to avoid partial matches
            orig_u = u
            try:
                # decode double-encoded artifacts
                try:
                    u_dec = unquote(unquote(u))
                except Exception:
                    try:
                        u_dec = unquote(u)
                    except Exception:
                        u_dec = u

                # Collect ASIN candidates
                asins = []

                # pattern /dp/ASIN or /gp/product/ASIN
                m = re.findall(r'/dp/([A-Z0-9]{10})', u_dec, flags=re.I)
                if m:
                    asins.extend([x.upper() for x in m])
                m2 = re.findall(r'/gp/(?:product|aw/d)/([A-Z0-9]{10})', u_dec, flags=re.I)
                if m2:
                    asins.extend([x.upper() for x in m2])

                # query string asin or asin list
                try:
                    qs = parse_qs(urlparse(u_dec).query)
                    q_asin = _first_asin_from_qs(qs)
                    if q_asin:
                        asins.append(q_asin)
                except Exception:
                    pass

                # fallback: any 10-char tokens
                fallback = _find_asins_in_string(u_dec)
                if fallback:
                    asins.extend(fallback)

                # sanitize order & choose ASIN
                chosen_asin = None
                if asins:
                    # pick the most common ASIN among candidates
                    count = Counter(asins)
                    chosen_asin = count.most_common(1)[0][0]

                if chosen_asin and re.match(r'^[A-Z0-9]{10}$', chosen_asin, flags=re.I):
                    short = f"https://www.amazon.in/dp/{chosen_asin}/?tag={AFFILIATE_TAG}"

                    # verify short link (GET with redirects) — if it looks valid, use it
                    try:
                        async with session.get(short, allow_redirects=True) as resp:
                            status = resp.status
                            # Accept 2xx or 3xx (redirects) as valid; otherwise consider invalid
                            if 200 <= status < 400:
                                replacement = short
                                print(f"🔁 Amazon converted and verified: {orig_u[:80]} -> {replacement} (status={status})")
                            else:
                                # fallback to original URL with tag
                                replaced = re.sub(r'([?&])tag=[^&\s]+', r'\1tag=' + AFFILIATE_TAG, orig_u)
                                if 'tag=' not in replaced:
                                    if '?' in replaced:
                                        replaced = replaced + "&tag=" + AFFILIATE_TAG
                                    else:
                                        replaced = replaced + "?tag=" + AFFILIATE_TAG
                                replacement = replaced
                                print(f"⚠️ Short link verification failed (status={status}) - falling back to original+tag for {orig_u[:80]}")
                    except Exception as e:
                        # network or blocked — fallback to original with tag
                        replaced = re.sub(r'([?&])tag=[^&\s]+', r'\1tag=' + AFFILIATE_TAG, orig_u)
                        if 'tag=' not in replaced:
                            if '?' in replaced:
                                replaced = replaced + "&tag=" + AFFILIATE_TAG
                            else:
                                replaced = replaced + "?tag=" + AFFILIATE_TAG
                        replacement = replaced
                        print(f"⚠️ Short link verification exception - falling back to original+tag for {orig_u[:80]}: {e}")
                else:
                    # no valid ASIN found: ensure tag param present (append if missing)
                    replaced = re.sub(r'([?&])tag=[^&\s]+', r'\1tag=' + AFFILIATE_TAG, orig_u)
                    if 'tag=' not in replaced:
                        if '?' in replaced:
                            replaced = replaced + "&tag=" + AFFILIATE_TAG
                        else:
                            replaced = replaced + "?tag=" + AFFILIATE_TAG
                    replacement = replaced
                    print(f"ℹ️ Amazon: no valid ASIN; using original+tag for {orig_u[:80]}")
                # finally replace in new_text
                new_text = new_text.replace(orig_u, replacement)
            except Exception as e:
                print("⚠️ convert_amazon_async error for", u, e)
    return new_text

# ---------------- EarnKaro conversion (kept intact) ---------------- #
async def convert_earnkaro(text):
    """Async EarnKaro conversion (non-blocking). Fallbacks to original URL on failure."""
    if not USE_EARNKARO:
        return text
    urls = re.findall(r"(https?://\S+)", text)
    if not urls:
        return text
    timeout = aiohttp.ClientTimeout(total=6)
    headers = {"Content-Type": "application/json"}
    async with aiohttp.ClientSession(timeout=timeout) as s:
        for u in urls:
            if any(k in u for k in ("flipkart", "myntra", "ajio")):
                try:
                    async with s.post("https://api.earnkaro.com/api/deeplink", json={"url": u}, headers=headers) as resp:
                        if resp.status == 200:
                            js = await resp.json()
                            ek = js.get("data", {}).get("link")
                            if ek:
                                text = text.replace(u, ek)
                                print(f"🔁 EarnKaro converted for {u}")
                        else:
                            print(f"⚠️ EarnKaro returned {resp.status} for {u}")
                except Exception as e:
                    print(f"⚠️ EarnKaro failed for {u}: {e}")
    return text

async def process(text):
    # expand shorteners first
    t = await expand_all(text)
    # convert amazon and verify (async)
    t = await convert_amazon_async(t)
    # earnkaro conversion (if enabled)
    t = await convert_earnkaro(t)
    return t

# ---------------- Product name / canonical / hash helpers ---------------- #
def extract_product_name(text):
    text_no_urls = re.sub(r'https?://\S+', '', text)
    patterns = [
        r"(?:Samsung|iPhone|OnePlus|Realme|Xiaomi|Redmi|Poco|Motorola|Nokia|LG|Sony|HP|Dell|Lenovo|Asus|Acer|MSI|Canon|Nikon|Boat|JBL|Noise|Fire-Boltt|pTron|Mi|Pepe\s+Jeans|Lee\s+Cooper|Fitspire)\s+[^@\n]+?(?=@|₹|http|$)",
        r"[A-Z][a-z]+(?:\s+[A-Za-z0-9]+)+?(?:\s+\d+(?:cm|inch|GB|TB|MB|mAh|MP|Hz))+(?=@|₹|http|$)",
        r"Upto\s+\d+%+\s+Off\s+On\s+([^@\n]+?)(?=@|₹|http|$)",
        r"Flat\s+\d+%+\s+Off\s+On\s+([^@\n]+?)(?=@|₹|http|$)",
        r"([A-Za-z][^@\n]{10,}?(?=@|₹|http|\n|$))",
    ]
    for p in patterns:
        m = re.search(p, text_no_urls, re.IGNORECASE)
        if m:
            prod = m.group(0).strip()
            prod = re.sub(r'^(Upto|Flat)\s+\d+%\s+Off\s+On\s+', '', prod, flags=re.IGNORECASE)
            if len(prod) > 10:
                return prod
    return None

def canonicalize(url):
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

def hash_text(msg):
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

def truncate_message(msg):
    if len(msg) <= MAX_MSG_LEN:
        return msg
    urls = re.findall(r"https?://\S+", msg)
    more_link = urls[0] if urls else ""
    return msg[:PREVIEW_LEN] + "...\n👉 More: " + more_link

def choose_hashtags():
    return random.choice(HASHTAG_SETS)

# ---------------- Image helpers (we send original bytes; overlay not used) ----------------
async def get_og_image_from_page(url: str, timeout_sec: int = 6):
    """Try to extract OG-image or first img src from page (async)."""
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
    except Exception:
        return None
    return None

async def fetch_image_bytes(url: str, timeout_sec: int = 8):
    """Fetch image bytes via aiohttp (async)."""
    if not url:
        return None
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, timeout=timeout) as r:
                if r.status == 200:
                    data = await r.read()
                    if data and len(data) > 1000:
                        return data
    except Exception:
        return None
    return None

async def extract_image_from_msg_obj(msg_obj):
    """
    1) If msg_obj has Telegram media, download it.
    2) Else, try OG-image from first URL in message text.
    Returns bytes or None.
    """
    try:
        if getattr(msg_obj, "media", None):
            b = io.BytesIO()
            try:
                await client.download_media(msg_obj.media, file=b)
                b.seek(0)
                data = b.read()
                if data and len(data) > 1000:
                    return data
            except Exception:
                pass
    except Exception:
        pass

    # fallback: detect first URL and scrape OG image
    try:
        text = msg_obj.message or ""
        urls = re.findall(r"(https?://\S+)", text)
        for u in urls:
            if any(k in u for k in ["amazon", "flipkart", "myntra", "ajio", "fkrt", "bit.ly", "amzn.to"]):
                img_url = await get_og_image_from_page(u)
                if img_url:
                    if img_url.startswith("//"):
                        img_url = "https:" + img_url
                    img_bytes = await fetch_image_bytes(img_url)
                    if img_bytes:
                        return img_bytes
    except Exception:
        pass

    return None

# keep build_promotional_image function for compatibility but we will NOT call it.
def build_promotional_image(product_bytes: bytes, badge_text: str = "🔥 Deal", price_text: str = None):
    """
    Optional: overlay badge and price using Pillow if available.
    If Pillow not installed, returns original bytes unchanged.
    """
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
        draw.rectangle([(10, 10), (240, 10 + 40)], fill=(255, 69, 0, 220))
        draw.text((18, 14), badge_text, font=font, fill="white")
        if price_text:
            h = im.height
            draw.rectangle([(0, h - 44), (im.width, h)], fill=(0, 0, 0, 200))
            draw.text((12, h - 36), price_text, font=font, fill="white")
        out = io.BytesIO()
        im.save(out, format="PNG")
        out.seek(0)
        return out.read()
    except Exception:
        return product_bytes

# ---------------- Updated: send_to_specific_channel now truncates caption & reduces admin spam ----------------
def _extract_first_url(text):
    r = re.search(r'(https?://\S+)', text)
    return r.group(1) if r else ""

async def send_to_specific_channel(message, channel_id, channel_name, msg_obj=None):
    """
    If msg_obj provided and contains image (Telegram media or OG), the bot will attempt to send image+caption.
    Falls back to send_message when no media or if send_file fails.
    Caption is truncated to avoid SendMediaRequest 'caption too long' errors.
    On truncated caption, the full message is sent as a follow-up text message (to preserve content).
    """
    try:
        if msg_obj is not None:
            try:
                image_bytes = await extract_image_from_msg_obj(msg_obj)
                if image_bytes:
                    # send original image bytes without overlay (no Pillow processing)
                    bio = io.BytesIO(image_bytes)
                    bio.name = "deal.png"

                    # ensure caption length within safe limits for send_file
                    CAPTION_SAFE_LIMIT = 800  # safe margin (Telegram's limit ~1024)
                    caption = message
                    if len(caption) > CAPTION_SAFE_LIMIT:
                        # truncate caption but keep a link to full in follow-up text
                        first_url = _extract_first_url(message) or ""
                        caption_short = caption[:CAPTION_SAFE_LIMIT - 20].rstrip() + "..."
                        # add small suffix so user can click More
                        if first_url:
                            caption_short += f"\n👉 More: {first_url}"
                        try:
                            await client.send_file(channel_id, file=bio, caption=caption_short, link_preview=False)
                            print(f"✅ Sent image+short-caption to {channel_name} ({channel_id})")
                        except Exception as e:
                            # log and notify admin with cooldown
                            print(f"⚠️ Image send failed for {channel_name}: {e}")
                            await notify_admin(f"⚠️ Image send failed for {channel_name}: {e}", error_key="image_send_fail")
                            # fallback to text-only send
                            await client.send_message(channel_id, message, link_preview=False)
                            print(f"✅ Fallback: Sent text-only to {channel_name} ({channel_id})")
                            return True

                        # send the full message as a follow-up text so nothing is lost
                        try:
                            await asyncio.sleep(0.6)
                            await client.send_message(channel_id, message, link_preview=False)
                        except Exception as e2:
                            print(f"⚠️ Follow-up full-text send failed for {channel_name}: {e2}")
                            await notify_admin(f"⚠️ Follow-up send failed for {channel_name}: {e2}", error_key="followup_send_fail")
                        return True
                    else:
                        # caption safe
                        try:
                            await client.send_file(channel_id, file=bio, caption=caption, link_preview=False)
                            print(f"✅ Sent image+caption to {channel_name} ({channel_id})")
                            return True
                        except Exception as e:
                            print(f"⚠️ Image send failed, falling back to text for {channel_name}: {e}")
                            await notify_admin(f"⚠️ Image send failed for {channel_name}: {e}", error_key="image_send_fail")
                            # fallback to text send
                            await client.send_message(channel_id, message, link_preview=False)
                            return True
            except Exception as e:
                # if media send fails, fall back to text send and notify admin once
                print(f"⚠️ Image processing failed for {channel_name}: {e}")
                await notify_admin(f"⚠️ Image processing failed for {channel_name}: {e}", error_key="image_process_fail")

        # Default: send text message
        await client.send_message(channel_id, message, link_preview=False)
        print(f"✅ Sent to {channel_name} ({channel_id})")
        return True
    except Exception as ex:
        print(f"❌ Telegram error for {channel_name} ({channel_id}): {ex}")
        # avoid spamming admin for repeated same errors
        await notify_admin(f"❌ {channel_name} error ({channel_id}): {ex}", error_key=str(ex)[:120])
        return False

# ---------------- Updated: process_and_send (keeps all behavior, adds msg_obj pass) ----------------
async def process_and_send(raw_txt, target_channel, channel_name, seen_dict, msg_obj=None):
    if not raw_txt and not getattr(msg_obj, "media", None):
        return False

    print(f"📨 [{channel_name}] Raw message: {(raw_txt or '')[:120]}...")

    urls_in_raw = re.findall(r"https?://\S+", raw_txt or "")
    if (not raw_txt or len(raw_txt.strip()) < 20) and not urls_in_raw and not getattr(msg_obj, "media", None):
        print(f"⚠️ [{channel_name}] Skipped: Message too short and no URLs and no media")
        return False

    try:
        processed = await process(raw_txt or "")
        urls = re.findall(r"https?://\S+", processed)

        # If no URLs and no media, skip
        if not urls and not getattr(msg_obj, "media", None):
            print(f"⚠️ [{channel_name}] Skipped: No valid URLs found after processing and no media")
            return False

        now = time.time()
        dedupe_keys = []

        # dedupe by canonical product keys
        for u in urls:
            c = canonicalize(u)
            if c:
                last_seen = seen_dict.get(c)
                if not last_seen or (now - last_seen) > DEDUPE_SECONDS:
                    dedupe_keys.append(c)
                    print(f"🔗 [{channel_name}] URL dedupe key: {c}")
                else:
                    print(f"⚠️ [{channel_name}] Duplicate URL skipped (seen {int(now - last_seen)}s ago): {c}")

        # dedupe by text hash
        text_key = hash_text(processed)
        last_seen = seen_dict.get(text_key)
        if not last_seen or (now - last_seen) > DEDUPE_SECONDS:
            dedupe_keys.append(text_key)
            print(f"📝 [{channel_name}] Text dedupe key: {text_key}")
        else:
            print(f"⚠️ [{channel_name}] Duplicate text skipped (seen {int(now - last_seen)}s ago): {text_key}")

        # Only skip if BOTH URL and text are duplicates
        if not dedupe_keys:
            print(f"⚠️ [{channel_name}] Skipped: All dedupe keys are duplicates")
            return False

        # Update seen for this channel
        for k in dedupe_keys:
            seen_dict[k] = now
        for u in urls:
            seen_urls.add(u)

        # labels
        label = ""
        all_urls = urls
        if any("amazon" in u for u in all_urls):
            label = "🔥 Amazon Deal:\n"
        elif any("flipkart" in u for u in all_urls):
            label = "⚡ Flipkart Deal:\n"
        elif any("myntra" in u for u in all_urls):
            label = "✨ Myntra Deal:\n"
        elif any("ajio" in u for u in all_urls):
            label = "🛍️ Ajio Deal:\n"
        else:
            label = "🎯 Fast Deal:\n"

        msg = label + truncate_message(processed)
        msg += f"\n\n{choose_hashtags()}"

        print(f"📤 [{channel_name}] Prepared message: {msg[:100]}...")

        # --- RATE LIMIT PER CHANNEL (avoid bursts / spam flags) ---
        now_ts = time.time()
        last = last_sent_channel.get(channel_name, 0)
        # choose base interval per channel
        if channel_name.lower().startswith("channel 1"):
            base_interval = MIN_INTERVAL_SECONDS_CHANNEL_1
        elif channel_name.lower().startswith("channel 2"):
            base_interval = MIN_INTERVAL_SECONDS_CHANNEL_2
        else:
            base_interval = MIN_INTERVAL_SECONDS
        need_wait = base_interval + random.randint(MIN_JITTER, MAX_JITTER)
        if (now_ts - last) < need_wait:
            print(f"⏱️ Rate limit: skip {channel_name}. Need {int(need_wait - (now_ts - last))}s more.")
            return False

        # small jitter before sending (stagger across channels)
        await asyncio.sleep(random.uniform(0.5, 2.5))

        # Pass msg_obj so send function can attempt media repost.  This function handles caption limit.
        success = await send_to_specific_channel(msg, target_channel, channel_name, msg_obj=msg_obj)

        if success:
            global last_msg_time
            last_msg_time = time.time()
            last_sent_channel[channel_name] = time.time()
            print(f"✅ [{channel_name}] Processed at {time.strftime('%H:%M:%S')}")
            return True
        else:
            print(f"❌ [{channel_name}] Failed to send")
            return False

    except Exception as ex:
        error_msg = f"❌ [{channel_name}] Error processing message: {str(ex)}"
        print(error_msg)
        if "two different IP addresses" not in str(ex):
            await notify_admin(error_msg, error_key="processing_error")
        return False

# ---------------- Bot main (same flow, preserves all source handling) ----------------
async def bot_main():
    global last_msg_time, seen_urls, seen_products, seen_channel_1, seen_channel_2

    try:
        await client.start()
        me = await client.get_me()
        print(f"✅ Logged in as: {me.first_name} (ID: {me.id})")
        await client.get_me()
    except Exception as e:
        error_msg = f"❌ Session validation failed: {e}"
        print(error_msg)
        if "two different IP addresses" in str(e):
            await notify_admin("🚨 CRITICAL: Session conflict! Generate NEW STRING_SESSION", error_key="session_conflict")
        return

    await notify_admin("🤖 Bot started successfully! Monitoring different sources for each channel.", error_key="bot_started")

    # Connect to source entities for Channel 1
    sources_channel_1 = []
    if USE_CHANNEL_1:
        for i in SOURCE_IDS_CHANNEL_1:
            try:
                e = await client.get_entity(i)
                sources_channel_1.append(e.id)
                print(f"✅ [Channel 1] Connected source: {e.title} ({i})")
            except Exception as ex:
                print(f"❌ [Channel 1] Failed source {i}: {ex}")
                await notify_admin(f"❌ [Channel 1] Failed to connect to source {i}: {ex}", error_key=f"src1_{i}")

    # Connect to source entities for Channel 2
    sources_channel_2 = []
    if USE_CHANNEL_2 and CHANNEL_ID_2 and int(CHANNEL_ID_2) != 0:
        for i in SOURCE_IDS_CHANNEL_2:
            try:
                e = await client.get_entity(i)
                sources_channel_2.append(e.id)
                print(f"✅ [Channel 2] Connected source: {e.title} ({i})")
            except Exception as ex:
                print(f"❌ [Channel 2] Failed source {i}: {ex}")
                await notify_admin(f"❌ [Channel 2] Failed to connect to source {i}: {ex}", error_key=f"src2_{i}")

    print(f"🎯 Channel Configuration:")
    print(f"   Channel 1: {CHANNEL_ID} ({'ENABLED' if USE_CHANNEL_1 else 'DISABLED'}) - {len(sources_channel_1)} sources")
    print(f"   Channel 2: {CHANNEL_ID_2} ({'ENABLED' if USE_CHANNEL_2 else 'DISABLED'}) - {len(sources_channel_2)} sources")

    # Handler for Channel 1 sources
    if USE_CHANNEL_1 and sources_channel_1:
        @client.on(events.NewMessage(chats=sources_channel_1))
        async def handler_channel_1(e):
            # pass full message object so media helper can extract images
            await process_and_send(e.message.message or "", CHANNEL_ID, "Channel 1", seen_channel_1, msg_obj=e.message)

    # Handler for Channel 2 sources
    if USE_CHANNEL_2 and sources_channel_2:
        @client.on(events.NewMessage(chats=sources_channel_2))
        async def handler_channel_2(e):
            await process_and_send(e.message.message or "", int(CHANNEL_ID_2), "Channel 2", seen_channel_2, msg_obj=e.message)

    print("🔄 Bot is now actively monitoring different sources for each channel...")
    await client.run_until_disconnected()

# ---------------- Deploy helpers & monitor (unchanged) ----------------
def redeploy_via_hook():
    """Use old webhook hook if provided (synchronous)."""
    if not RENDER_DEPLOY_HOOK:
        return False
    try:
        requests.post(RENDER_DEPLOY_HOOK, timeout=10)
        print("✅ Redeploy hook fired")
        return True
    except Exception as e:
        print(f"⚠️ Redeploy hook failed: {e}")
        return False

async def trigger_render_deploy_async():
    """Trigger Render deploy via official API (async)."""
    api_key = RENDER_API_KEY
    service_id = RENDER_SERVICE_ID
    if not api_key or not service_id:
        print("ℹ️ Render API not configured")
        return False
    url = f"https://api.render.com/v1/services/{service_id}/deploys"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=headers, json={}, timeout=15) as r:
                text = await r.text()
                if r.status in (200, 201):
                    print("✅ Render API deploy triggered")
                    return True
                else:
                    print(f"❌ Render API deploy failed {r.status}: {text}")
                    return False
    except Exception as e:
        print(f"⚠️ Render deploy error: {e}")
        return False

def monitor_health():
    """Background monitor that triggers redeploy with exponential backoff."""
    global last_msg_time, redeploy_count_today, last_redeploy_time
    while True:
        time.sleep(300)  # check every 5 minutes
        idle = time.time() - last_msg_time
        if idle > 1800:
            now = time.time()
            if redeploy_count_today >= MAX_REDEPLOYS_PER_DAY:
                print("⚠️ Max redeploys reached for today, skipping redeploy")
                continue
            wait = REDEPLOY_BACKOFF_BASE * (2 ** redeploy_count_today)
            if (now - last_redeploy_time) < wait:
                print(f"ℹ️ Waiting backoff before deploy: {int(wait - (now - last_redeploy_time))}s left")
                continue
            print("⚠️ Idle 30+ min, attempting redeploy (Render API preferred)")
            # Prefer Render API if keys present, fallback to hook
            triggered = False
            if RENDER_API_KEY and RENDER_SERVICE_ID:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    triggered = loop.run_until_complete(trigger_render_deploy_async())
                    loop.close()
                except Exception as e:
                    print(f"⚠️ Async render deploy error: {e}")
            if not triggered and RENDER_DEPLOY_HOOK:
                triggered = redeploy_via_hook()
            if triggered:
                redeploy_count_today += 1
                last_redeploy_time = time.time()

# ---------------- Keep-alive ping loop ---------------- #
def keep_alive():
    while True:
        time.sleep(300)  # every 5 minutes
        try:
            requests.get(f"http://127.0.0.1:{PORT}/ping", timeout=5)
            print("✅ Internal ping successful")
        except Exception as e:
            print(f"⚠️ Internal ping failed: {e}")
        if EXTERNAL_URL:
            try:
                r = requests.get(f"{EXTERNAL_URL}/ping", timeout=10)
                print(f"✅ External ping successful: {r.status_code}")
            except Exception as e:
                print(f"⚠️ External ping failed: {e}")
        else:
            print("ℹ️ No EXTERNAL_URL set, skipping external ping")

# ---------------- Flask endpoints ---------------- #
@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "telegram_primary": CHANNEL_ID,
        "telegram_secondary": CHANNEL_ID_2,
        "channel_1_enabled": USE_CHANNEL_1,
        "channel_2_enabled": USE_CHANNEL_2,
        "channel_1_sources": len(SOURCE_IDS_CHANNEL_1),
        "channel_2_sources": len(SOURCE_IDS_CHANNEL_2),
    })

@app.route("/ping")
def ping():
    return "pong"

@app.route("/health")
def health():
    return jsonify({
        "time_since_last_message": int(time.time() - last_msg_time),
        "unique_links": len(seen_urls),
        "status": "healthy" if (time.time() - last_msg_time) < 3600 else "inactive"
    })

@app.route("/stats")
def stats():
    return jsonify({
        "unique_links": len(seen_urls),
        "last_message_time": last_msg_time,
        "telegram_channels": [CHANNEL_ID, CHANNEL_ID_2] if CHANNEL_ID_2 and int(CHANNEL_ID_2) != 0 else [CHANNEL_ID],
        "channel_1_enabled": USE_CHANNEL_1,
        "channel_2_enabled": USE_CHANNEL_2,
    })

@app.route("/redeploy", methods=["POST"])
def redeploy_endpoint():
    # safe synchronous redeploy that uses Render API if configured
    triggered = False
    if RENDER_API_KEY and RENDER_SERVICE_ID:
        try:
            # run async trigger
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            triggered = loop.run_until_complete(trigger_render_deploy_async())
            loop.close()
        except Exception as e:
            print(f"⚠️ Redeploy async error: {e}")
    if not triggered and RENDER_DEPLOY_HOOK:
        triggered = redeploy_via_hook()
    return ("ok", 200) if triggered else ("fail", 500)

# ---------------- Entrypoint ---------------- #
if __name__ == "__main__":
    # start bot
    loop = asyncio.new_event_loop()
    Thread(target=lambda: asyncio.set_event_loop(loop) or loop.run_until_complete(bot_main()), daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()
    Thread(target=monitor_health, daemon=True).start()
    port = int(os.environ.get("PORT", PORT))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
