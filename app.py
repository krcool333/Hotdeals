# app.py - FASTDEALS Bot (ready-to-paste)
# Preserves your per-channel source routing and in-memory dedupe.
# Adds: media mode (separate/caption/embedded), single-affiliate-tag enforcement, FORCE_MEDIA toggle.
# CHANGE MARKERS: see "### CHANGE 1/2/3" comments below.

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
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from dotenv import load_dotenv

# Optional Pillow for embedded-image mode. If not installed, code will fallback.
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

CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
CHANNEL_ID_2 = int(os.getenv("CHANNEL_ID_2", "0"))

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

# Render API credentials (optional)
RENDER_API_KEY = os.getenv("RENDER_API_KEY", "").strip()
RENDER_SERVICE_ID = os.getenv("RENDER_SERVICE_ID", "").strip()

PORT = int(os.getenv("PORT", "10000"))
EXTERNAL_URL = os.getenv("EXTERNAL_URL", "").strip()

# Rate limiting & backoff
MIN_INTERVAL_SECONDS = int(os.getenv("MIN_INTERVAL_SECONDS", "60"))
MIN_JITTER = int(os.getenv("MIN_JITTER", "3"))
MAX_JITTER = int(os.getenv("MAX_JITTER", "12"))
MAX_REDEPLOYS_PER_DAY = int(os.getenv("MAX_REDEPLOYS_PER_DAY", "3"))
REDEPLOY_BACKOFF_BASE = int(os.getenv("REDEPLOY_BACKOFF_BASE", "60"))

# ---------------- NEW changes (3) - defaults ----------------
# CHANGE 1: MEDIA_MODE -> 'separate' | 'caption' | 'embedded'
MEDIA_MODE = os.getenv("MEDIA_MODE", "separate").lower()  # default 'separate'
# CHANGE 3: FORCE_MEDIA toggle env
FORCE_MEDIA = os.getenv("FORCE_MEDIA", "true").lower() == "true"

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
    "#Free #Offer #Sale",
    "#TopDeals #BigSale #BestPrice",
    "#PriceDrop #FlashSale #DealAlert",
]

# ---------------- Runtime state ---------------- #
seen_urls = set()
seen_channel_1 = {}
seen_channel_2 = {}
last_msg_time = time.time()
last_sent_channel = {}
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
async def notify_admin(message):
    """Send notification to admin (via user session)."""
    if not ADMIN_NOTIFY:
        return
    try:
        target = ADMIN_NOTIFY[1:] if ADMIN_NOTIFY.startswith("@") else ADMIN_NOTIFY
        await client.send_message(target, message)
        print(f"📢 Admin notified: {message}")
    except Exception as e:
        print(f"⚠️ Admin notify failed: {e}")

async def expand_all(text):
    """Expand short URLs like fkrt.cc, amzn.to etc (async)."""
    urls = sum((re.findall(p, text) for p in SHORT_PATTERNS), [])
    if not urls:
        return text
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        for u in urls:
            try:
                async with s.head(u, allow_redirects=True) as r:
                    expanded_url = str(r.url)
                    text = text.replace(u, expanded_url)
                    print(f"🔗 Expanded {u} → {expanded_url}")
            except Exception as e:
                print(f"⚠️ Expansion failed for {u}: {e}")
    return text

# ---------------- CHANGE 2: Single affiliate tag enforcement ----------------
def ensure_affiliate_tag(url: str) -> str:
    """Return url with single affiliate tag param (AFFILIATE_TAG)."""
    try:
        parts = urlparse(url)
        qs = dict(parse_qsl(parts.query, keep_blank_values=True))
        qs['tag'] = AFFILIATE_TAG
        new_query = urlencode(qs, doseq=True)
        new_parts = parts._replace(query=new_query)
        return urlunparse(new_parts)
    except Exception:
        return url

def convert_amazon(text: str) -> str:
    """
    Replace/add affiliate tag for any Amazon product links found in text.
    Ensures a single tag parameter.
    """
    def repl(m):
        full = m.group(0)
        fixed = ensure_affiliate_tag(full)
        return fixed

    pat = re.compile(r'https?://(?:www\.)?amazon\.(?:in|com)/(?:.*?/)?(?:dp|gp/product)/[A-Z0-9]{10}[^ \n]*', flags=re.I)
    return pat.sub(repl, text)

# Earnkaro conversion (same)
async def convert_earnkaro(text):
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
    t = await expand_all(text)
    t = convert_amazon(t)
    t = await convert_earnkaro(t)
    return t

def extract_product_name(text):
    text_no_urls = re.sub(r'https?://\S+', '', text)
    patterns = [
        r"(?:Samsung|iPhone|OnePlus|Realme|Xiaomi|Redmi|Poco|Motorola|Nokia|LG|Sony|HP|Dell|Lenovo|Asus|Acer|MSI|Canon|Nikon|Boat|JBL|Noise|Fire-Boltt|pTron|Mi)\s+[^@\n]+?(?=@|₹|http|$)",
        r"[A-Z][a-z]+(?:\s+[A-Za-z0-9]+)+?(?:\s+\d+(?:cm|inch|GB|TB|MB|mAh|MP|Hz))+(?=@|₹|http|$)",
        r"([A-Za-z][^@\n]{10,}?(?=@|₹|http|\n|$))",
    ]
    for p in patterns:
        m = re.search(p, text_no_urls, re.IGNORECASE)
        if m:
            product_name = m.group(0).strip()
            if len(product_name) > 10:
                return product_name
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

# ---------------- Image helpers ----------------
async def get_og_image_from_page(url: str, timeout_sec: int = 6):
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

def build_embedded_image(text_top: str, product_bytes: bytes, max_width: int = 900) -> bytes:
    """
    Compose a new image where the top area contains wrapped text (text_top),
    and the product image is below. Requires Pillow. Returns PNG bytes.
    """
    if not PIL_AVAILABLE:
        raise RuntimeError("Pillow not installed for embedded mode")
    try:
        # open product image
        prod_im = Image.open(io.BytesIO(product_bytes)).convert("RGBA")
        # scale product image to max width
        if prod_im.width > max_width:
            ratio = max_width / prod_im.width
            prod_im = prod_im.resize((max_width, int(prod_im.height * ratio)), Image.LANCZOS)

        # prepare text area
        try:
            font = ImageFont.truetype("DejaVuSans-Bold.ttf", 22)
        except Exception:
            font = ImageFont.load_default()

        # wrap the text to fit width
        draw_temp = ImageDraw.Draw(prod_im)
        # we need width of the final image - using prod_im.width
        wrap_width = prod_im.width - 20
        words = text_top.split()
        lines = []
        line = ""
        for w in words:
            if draw_temp.textsize((line + " " + w).strip(), font=font)[0] <= wrap_width:
                line = (line + " " + w).strip()
            else:
                lines.append(line)
                line = w
        if line:
            lines.append(line)
        text_height = sum(draw_temp.textsize(l, font=font)[1] + 6 for l in lines) + 16

        # create final canvas (text area above product)
        final_h = text_height + prod_im.height + 20
        final_im = Image.new("RGBA", (prod_im.width + 20, final_h), (255, 255, 255, 255))
        draw = ImageDraw.Draw(final_im)
        # draw text
        y = 8
        for ln in lines:
            draw.text((10, y), ln, font=font, fill=(0, 0, 0))
            y += draw.textsize(ln, font=font)[1] + 6
        # paste product image
        final_im.paste(prod_im, (10, text_height + 8), prod_im if prod_im.mode == "RGBA" else None)

        out = io.BytesIO()
        final_im.save(out, format="PNG")
        out.seek(0)
        return out.read()
    except Exception as e:
        raise

# ---------------- Sending logic: supports MEDIA_MODE and FORCE_MEDIA ----------------
async def send_to_specific_channel(message, channel_id, channel_name, msg_obj=None):
    """
    MEDIA_MODE handling:
      - 'separate' : sends text first (clickable), then image as separate message (image below).
      - 'caption'  : single message — image with caption (caption below image).
      - 'embedded' : single image composed with text on top and product image below (requires Pillow).
    FORCE_MEDIA False -> only send text (no media).
    """
    try:
        # Respect FORCE_MEDIA
        if not FORCE_MEDIA:
            await client.send_message(channel_id, message, link_preview=False)
            print(f"✅ Sent text-only to {channel_name} ({channel_id}) due to FORCE_MEDIA=false")
            return True

        # Try to extract image bytes if message object provided
        image_bytes = None
        if msg_obj is not None:
            try:
                image_bytes = await extract_image_from_msg_obj(msg_obj)
            except Exception as e:
                print(f"⚠️ Image extraction error: {e}")
                image_bytes = None

        # MEDIA_MODE behavior
        mode = MEDIA_MODE or "separate"
        # default fallbacks if unsupported mode or missing dependencies
        if mode == "embedded" and not PIL_AVAILABLE:
            # fallback to caption (safer) if Pillow missing
            mode = "caption"

        # Option: if no image found, just send text
        if not image_bytes:
            await client.send_message(channel_id, message, link_preview=False)
            print(f"✅ Sent text to {channel_name} ({channel_id}) (no image found)")
            return True

        # Mode: separate -> text then image (keeps link clickable)
        if mode == "separate":
            # Send text first (no preview to avoid duplicate large preview)
            try:
                await client.send_message(channel_id, message, link_preview=False)
                print(f"✅ Sent text to {channel_name} ({channel_id}) (separate mode)")
            except Exception as e_text:
                print(f"❌ Failed to send text to {channel_name}: {e_text}")
                if "two different IP addresses" not in str(e_text):
                    try:
                        await notify_admin(f"❌ {channel_name} text send failed: {e_text}")
                    except Exception:
                        pass
                return False
            # short delay to ensure ordering
            await asyncio.sleep(0.7)
            # send image separately
            try:
                bio = io.BytesIO(image_bytes)
                bio.name = "deal.png"
                await client.send_file(channel_id, file=bio, caption=None, link_preview=False)
                print(f"✅ Sent image to {channel_name} ({channel_id}) (separate mode)")
                return True
            except Exception as e_img:
                print(f"⚠️ Image send failed for {channel_name}, text already sent: {e_img}")
                try:
                    await notify_admin(f"⚠️ Image send failed for {channel_name}: {e_img}")
                except Exception:
                    pass
                return True  # text already sent -> treat as success

        # Mode: caption -> single message: image + caption (caption below image)
        elif mode == "caption":
            try:
                promo = image_bytes
                # optional overlay (keep original image if overlay fails)
                if PIL_AVAILABLE:
                    try:
                        promo = build_embedded_image("", image_bytes)  # using empty text overlay
                    except Exception:
                        promo = image_bytes
                bio = io.BytesIO(promo)
                bio.name = "deal.png"
                # send image with caption (caption will appear below image)
                await client.send_file(channel_id, file=bio, caption=message, link_preview=False)
                print(f"✅ Sent image+caption to {channel_name} ({channel_id}) (caption mode)")
                return True
            except Exception as e:
                print(f"❌ Caption-mode send failed: {e}")
                if "two different IP addresses" not in str(e):
                    try:
                        await notify_admin(f"❌ {channel_name} caption send failed: {e}")
                    except Exception:
                        pass
                return False

        # Mode: embedded -> compose image with text on top (single message, link not clickable)
        elif mode == "embedded":
            try:
                # Compose image with text on top
                try:
                    final_bytes = build_embedded_image(message, image_bytes)
                except Exception as e_embed:
                    print(f"⚠️ Embedded compose failed: {e_embed} (falling back to caption mode)")
                    # fallback to caption
                    final_bytes = image_bytes
                bio = io.BytesIO(final_bytes)
                bio.name = "deal.png"
                await client.send_file(channel_id, file=bio, caption=None, link_preview=False)
                print(f"✅ Sent embedded image to {channel_name} ({channel_id}) (embedded mode)")
                return True
            except Exception as e:
                print(f"❌ Embedded-mode send failed: {e}")
                if "two different IP addresses" not in str(e):
                    try:
                        await notify_admin(f"❌ {channel_name} embedded send failed: {e}")
                    except Exception:
                        pass
                return False

        else:
            # unknown mode -> fallback to separate
            return await send_to_specific_channel(message, channel_id, channel_name, msg_obj)
    except Exception as ex:
        print(f"❌ Telegram error for {channel_name} ({channel_id}): {ex}")
        if "two different IP addresses" not in str(ex):
            try:
                await notify_admin(f"❌ {channel_name} error ({channel_id}): {ex}")
            except Exception:
                pass
        return False

# ---------------- Updated process_and_send accepts optional msg_obj ----------------
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
                    print(f"⚠️ [{channel_name}] Duplicate URL skipped: {c}")

        # dedupe by text hash
        text_key = hash_text(processed)
        last_seen = seen_dict.get(text_key)
        if not last_seen or (now - last_seen) > DEDUPE_SECONDS:
            dedupe_keys.append(text_key)
            print(f"📝 [{channel_name}] Text dedupe key: {text_key}")
        else:
            print(f"⚠️ [{channel_name}] Duplicate text skipped: {text_key}")

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
        need_wait = MIN_INTERVAL_SECONDS + random.randint(MIN_JITTER, MAX_JITTER)
        if (now_ts - last) < need_wait:
            print(f"⏱️ Rate limit: skip {channel_name}. Need {int(need_wait - (now_ts - last))}s more.")
            return False

        # small jitter before sending (stagger across channels)
        await asyncio.sleep(random.uniform(0.5, 2.5))

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
            await notify_admin(error_msg)
        return False

# ---------------- Bot main ---------------- #
async def bot_main():
    global last_msg_time, seen_urls, seen_channel_1, seen_channel_2

    try:
        await client.start()
        me = await client.get_me()
        print(f"✅ Logged in as: {me.first_name} (ID: {me.id})")
    except Exception as e:
        error_msg = f"❌ Session validation failed: {e}"
        print(error_msg)
        if "two different IP addresses" in str(e):
            await notify_admin("🚨 CRITICAL: Session conflict! Generate NEW STRING_SESSION")
        return

    await notify_admin("🤖 Bot started successfully! Monitoring different sources for each channel.")

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
                await notify_admin(f"❌ [Channel 1] Failed to connect to source {i}: {ex}")

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
                await notify_admin(f"❌ [Channel 2] Failed to connect to source {i}: {ex}")

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

# ---------------- Deploy helpers & monitor ---------------- #
def redeploy_via_hook():
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
    global last_msg_time, redeploy_count_today, last_redeploy_time
    while True:
        time.sleep(300)
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
        time.sleep(300)
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
    triggered = False
    if RENDER_API_KEY and RENDER_SERVICE_ID:
        try:
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
    loop = asyncio.new_event_loop()
    Thread(target=lambda: asyncio.set_event_loop(loop) or loop.run_until_complete(bot_main()), daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()
    Thread(target=monitor_health, daemon=True).start()
    port = int(os.environ.get("PORT", PORT))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
