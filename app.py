# FastDeals Bot - Flexible Channel Control
import os
import re
import time
import requests
import asyncio
import hashlib
import random
import atexit
from threading import Thread
from flask import Flask, jsonify, request
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from dotenv import load_dotenv
import aiohttp

# ---------------- Load env ---------------- #
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=dotenv_path)

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
CHANNEL_ID_2 = int(os.getenv("CHANNEL_ID_2", "0"))

# Flexible channel control
USE_CHANNEL_1 = os.getenv("USE_CHANNEL_1", "true").lower() == "true"
USE_CHANNEL_2 = os.getenv("USE_CHANNEL_2", "true").lower() == "true"

AMAZON_TAG = os.getenv("AFFILIATE_TAG", "lootfastdeals-21")
DEPLOY_HOOK = os.getenv("RENDER_DEPLOY_HOOK")
ADMIN_NOTIFY = os.getenv("ADMIN_NOTIFY", "").strip()

USE_EARNKARO = os.getenv("USE_EARNKARO", "false").lower() == "true"
DEDUPE_SECONDS = int(os.getenv("DEDUPE_SECONDS", "3600"))
MAX_MSG_LEN = int(os.getenv("MAX_MSG_LEN", "700"))
PREVIEW_LEN = int(os.getenv("PREVIEW_LEN", "500"))

SOURCE_IDS = [
    -1001448358487,
    -1001767957702,
    -1001387180060,
    -1001378801949
]

SHORT_PATTERNS = [
    r"(https?://fkrt\.cc/\S+)", r"(https?://myntr\.it/\S+)",
    r"(https?://dl\.flipkart\.com/\S+)", r"(https?://ajio\.me/\S+)",
    r"(https?://amzn\.to/\S+)", r"(https?://amzn\.in/\S+)",
    r"(https?://bit\.ly/\S+)", r"(https?://tinyurl\.com/\S+)",
    r"(https?://fktt\.co/\S+)", r"(https?://bitly\.cx/\S+)",
    r"(https?://fkt\.co/\S+)"
]

# ---------------- Runtime state ---------------- #
seen_urls = set()
seen_products = {}
last_msg_time = time.time()

# ---------------- Session handling ---------------- #
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "session")

if STRING_SESSION:
    print("Using STRING_SESSION from env")
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
else:
    print(f"Using file session: {SESSION_NAME}.session")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

app = Flask(__name__)

# Rotating hashtags pool
HASHTAG_SETS = [
    "#LootDeals #Discount #OnlineShopping",
    "#Free #Offer #Sale",
    "#TopDeals #BigSale #BestPrice",
    "#PriceDrop #FlashSale #DealAlert",
]

# ---------------- Graceful shutdown ---------------- #
def cleanup():
    """Ensure session is properly closed"""
    try:
        if client.is_connected():
            client.disconnect()
            print("✅ Session properly closed")
    except:
        pass

atexit.register(cleanup)

# ---------------- Helpers ---------------- #
async def notify_admin(message):
    """Send notification to admin about bot status"""
    if not ADMIN_NOTIFY or not ADMIN_NOTIFY.startswith("@"):
        return
    
    try:
        username = ADMIN_NOTIFY[1:] if ADMIN_NOTIFY.startswith("@") else ADMIN_NOTIFY
        await client.send_message(username, message)
        print(f"📢 Admin notified: {message}")
    except Exception as e:
        print(f"⚠️ Admin notify failed: {e}")

async def expand_all(text):
    """Expand short URLs like fkrt.cc, amzn.to etc."""
    urls = sum((re.findall(p, text) for p in SHORT_PATTERNS), [])
    if not urls:
        return text
    async with aiohttp.ClientSession() as s:
        for u in urls:
            try:
                async with s.head(u, allow_redirects=True, timeout=5) as r:
                    expanded_url = str(r.url)
                    text = text.replace(u, expanded_url)
                    print(f"🔗 Expanded {u} → {expanded_url}")
            except Exception as e:
                print(f"⚠️ Expansion failed for {u}: {e}")
    return text

def convert_amazon(text):
    """Force Amazon affiliate tag - FIXED no double tags"""
    # First, replace existing tags with our tag
    text = re.sub(r'([?&])tag=[^&\s&]+', r'\1tag=' + AMAZON_TAG, text)
    
    # Then handle Amazon product links without tags
    pat = r'(https?://(?:www\.)?amazon\.(?:com|in)/(?:.*?/)?(?:dp|gp/product)/([A-Z0-9]{10}))'
    def repl(m):
        asin = m.group(2)
        # Check if URL already has query parameters
        if '?' in m.group(1):
            return f"{m.group(1)}&tag={AMAZON_TAG}"
        else:
            return f"https://www.amazon.in/dp/{asin}/?tag={AMAZON_TAG}"
    text = re.sub(pat, repl, text, flags=re.I)
    
    return text

async def convert_earnkaro(text):
    """Optional EarnKaro wrapping with fallback"""
    if not USE_EARNKARO:
        return text
    urls = re.findall(r"(https?://\S+)", text)
    for u in urls:
        if any(x in u for x in ["flipkart", "myntra", "ajio"]):
            try:
                r = requests.post(
                    "https://api.earnkaro.com/api/deeplink",
                    json={"url": u},
                    headers={"Content-Type": "application/json"},
                    timeout=6
                )
                if r.status_code == 200:
                    ek = r.json().get("data", {}).get("link")
                    if ek:
                        text = text.replace(u, ek)
                        continue
            except Exception as e:
                print(f"⚠️ EarnKaro failed for {u}: {e}")
    return text

async def process(text):
    t = await expand_all(text)
    t = convert_amazon(t)
    t = await convert_earnkaro(t)
    return t

def extract_product_name(text):
    """Improved product name extraction"""
    text_no_urls = re.sub(r'https?://\S+', '', text)
    
    # More flexible patterns for various products
    patterns = [
        r"(?:Samsung|iPhone|OnePlus|Realme|Xiaomi|Redmi|Poco|Motorola|Nokia|LG|Sony|HP|Dell|Lenovo|Asus|Acer|MSI|Canon|Nikon|Boat|JBL|Noise|Fire-Boltt|pTron|Mi|Pepe\s+Jeans|Lee\s+Cooper|Fitspire|Balaymath|Shilsjit|Glutathione|Apple|Elder|Vinegar)\s+[^@\n]+?(?=@|₹|http|$)",
        r"[A-Z][a-z]+(?:\s+[A-Za-z0-9]+)+?(?:\s+\d+(?:cm|inch|mm|GB|TB|MB|MHz|GHz|W|mAh|Hz|MP|K|°|'|”|kg|g|mg|ml|L|tabs|tablets|capsules))+(?=@|₹|http|$)",
        r"Upto\s+\d+%+\s+Off\s+On\s+([^@\n]+?)(?=@|₹|http|$)",
        r"Flat\s+\d+%+\s+Off\s+On\s+([^@\n]+?)(?=@|₹|http|$)",
        r"([A-Za-z][^@\n]{10,}?(?=@|₹|http|\n|$))",  # Generic catch-all for longer product names
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text_no_urls, re.IGNORECASE)
        if match:
            product_name = match.group(0).strip()
            # Clean up common prefixes
            product_name = re.sub(r'^(Upto|Flat)\s+\d+%\s+Off\s+On\s+', '', product_name, flags=re.IGNORECASE)
            if len(product_name) > 10:  # Ensure it's a meaningful name
                return product_name
    return None

def canonicalize(url):
    """Improved URL canonicalization"""
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
    """Improved hashing - less aggressive deduplication"""
    product_name = extract_product_name(msg)
    if product_name:
        clean = re.sub(r"\s+", " ", product_name.lower())
        clean = re.sub(r"[^\w\s]", "", clean)
        # Only use product name if it's substantial
        if len(clean) > 15:
            return hashlib.md5(clean.encode()).hexdigest()
    
    # Fallback: use first 100 chars + URLs for hash
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

async def send_to_telegram_channels(message):
    """Send message to Telegram channels with flexible control - FIXED"""
    channels = []
    
    # Add Channel 1 if enabled
    if USE_CHANNEL_1:
        channels.append(CHANNEL_ID)
        print(f"📢 Channel 1 enabled: {CHANNEL_ID}")
    
    # Add Channel 2 if enabled and valid
    if USE_CHANNEL_2 and CHANNEL_ID_2 and int(CHANNEL_ID_2) != 0:
        channels.append(int(CHANNEL_ID_2))
        print(f"📢 Channel 2 enabled: {CHANNEL_ID_2}")
    
    if not channels:
        print("❌ No channels enabled! Check USE_CHANNEL_1 and USE_CHANNEL_2 settings")
        await notify_admin("🚨 No channels enabled! Check environment variables.")
        return False
    
    print(f"🎯 Sending to {len(channels)} channel(s): {channels}")
    
    success_count = 0
    for channel_id in channels:
        try:
            await client.send_message(channel_id, message, link_preview=False)
            print(f"✅ Sent to Telegram channel {channel_id}")
            success_count += 1
        except Exception as ex:
            print(f"❌ Telegram error for channel {channel_id}: {ex}")
            # Don't notify admin for session conflicts to avoid spam
            if "two different IP addresses" not in str(ex):
                await notify_admin(f"❌ Channel error {channel_id}: {ex}")
    
    print(f"📊 Sent successfully to {success_count}/{len(channels)} channels")
    return success_count > 0

# ---------------- Bot main ---------------- #
async def bot_main():
    global last_msg_time, seen_urls, seen_products
    
    # Validate session first
    try:
        await client.start()
        me = await client.get_me()
        print(f"✅ Logged in as: {me.first_name} (ID: {me.id})")
        
        # Test connection
        await client.get_me()
        print("✅ Session validation successful")
        
    except Exception as e:
        error_msg = f"❌ Session validation failed: {e}"
        print(error_msg)
        if "two different IP addresses" in str(e):
            await notify_admin("🚨 CRITICAL: Session conflict! Generate NEW STRING_SESSION")
        return
    
    # Notify admin that bot started
    await notify_admin("🤖 Bot started successfully! Monitoring 4 source groups.")
    
    sources = []
    for i in SOURCE_IDS:
        try:
            e = await client.get_entity(i)
            sources.append(e.id)
            print(f"✅ Connected source: {e.title} ({i})")
        except Exception as ex:
            print(f"❌ Failed source {i}: {ex}")
            await notify_admin(f"❌ Failed to connect to source {i}: {ex}")

    # Show channel configuration
    print(f"🎯 Channel Configuration:")
    print(f"   Primary Channel: {CHANNEL_ID} ({'ENABLED' if USE_CHANNEL_1 else 'DISABLED'})")
    print(f"   Secondary Channel: {CHANNEL_ID_2} ({'ENABLED' if USE_CHANNEL_2 else 'DISABLED'})")

    @client.on(events.NewMessage(chats=sources))
    async def handler(e):
        global seen_products, seen_urls, last_msg_time

        raw_txt = e.message.message or ""
        if not raw_txt:
            return

        print(f"📨 Raw message: {raw_txt[:120]}...")
        
        # Skip messages that are too short or don't contain URLs
        urls_in_raw = re.findall(r"https?://\S+", raw_txt)
        if len(raw_txt.strip()) < 20 and not urls_in_raw:
            print("⚠️ Skipped: Message too short and no URLs")
            return
            
        try:
            processed = await process(raw_txt)
            urls = re.findall(r"https?://\S+", processed)

            # If no URLs after processing, skip
            if not urls:
                print("⚠️ Skipped: No valid URLs found after processing")
                return

            now = time.time()
            dedupe_keys = []

            # Dedup by product URL (only for valid e-commerce URLs)
            url_keys_added = 0
            for u in urls:
                c = canonicalize(u)
                if c:
                    last_seen = seen_products.get(c)
                    if not last_seen or (now - last_seen) > DEDUPE_SECONDS:
                        dedupe_keys.append(c)
                        url_keys_added += 1
                        print(f"🔗 URL dedupe key: {c}")
                    else:
                        print(f"⚠️ Duplicate URL skipped: {c}")

            # Dedup by text hash (less aggressive)
            text_key = hash_text(processed)
            last_seen = seen_products.get(text_key)
            if not last_seen or (now - last_seen) > DEDUPE_SECONDS:
                dedupe_keys.append(text_key)
                print(f"📝 Text dedupe key: {text_key}")
            else:
                print(f"⚠️ Duplicate text skipped: {text_key}")

            # Only skip if BOTH URL and text are duplicates
            if not dedupe_keys:
                print("⚠️ Skipped: All dedupe keys are duplicates")
                return

            # Update seen products
            for k in dedupe_keys:
                seen_products[k] = now
            for u in urls:
                seen_urls.add(u)

            # Label + hashtags
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

            print(f"📤 Prepared message: {msg[:100]}...")

            # Repost: send to targets (not raw forward)
            success = await send_to_telegram_channels(msg)

            if success:
                last_msg_time = time.time()
                print(f"✅ Processed at {time.strftime('%H:%M:%S')}")
            else:
                print(f"❌ Failed to send to channels")
            
        except Exception as ex:
            error_msg = f"❌ Error processing message: {str(ex)}"
            print(error_msg)
            if "two different IP addresses" not in str(ex):
                await notify_admin(error_msg)

    print("🔄 Bot is now actively monitoring for messages...")
    await client.run_until_disconnected()

# ---------------- Maintenance & HTTP ---------------- #
def redeploy():
    if not DEPLOY_HOOK:
        return False
    try:
        requests.post(DEPLOY_HOOK, timeout=10)
        return True
    except:
        return False

def keep_alive():
    while True:
        time.sleep(14 * 60)
        try:
            requests.get("http://127.0.0.1:10000/ping", timeout=5)
        except:
            pass

def monitor_health():
    global last_msg_time
    while True:
        time.sleep(300)
        if (time.time() - last_msg_time) > 1800:
            print("⚠️ Idle 30+ min, redeploying")
            redeploy()

def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(bot_main())

# ---------------- Flask endpoints ---------------- #
@app.route("/")
def home():
    return jsonify({
        "status": "running", 
        "telegram_primary": CHANNEL_ID, 
        "telegram_secondary": CHANNEL_ID_2,
        "channel_1_enabled": USE_CHANNEL_1,
        "channel_2_enabled": USE_CHANNEL_2
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
        "channel_2_enabled": USE_CHANNEL_2
    })

@app.route("/redeploy", methods=["POST"])
def redeploy_endpoint():
    return ("ok", 200) if redeploy() else ("fail", 500)

# ---------------- Entrypoint ---------------- #
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    Thread(target=start_loop, args=(loop,), daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()
    Thread(target=monitor_health, daemon=True).start()
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)