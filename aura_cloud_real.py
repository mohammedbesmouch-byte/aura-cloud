# -*- coding: utf-8 -*-
# AURA CLOUD V4
import asyncio, json, os, requests, re, urllib.parse, base64, sys, locale, time, threading, traceback, gc
from requests.adapters import HTTPAdapter
from datetime import datetime, timezone, timedelta, time as dt_time
from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver
from urllib.parse import urlparse
from openai import OpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
import logging
# Load env vars with BOM-safe UTF-8 handling
from dotenv import load_dotenv
try:
    # Some .env files have BOM, read as utf-8-sig
    with open('.env', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")
except:
    load_dotenv(override=True)

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass
try: locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
except Exception: pass
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('aura.log'), logging.StreamHandler()])

# ─── CONFIG ───
DEFAULT_POSITION_SIZE = float(_env("POSITION_SIZE", "1000"))
# Allow loading from .env even if env vars not set
def _env(k, default=""):
    v = os.environ.get(k, "")
    if not v:
        try:
            with open('.env', encoding='utf-8-sig') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith(k):
                        v = line.split("=",1)[1].strip().strip('"').strip("'")
                        break
        except: pass
    return v or default

TELEGRAM_BOT_TOKEN = _env("TELEGRAM_BOT_TOKEN")
OPENROUTER_API_KEY = _env("OPENROUTER_API_KEY")
TWELVEDATA_API_KEY = _env("TWELVEDATA_API_KEY")
NEWS_API_KEY = _env("NEWS_API_KEY")
RADAR_CHAT_ID = int(_env("RADAR_CHAT_ID", "0"))
BOT_PASSWORD = _env("BOT_PASSWORD", "aura2026")
ADMIN_ID = int(_env("ADMIN_ID", str(RADAR_CHAT_ID)))
GITHUB_TOKEN = _env("GITHUB_TOKEN", "")
GITHUB_REPO = "mohammedbesmouch-byte/aura-monitor"
FPS_API_KEY = _env("FPS_API_KEY", "")
FPS_SERVER_ID = _env("FPS_SERVER_ID", "03a1779c")
GROQ_API_KEY = _env("GROQ_API_KEY", "")
DEEPSEEK_API_KEY = _env("DEEPSEEK_API_KEY", "")

signal_lock = threading.Lock()
alerts_lock = threading.Lock()

RADAR_ASSETS = ["EURUSD","USDJPY","GBPUSD","AUDUSD","USDCAD","USDCHF","NZDUSD",
                "GBPJPY","EURJPY","GBPAUD","EURCHF","EURAUD","GBPCAD","AUDJPY",
                "CADJPY","NZDJPY","CHFJPY","XAUUSD","XAGUSD","BTCUSD","ETHUSD",
                "EURGBP","GBPCHF","XRPUSD","LTCUSD"]
RADAR_INTERVAL = 1800; RADAR_ENABLED = True; MIN_CONFIDENCE = 80
last_signals = []; price_alerts = []; signal_log = []

def load_json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f: return json.load(f)
    except Exception as e:
        logging.warning(f"load_json {path}: {e}")
        return default

def save_json(path, data):
    try:
        with open(path, 'w', encoding='utf-8') as f: json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e: logging.error(f"Save {path}: {e}")

def archive_append(signal):
    try:
        with open('archive.json', 'a', encoding='utf-8') as f:
            f.write(json.dumps(signal, ensure_ascii=False) + '\n')
    except Exception as e:
        logging.error(f"Archive append: {e}")

signal_log = load_json('signals.json', [])
# Clean garbage WAIT signals on startup
signal_log = [s for s in signal_log if s.get("confidence", 0) >= 20 or s.get("rec") not in ("WAIT", "انتظار")]
if len(signal_log) > 200: signal_log[:] = signal_log[-200:]
price_alerts = load_json('alerts.json', [])
authorized_users = set(load_json('users.json', []))
_DATA_CACHE = None; _DATA_CACHE_TS = 0; _DATA_CACHE_TTL = 300
_SCAN_CACHE = None; _SCAN_CACHE_TS = 0; _SCAN_CACHE_TTL = 120
PRICE_CACHE = load_json('price_cache.json', {}); PRICE_CACHE_TS = PRICE_CACHE.get("_ts", 0)
def _invalidate_cache(): global _DATA_CACHE, _DATA_CACHE_TS; _DATA_CACHE = None; _DATA_CACHE_TS = 0

# ─── AI PROVIDERS ───
BACKUP_API_KEY = _env("BACKUP_API_KEY", "")
AI_PROVIDERS = []
_ai_clients_cache = {}
_http_session = requests.Session()
_http_session.mount('https://', HTTPAdapter(pool_connections=5, pool_maxsize=10))
_http_session.mount('http://', HTTPAdapter(pool_connections=5, pool_maxsize=10))
# Use session for all requests to prevent fd leak
requests.get = _http_session.get
requests.post = _http_session.post
requests.put = _http_session.put
# 1) Groq — Primary
if GROQ_API_KEY:
    AI_PROVIDERS.append({"name": "groq", "base_url": "https://api.groq.com/openai/v1", "api_key": GROQ_API_KEY, "model": "llama-3.3-70b-versatile"})
# 2) MiniMax M2.5 via OpenRouter — Fallback
if BACKUP_API_KEY:
    AI_PROVIDERS.append({"name": "minimax", "base_url": "https://openrouter.ai/api/v1", "api_key": BACKUP_API_KEY, "model": "minimax/minimax-m2.7"})
# 3) DeepSeek via OpenRouter — Backup
if BACKUP_API_KEY:
    AI_PROVIDERS.append({"name": "deepseek", "base_url": "https://openrouter.ai/api/v1", "api_key": BACKUP_API_KEY, "model": "deepseek/deepseek-chat-v3-0324:free"})
# 4) Gemini Flash via OpenRouter — Backup
if BACKUP_API_KEY:
    AI_PROVIDERS.append({"name": "gemini", "base_url": "https://openrouter.ai/api/v1", "api_key": BACKUP_API_KEY, "model": "google/gemini-2.0-flash-001"})
_current_provider = 0
_last_primary_check = 0
_PRIMARY_CHECK_INTERVAL = 1800
# Learning system: stores lessons from past trades
lessons_log = load_json('lessons.json', [])
MAX_LESSONS = 50

def _ai_request(prompt, temperature=0.1, provider_idx=None, timeout=30.0):
    global _current_provider, _last_primary_check
    errors = []
    if provider_idx is not None and provider_idx < len(AI_PROVIDERS):
        start = provider_idx
        loop_count = 1
    else:
        start = _current_provider
        if start != 0:
            now = time.time()
            if now - _last_primary_check > _PRIMARY_CHECK_INTERVAL:
                start = 0
                _last_primary_check = now
        loop_count = len(AI_PROVIDERS)
    for i in range(loop_count):
        idx = (start + i) % len(AI_PROVIDERS)
        p = AI_PROVIDERS[idx]
        try:
            ckey = p["api_key"] + p["base_url"] + str(timeout)
            if ckey not in _ai_clients_cache:
                _ai_clients_cache[ckey] = OpenAI(api_key=p["api_key"], base_url=p["base_url"], max_retries=2, timeout=timeout)
            c = _ai_clients_cache[ckey]
            r = c.chat.completions.create(model=p["model"],
                messages=[{"role":"user","content":prompt}], temperature=temperature, max_tokens=4096)
            if idx != _current_provider and provider_idx is None:
                _current_provider = idx
                logging.info(f"Switched AI provider → {p['name']}")
            return r
        except Exception as e:
            err = str(e).lower()
            if "402" in err or "429" in err or "404" in err or "quota" in err or "rate limit" in err or "insufficient" in err or "not found" in err or "no endpoints" in err:
                if provider_idx is not None:
                    errors.append(f"{p['name']}: {e}"); break
                nxt = (idx + 1) % len(AI_PROVIDERS)
                if nxt != idx:
                    _current_provider = nxt
                    logging.warning(f"Provider {p['name']} ({p['model']}) → {err[:80]} → switching to {AI_PROVIDERS[nxt]['name']}")
                continue
            errors.append(f"{p['name']}: {e}")
            if i < loop_count - 1:
                continue
            break
    raise Exception(f"AI all failed: {' | '.join(errors)}")

price_cache = {}
price_cache_time = 0



# ─── AUTH ───
def is_authorized(user_id):
    return user_id == ADMIN_ID or user_id in authorized_users

async def auth_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, handler):
    user = update.effective_user
    if not user:
        return
    if not is_authorized(user.id):
        authorized_users.add(user.id)
        save_json('users.json', list(authorized_users))
    return await handler(update, context)

async def unlock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pwd = context.args[0] if context.args else ""
    if pwd == BOT_PASSWORD:
        authorized_users.add(update.effective_user.id)
        save_json('users.json', list(authorized_users))
        await update.message.reply_text("[OK] *تم فتح القفل!*\nأهلاً بك في AURA CLOUD \nاستخدم /start للبدء", parse_mode="Markdown")
    else:
        await update.message.reply_text("[XX] كلمة المرور غير صحيحة.")

async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("[XX] هذا الأمر للمدير فقط.")
    if not context.args:
        return await update.message.reply_text("[XX] الاستخدام: /approve <user_id>\nلرؤية المستخدمين: /users")
    try:
        uid = int(context.args[0])
        authorized_users.add(uid)
        save_json('users.json', list(authorized_users))
        await update.message.reply_text(f"[OK] تمت الموافقة على المستخدم `{uid}`", parse_mode="Markdown")
        try: await context.bot.send_message(chat_id=uid, text="[OK] *تمت الموافقة عليك!*\nأهلاً بك في AURA CLOUD ", parse_mode="Markdown")
        except Exception as e: logging.warning(f"approve notify: {e}")
    except: await update.message.reply_text("[XX] معرف المستخدم غير صالح.")

async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = "*Authorized Users:*\n───────────────\n"
    for uid in sorted(authorized_users): msg += f"* `{uid}`\n"
    msg += f"───────────────\nTotal: `{len(authorized_users)}`"
    await update.message.reply_text(msg, parse_mode="Markdown")

# ─── API HELPERS ───
YAHOO_MAP = {"XAUUSD":"GC=F","XAGUSD":"SI=F","BTCUSD":"BTC-USD","ETHUSD":"ETH-USD","XRPUSD":"XRP-USD","LTCUSD":"LTC-USD"}

def yahoo_symbol(s):
    if s in YAHOO_MAP:
        return YAHOO_MAP[s]
    if len(s) == 6 and '/' not in s:
        return f"{s[:3]}{s[3:]}=X"
    return s

def yahoo_price(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol(symbol)}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        d = r.json()
        return float(d["chart"]["result"][0]["meta"]["regularMarketPrice"]), None
    except: return None, "yahoo error"

def yahoo_history(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol(symbol)}?range=5d&interval=15m"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        d = r.json()
        closes = d["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [c for c in closes if c], None
    except: return None, "yahoo history error"

def yahoo_ohlcv(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol(symbol)}?range=5d&interval=15m"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        d = r.json()
        result = d["chart"]["result"][0]
        ts = result["timestamp"]
        q = result["indicators"]["quote"][0]
        opens, highs, lows, closes, vols = q.get("open"), q.get("high"), q.get("low"), q.get("close"), q.get("volume")
        bars = []
        for i in range(len(ts)):
            if closes and i < len(closes) and closes[i] is not None:
                bars.append({
                    "time": ts[i] * 1000,
                    "open": round(opens[i], 5) if opens and i < len(opens) and opens[i] else round(closes[i], 5),
                    "high": round(highs[i], 5) if highs and i < len(highs) and highs[i] else round(closes[i], 5),
                    "low": round(lows[i], 5) if lows and i < len(lows) and lows[i] else round(closes[i], 5),
                    "close": round(closes[i], 5),
                    "volume": vols[i] if vols and i < len(vols) and vols[i] else 0
                })
        return bars, None
    except Exception as e:
        return None, str(e)

def _refresh_price_cache():
    global PRICE_CACHE, PRICE_CACHE_TS
    while True:
        try:
            ph = {}; oh = {}
            for asset in RADAR_ASSETS:
                try:
                    hist, err = yahoo_history(asset)
                    if hist and not err: ph[asset] = [round(p,3) for p in hist[-60:]]
                except Exception as e: logging.warning(f"price_cache hist {asset}: {e}")
                try:
                    bars, err = yahoo_ohlcv(asset)
                    if bars and not err: oh[asset] = bars[-100:]
                except Exception as e: logging.warning(f"price_cache ohlcv {asset}: {e}")
            PRICE_CACHE = {"history": ph, "ohlcv": oh, "_ts": time.time()}
            PRICE_CACHE_TS = time.time()
            save_json('price_cache.json', PRICE_CACHE)
        except Exception as e: logging.warning(f"price_cache cycle: {e}")
        time.sleep(120)

def _track_loop():
    while True:
        try: track_active_trades()
        except Exception as e: logging.warning(f"_track_loop: {e}")
        time.sleep(30)

def twelvedata_get(endpoint, params):
    params['apikey'] = TWELVEDATA_API_KEY
    try:
        r = requests.get(f"https://api.twelvedata.com/{endpoint}", params=params, timeout=10)
        d = r.json()
        if 'code' in d and d['code'] in (400, 401, 404, 429): return None, d.get('message', '')
        return d, None
    except: return None, "connection error"

def get_price(symbol):
    now = time.time()
    global price_cache, price_cache_time
    if now - price_cache_time < 55 and symbol in price_cache:
        return price_cache[symbol], None
    p, err = yahoo_price(symbol)
    if not err:
        price_cache[symbol] = p
        price_cache_time = now
        return p, None
    f = symbol[:3]+'/'+symbol[3:] if len(symbol)==6 and '/' not in symbol else symbol
    d, err2 = twelvedata_get('price', {'symbol': f})
    if err2: return None, err2
    p = float(d.get('price', 0))
    price_cache[symbol] = p
    price_cache_time = now
    return p, None

def get_historical(symbol, interval='15min', count=15):
    hist, err = yahoo_history(symbol)
    if hist: return hist[:count]
    f = symbol[:3]+'/'+symbol[3:] if len(symbol)==6 and '/' not in symbol else symbol
    d, err = twelvedata_get('time_series', {'symbol': f, 'interval': interval, 'outputsize': count})
    if err or 'values' not in d: return []
    return [float(c['close']) for c in d['values']]

def get_news(symbol):
    try:
        r = requests.get(f"https://newsapi.org/v2/everything?q={symbol}&apiKey={NEWS_API_KEY}&pageSize=3&language=en", timeout=10)
        arts = r.json().get('articles', [])
        return [(a['title'], a.get('description','')) for a in arts[:3]] or [("—", "")]
    except: return [("—", "")]

# ─── TECHNICAL INDICATORS ───
def calc_atr(bars, period=14):
    if not bars or len(bars) < period + 1: return None
    trs = []
    for i in range(1, len(bars)):
        hl = bars[i]["high"] - bars[i]["low"]
        hc = abs(bars[i]["high"] - bars[i-1]["close"])
        lc = abs(bars[i]["low"] - bars[i-1]["close"])
        trs.append(max(hl, hc, lc))
    if not trs: return None
    return sum(trs[-period:]) / period

def calc_rsi(prices, period=14):
    if len(prices) < period+1: return None
    d = [prices[i]-prices[i-1] for i in range(1,len(prices))]
    g = [x if x>0 else 0 for x in d]; l = [-x if x<0 else 0 for x in d]
    ag = sum(g[:period])/period; al = sum(l[:period])/period
    return round(100.0-(100.0/(1.0+ag/al)),2) if al else 100.0

def rsi_score(rsi):
    if rsi is None: return 50
    if rsi <= 30: return 90;
    if rsi <= 40: return 75;
    if rsi >= 70: return 90;
    if rsi >= 60: return 75;
    return 50

# ─── MARKET REGIME ───
def yahoo_daily(symbol, days=30):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol(symbol)}?range={days}d&interval=1d"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        d = r.json()
        closes = d["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [c for c in closes if c], None
    except:
        return None, "error"

def calc_adx(bars, period=14):
    if not bars or len(bars) < period + 1: return None
    trs = []; pds = []; mds = []
    for i in range(1, len(bars)):
        hl = bars[i]["high"] - bars[i]["low"]
        hc = abs(bars[i]["high"] - bars[i-1]["close"])
        lc = abs(bars[i]["low"] - bars[i-1]["close"])
        trs.append(max(hl, hc, lc))
        up = bars[i]["high"] - bars[i-1]["high"]
        down = bars[i-1]["low"] - bars[i]["low"]
        pds.append(up if up > down and up > 0 else 0)
        mds.append(down if down > up and down > 0 else 0)
    if len(trs) < period: return None
    atr = sum(trs[-period:]) / period or 0.001
    pdi = sum(pds[-period:]) / period / atr * 100 if atr else 0
    mdi = sum(mds[-period:]) / period / atr * 100 if atr else 0
    dx = abs(pdi - mdi) / (pdi + mdi) * 100 if (pdi + mdi) else 0
    return round(dx, 1), round(pdi, 1), round(mdi, 1)

def calc_bb(prices, period=20):
    if len(prices) < period: return None
    sma = sum(prices[-period:]) / period
    variance = sum((p - sma) ** 2 for p in prices[-period:]) / period
    std = variance ** 0.5
    return {"upper": round(sma + 2*std, 5), "lower": round(sma - 2*std, 5), "middle": round(sma, 5), "width": round((2*std)/sma*100, 1) if sma else 0, "position": round((prices[-1]-sma+2*std)/(4*std)*100, 1) if std else 50}

def calc_sma(prices, period):
    if len(prices) < period: return None
    return sum(prices[-period:]) / period

def calc_ema(prices, period):
    if len(prices) < period + 1: return None
    multiplier = 2 / (period + 1)
    ema = sum(prices[-period:]) / period
    for p in prices[-(period+1):-period]:
        ema = (p - ema) * multiplier + ema
    return ema

def calc_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return None
    ema_fast = calc_ema(prices, fast)
    ema_slow = calc_ema(prices, slow)
    if ema_fast is None or ema_slow is None: return None
    macd_line = ema_fast - ema_slow
    return {"macd": round(macd_line, 5), "signal": 0, "histogram": 0}

def calc_stochastic(bars, period=14, k_smooth=3):
    if not bars or len(bars) < period: return None
    recent = bars[-period:]
    high = max(b["high"] for b in recent)
    low = min(b["low"] for b in recent)
    close = recent[-1]["close"]
    if high == low: return 50
    return round((close - low) / (high - low) * 100, 1)

def calc_pivot_points(bars):
    if not bars or len(bars) < 2: return None
    prev = bars[-2] if len(bars) >= 2 else bars[-1]
    high, low, close = prev["high"], prev["low"], prev["close"]
    pp = (high + low + close) / 3
    r1 = 2 * pp - low; r2 = pp + (high - low)
    s1 = 2 * pp - high; s2 = pp - (high - low)
    return {"pp": round(pp, 5), "r1": round(r1, 5), "r2": round(r2, 5), "s1": round(s1, 5), "s2": round(s2, 5)}

def detect_regime(adx_result, bb_result):
    trending = "sideways"
    strength = "low"
    if adx_result:
        adx, pdi, mdi = adx_result
        if adx >= 25: trending = "trending"
        elif adx >= 15: trending = "weak_trend"
        else: trending = "ranging"
        if adx >= 40: strength = "strong"
        elif adx >= 25: strength = "medium"
        direction = "up" if pdi > mdi else "down" if mdi > pdi else "flat"
    else: direction = "flat"
    volatility = "normal"
    if bb_result:
        if bb_result["width"] > 8: volatility = "high"
        elif bb_result["width"] < 3: volatility = "low"
    return {"trend": trending, "direction": direction, "strength": strength, "volatility": volatility}

CANDLE_PATTERNS = {
    "doji": lambda o,h,l,c: abs(c-o) <= (h-l)*0.05,
    "hammer": lambda o,h,l,c: (c-l) > (h-c)*2 and (c-l) > (h-l)*0.6 and abs(c-o) <= (h-l)*0.1,
    "shooting_star": lambda o,h,l,c: (h-c) > (c-l)*2 and (h-c) > (h-l)*0.6 and abs(c-o) <= (h-l)*0.1,
    "bullish_engulfing": lambda p, c: p["close"] < p["open"] and c["close"] > c["open"] and c["close"] > p["open"] and c["open"] < p["close"],
    "bearish_engulfing": lambda p, c: p["close"] > p["open"] and c["close"] < c["open"] and c["close"] < p["open"] and c["open"] > p["close"],
}
def detect_candle_patterns(bars):
    if not bars or len(bars) < 3: return [], "", False, False
    last = bars[-1]; o,h,l,c = last["open"],last["high"],last["low"],last["close"]
    patterns = []
    if CANDLE_PATTERNS["doji"](o,h,l,c): patterns.append("Doji")
    if CANDLE_PATTERNS["hammer"](o,h,l,c): patterns.append("Hammer")
    if CANDLE_PATTERNS["shooting_star"](o,h,l,c): patterns.append("Shooting Star")
    if len(bars) >= 2:
        prev = bars[-2]
        if CANDLE_PATTERNS["bullish_engulfing"](prev, last): patterns.append("Bullish Engulfing")
        if CANDLE_PATTERNS["bearish_engulfing"](prev, last): patterns.append("Bearish Engulfing")
    # Volume check
    avg_vol = sum(b.get("volume",0) for b in bars[-10:]) / 10 if len(bars) >= 10 else 0
    high_vol = last.get("volume",0) > avg_vol * 1.5 if avg_vol else False
    low_vol = last.get("volume",0) < avg_vol * 0.5 if avg_vol else False
    vol_note = ""
    if high_vol: vol_note = "📊 حجم تداول عالي (+50%) — تأكيد"
    elif low_vol: vol_note = "📊 حجم تداول منخفض (-50%) — ضعف"
    return patterns, vol_note, high_vol, low_vol

# ─── SENTIMENT ───
def analyze_sentiment(headlines):
    if not headlines or headlines[0][0] == "—": return 0, "[NEU]"
    text = "\n".join([f"{t} — {d}" for t,d in headlines if t != "—"])
    try:
        r = _ai_request(f"حلل المشاعر لهذه الأخبار المالية وأعط رقم بين -1 و +1 وكلمة (إيجابي/سلبي/محايد):\n{text}", temperature=0.1)
        resp = r.choices[0].message.content.strip()
        try:
            m = re.search(r'([+-]?\d+\.?\d*)', resp)
            score = max(-1, min(1, float(m.group(1)))) if m else 0
        except: score = 0
        label = "[POS]" if score > 0.3 else ("[NEG]" if score < -0.3 else "[NEU]")
        return score, label
    except: return 0, "[NEU]"

# ─── CORRELATION ───
ASSET_CORRELATION = {
    "EURUSD": {"GBPUSD": 0.85, "USDCHF": -0.9, "USDCAD": -0.7, "GBPJPY": 0.55},
    "GBPUSD": {"EURUSD": 0.85, "USDCHF": -0.75, "USDCAD": -0.6, "EURJPY": 0.5},
    "USDJPY": {"XAUUSD": -0.6, "USDCHF": 0.65},
    "XAUUSD": {"USDJPY": -0.6, "EURUSD": 0.4},
}
def get_correlation_warnings(asset1, asset2):
    if asset1 in ASSET_CORRELATION and asset2 in ASSET_CORRELATION[asset1]:
        corr = ASSET_CORRELATION[asset1][asset2]
        if abs(corr) > 0.7:
            return f"⚠️ تنبيه ارتباط: {asset1} / {asset2} = {corr:.0%} — لا تدخل معاً"
        elif abs(corr) > 0.5:
            return f"ℹ️ ملاحظة ارتباط: {asset1} / {asset2} = {corr:.0%} — ترابط متوسط"
    return ""

# ─── SIGNAL PARSING ───
def parse_signal(text):
    if not text: return "انتظار", "", 50, "", "", "", "", "", ""
    def ex(p):
        try:
            m = re.search(p, text, re.IGNORECASE)
            return m.group(1).replace(',','') if m else ""
        except: return ""
    rec = "انتظار"
    try:
        if re.search(r'شراء(?:\s*قوي)?', text): rec = "شراء"
        elif re.search(r'بيع(?:\s*قوي)?', text): rec = "بيع"
    except Exception: pass
    strength = ""
    try:
        if re.search(r'قوي|strong', text, re.IGNORECASE): strength = " قوي"
    except Exception: pass
    confidence = 50
    try:
        conf_m = re.search(r'(?:ثقة|confidence)[:\s]*(\d+)', text, re.IGNORECASE)
        if conf_m: confidence = int(conf_m.group(1))
    except Exception: pass
    tp = ex(r'(?:هدف|take.?profit|tp|TP)[:\s]*([\d,]+\.?\d*)')
    sl = ex(r'(?:وقف|stop.?loss|sl|SL)[:\s]*([\d,]+\.?\d*)')
    rr = ex(r'(?:risk.?reward|rr|مخاطرة|R/R)[:\s]*([\d,]+\.?\d*)')
    sup = ex(r'(?:دعم|support)[:\s]*([\d,]+\.?\d*)')
    res = ex(r'(?:مقاومة|resistance)[:\s]*([\d,]+\.?\d*)')
    reason = ""
    try:
        rm = re.search(r'(?:سبب|reason|التحليل)[:\s]*(.+?)(?:\n|$)', text, re.IGNORECASE)
        if rm: reason = rm.group(1).strip()[:120]
    except Exception: pass
    return rec, strength, confidence, tp, sl, rr, sup, res, reason

def build_signal_card(symbol, price, rsi, rec, strength, confidence, tp, sl, rr, sup, res, sentiment_label, sentiment_score, reason="", regime=None, daily_rsi=None):
    rec_label = "[BUY]" if "شراء" in rec else ("[SELL]" if "بيع" in rec else "[WAIT]")
    rec_sign = "(+)" if "شراء" in rec else ("(-)" if "بيع" in rec else "(=)")
    sep = "="*16
    lines = [f"{rec_sign} {symbol}  {rec_label}  {confidence}%", sep]
    if price: lines += [f"Price: {price}"]
    if rsi: lines += [f"RSI(15m): {rsi}"]
    if regime:
        ri = {"trending":"📈","weak_trend":"📊","ranging":"📉"}.get(regime["trend"],"📊")
        di = {"up":"↑","down":"↓","flat":"→"}.get(regime["direction"],"→")
        lines += [f"Market: {ri} {regime['trend']} {di}"]
    if daily_rsi: lines += [f"RSI(1d): {daily_rsi}"]
    sent_mark = "(+)" if sentiment_score > 0.3 else ("(-)" if sentiment_score < -0.3 else "(=)")
    lines += [f"Sentiment: {sentiment_label} {sent_mark} {sentiment_score:+.1f}", sep]
    if tp and sl: lines += [f"TP: {tp} | SL: {sl}"]
    if rr: lines += [f"R/R: {rr}"]
    if sup: lines += [f"Support: {sup}"]
    if res: lines += [f"Resist: {res}"]
    if reason:
        r = reason.replace("*","").replace("_","").replace("`","").strip()
        if r: lines += [f"* {r[:200]}"]
    return "\n".join(lines)

def dynamic_confidence(rsi_val, sentiment_score, ai_conf, rec):
    rsi_s = rsi_score(rsi_val); sent_s = (sentiment_score+1)*50; ai_s = ai_conf
    if rec == "انتظار": ai_s *= 0.5
    return max(0, min(100, int(rsi_s*0.25 + sent_s*0.25 + ai_s*0.5)))

# ─── CORE ANALYSIS ───
def auto_tp_sl(symbol, price, rec, bars):
    """Calculate TP/SL using ATR if AI didn't provide them."""
    atr = calc_atr(bars) if bars else None
    if not atr: atr = price * 0.003  # fallback 0.3%
    if "شراء" in rec:
        tp = round(price + atr * 2, 5)
        sl = round(price - atr, 5)
        rr = round(abs(tp - price) / abs(price - sl), 2) if abs(price - sl) > 0 else 1
        sup = round(price - atr * 1.5, 5)
        res = round(price + atr * 2.5, 5)
    elif "بيع" in rec:
        tp = round(price - atr * 2, 5)
        sl = round(price + atr, 5)
        rr = round(abs(tp - price) / abs(price - sl), 2) if abs(price - sl) > 0 else 1
        sup = round(price - atr * 2.5, 5)
        res = round(price + atr * 1.5, 5)
    else:
        return "", "", "", "", ""
    return str(tp), str(sl), str(rr), str(sup), str(res)

def get_active_sessions():
    now = datetime.now(timezone.utc)
    h = now.hour
    weekday = now.weekday()
    sessions = []
    if weekday >= 5:
        return ["weekend"]
    if 0 <= h < 9:
        sessions.append("tokyo")
    if 8 <= h < 17:
        sessions.append("london")
    if 13 <= h < 22:
        sessions.append("newyork")
    if "london" in sessions and "newyork" in sessions:
        sessions.append("overlap")
    return sessions if sessions else ["asia"]

def extract_lesson(signal):
    try:
        rsi_val = float(signal.get("rsi", 50))
        adx_val = float(signal.get("adx", 15)) if signal.get("adx") else None
        return {
            "asset": signal.get("asset", "?"),
            "rec": signal.get("rec", ""),
            "result": signal.get("result", ""),
            "rsi": rsi_val,
            "adx": adx_val,
            "confidence": signal.get("confidence", 50),
            "patterns": signal.get("candle_patterns", ""),
            "reason": (signal.get("reason", "") or "")[:80]
        }
    except:
        return None

def get_relevant_lessons(symbol, count=3):
    relevant = [l for l in lessons_log if l.get("asset") == symbol and l.get("result") in ("win","loss")]
    relevant.sort(key=lambda l: lessons_log.index(l), reverse=True)
    return relevant[:count]

def inject_lessons_into_prompt(prompt, symbol):
    lessons = get_relevant_lessons(symbol)
    if not lessons:
        return prompt
    lines = ["\n\n--- دروس من صفقات سابقة على هذا الأصل ---"]
    for l in lessons:
        icon = "✅" if l["result"] == "win" else "❌"
        lines.append(f"{icon} {l['rec']} (ثقة {l['confidence']}% | RSI {l['rsi']}) → {l['result']}")
        if l["reason"]:
            lines.append(f"   السبب: {l['reason'][:60]}")
    lines.append("--- استفد من هذه الدروس في تحليلك ---\n")
    return prompt + "\n".join(lines)

def _build_analysis_prompt(symbol, price, rsi, hr_rsi, daily_rsi, regime, headlines, candle_patterns, tf_note, adx_val=None, macd=None, stoch=None, pivots=None, sma_50=None, ema_20=None):
    headlines_str = '\n'.join(f'- {h}' for h in headlines)
    patterns_str = ', '.join(candle_patterns) if candle_patterns else 'لا يوجد'
    regime_str = f"{regime['trend']} / {regime['direction']} / تقلب: {regime['volatility']}"
    adx_note = f"\n- ADX: {adx_val}" if adx_val is not None else ""
    macd_note = f"\n- MACD: {macd['macd']} | Signal: {macd.get('signal',0)} | Hist: {macd.get('histogram',0)}" if macd else ""
    stoch_note = f"\n- Stochastic: {stoch}" if stoch is not None else ""
    pivot_note = f"\n- Pivot: {pivots['pp']} | R1: {pivots['r1']} R2: {pivots['r2']} | S1: {pivots['s1']} S2: {pivots['s2']}" if pivots else ""
    ma_note = f"\n- SMA50: {sma_50} | EMA20: {ema_20}" if sma_50 or ema_20 else ""

    few_shot = """أمثلة على التنسيق المطلوب:

مثال 1 - شراء قوي (تأكيد متعدد الفريمات):
{"recommendation": "شراء","confidence": 85,"tp": 1.0875,"sl": 1.0835,"risk_reward": 1.7,"support": 1.0820,"resistance": 1.0900,"reason": "RSI 15m 28 + RSI ساعي 32 = ذروة بيع في فريمين. ADX 28 (trending). ارتداد من دعم رئيسي. MACD يعطي إشارة شراء. حجم التداول مرتفع."}

مثال 2 - بيع (تباعد سلبي):
{"recommendation": "بيع","confidence": 80,"tp": 150.20,"sl": 150.80,"risk_reward": 2.0,"support": 149.50,"resistance": 151.00,"reason": "RSI 15m 75 مع تباعد سلبي على فريم الساعة. Stochastic في ذروة شراء (88). السوق trending مع ADX 32. نموذج Doji بعد قمة."}

مثال 3 - انتظار (تضارب):
{"recommendation": "انتظار","confidence": 50,"tp": null,"sl": null,"risk_reward": null,"support": null,"resistance": null,"reason": "لا توجد إشارة واضحة. RSI 54 (محايد). ADX 12 (سوق متذبذب). لا يوجد نمط شمعة واضح. MACD متقاطع عرضي."}

مثال 4 - شراء حذر (سوق متذبذب):
{"recommendation": "شراء","confidence": 60,"tp": 1.0920,"sl": 1.0900,"risk_reward": 1.2,"support": 1.0895,"resistance": 1.0930,"reason": "RSI اليومي 33 (دعم طويل المدى) لكن ADX 13 (متذبذب). نشتري قرب الدعم مع SL ضيق."}

الآن قم بتحليل البيانات التالية وأعد JSON فقط (بدون أي نص إضافي):"""

    return f"""{few_shot}

بيانات التحليل:
- الأصل: {symbol}
- السعر الحالي: {price}
- RSI (15 دقيقة): {rsi or 'N/A'}
- RSI (ساعة): {hr_rsi or 'N/A'}
- RSI (يومي): {daily_rsi or 'N/A'}
- حالة السوق: {regime_str}
- ملاحظات فنية: {tf_note}{adx_note}{macd_note}{stoch_note}{pivot_note}{ma_note}
- الأخبار: {headlines_str}
- أنماط الشموع: {patterns_str}

قواعد مهمة:
1. إذا كان RSI اليومي أقل من 35، لا توصي ببيع
2. إذا كان RSI اليومي أكبر من 65، لا توصي بشراء
3. TP وSL يجب أن يكونا ضمن 0.1% إلى 2% من السعر الحالي ({price})
4. إذا لم تكن هناك إشارة واضحة، أعد recommendation: "انتظار" ولا تخمن
5. اذكر سبباً محدداً (ماذا ترى في البيانات) وليس كلاماً عاماً
6. كلما زادت المؤشرات المتوافقة (RSI + MACD + ADX + أنماط) زادت الثقة

أعد JSON فقط:"""

def analyze(symbol, ai_timeout=30.0):
    price, err = get_price(symbol)
    if err: return f"[XX] {err}", 0, "انتظار", None, None, None, None, None, None, None, None, None, None
    sessions = get_active_sessions()
    session_str = " + ".join(sessions)
    if "weekend" in sessions:
        return f"[!] السوق مغلق (عطلة نهاية الأسبوع)", 0, "انتظار", None, None, None, None, None, None, None, None, None, None
    now = datetime.now().strftime("%d %B %Y %H:%M")
    # 15min data
    hist = get_historical(symbol, "15min", 15)
    rsi = calc_rsi(hist) if hist else None
    # 1h data
    hr_hist = get_historical(symbol, "60min", 14)
    hr_rsi = calc_rsi(hr_hist) if hr_hist else None
    # Daily data
    daily_prices, _ = yahoo_daily(symbol, 30)
    daily_rsi = calc_rsi(daily_prices) if daily_prices and len(daily_prices) >= 15 else None
    daily_bb = calc_bb(daily_prices) if daily_prices and len(daily_prices) >= 20 else None
    ohlcv_bars, _ = yahoo_ohlcv(symbol)
    # Timeframe analysis notes
    tf_note = ""
    # Candle patterns + volume
    candle_patterns = []
    vol_note = ""
    high_vol = False
    if ohlcv_bars and len(ohlcv_bars) >= 3:
        patterns_list, vol_note, high_vol, low_vol_local = detect_candle_patterns(ohlcv_bars)
        candle_patterns = patterns_list
        if vol_note: tf_note += "\n" + vol_note
    # Market Regime
    adx_result = calc_adx(ohlcv_bars) if ohlcv_bars and len(ohlcv_bars) > 15 else None
    bb_result = calc_bb([b["close"] for b in ohlcv_bars[-30:]]) if ohlcv_bars and len(ohlcv_bars) >= 20 else None
    regime = detect_regime(adx_result, daily_bb)
    # Economic calendar check
    news = get_news(symbol)
    headlines = [h for h,_ in news]
    high_impact = any(k in " ".join(headlines).upper() for k in ["NFP","CPI","FOMC","NONFARM","INTEREST RATE","FED","INFLATION","GDP","UNEMPLOYMENT","PMI","BANK OF JAPAN","ECB","SNB"])
    sentiment_score, sentiment_label = analyze_sentiment(news)
    # 15min + 1h alignment
    if rsi and hr_rsi:
        if (rsi < 30 and hr_rsi < 35): tf_note = "تأكيد: RSI منخفض في الفريمين (ذروة بيع)"
        elif (rsi > 70 and hr_rsi > 65): tf_note = "تأكيد: RSI مرتفع في الفريمين (ذروة شراء)"
        elif (rsi < 40 and hr_rsi > 50): tf_note = "تباين: RSI لحظي هابط لكن ساعي صاعد ⚠️"
        elif (rsi > 60 and hr_rsi < 50): tf_note = "تباين: RSI لحظي صاعد لكن ساعي هابط ⚠️"
    # Daily alignment
    if daily_rsi:
        if daily_rsi < 35: tf_note += "\n📅 اليومي: ذروة بيع (دعم طويل المدى)"
        elif daily_rsi > 65: tf_note += "\n📅 اليومي: ذروة شراء (مقاومة طويل المدى)"
        else: tf_note += f"\n📅 اليومي: RSI {daily_rsi} (محايد)"
    # Regime info
    regime_icons = {"trending": "📈", "weak_trend": "📊", "ranging": "📉"}
    direction_icons = {"up": "↑", "down": "↓", "flat": "→"}
    tf_note += f"\n{regime_icons.get(regime['trend'],'📊')} السوق: {regime['trend']} {direction_icons.get(regime['direction'],'→')} ({regime['strength']})"
    if regime["volatility"] == "high": tf_note += "\n⚠️ تقلب عالي — تشتت كبير"
    if high_impact: tf_note += "\n⚠️ تنبيه: اليوم فيه أخبار اقتصادية قوية!"
    adx_val = adx_result[0] if adx_result else None
    # ─── New indicators ───
    daily_prices_list = daily_prices or []
    macd = calc_macd(daily_prices_list) if len(daily_prices_list) >= 26 else None
    stoch = calc_stochastic(ohlcv_bars) if ohlcv_bars and len(ohlcv_bars) >= 14 else None
    pivots = calc_pivot_points(ohlcv_bars) if ohlcv_bars and len(ohlcv_bars) >= 2 else None
    sma_50 = calc_sma(daily_prices_list, 50) if len(daily_prices_list) >= 50 else None
    ema_20 = calc_ema(daily_prices_list, 20) if len(daily_prices_list) >= 21 else None
    try:
        tech_text = ""
        # All prompts
        tech_prompt = _build_analysis_prompt(symbol, price, rsi, hr_rsi, daily_rsi, regime, headlines, candle_patterns, tf_note, adx_val, macd, stoch, pivots, sma_50, ema_20)
        tech_prompt = inject_lessons_into_prompt(tech_prompt, symbol)
        sent_prompt = f"حلل المشاعر للأخبار التالية وأعط رقماً بين -1 و +1 (بدون تفسير):\n{chr(10).join('- '+h for h in headlines)}"

        provider_names = {p["name"]: i for i, p in enumerate(AI_PROVIDERS)}

        # Step 1: Independent analysis from BOTH providers (Multi-AI)
        tech_texts = {}; tech_recs = {}
        for attempt in range(2):
            for tp in ["groq", "minimax"]:
                if tp in provider_names:
                    try:
                        r1 = _ai_request(tech_prompt, temperature=0.1, provider_idx=provider_names[tp], timeout=ai_timeout)
                        txt = r1.choices[0].message.content.strip()
                        tech_texts[tp] = txt
                        try:
                            clean = re.sub(r'```(?:json)?\s*', '', txt).strip()
                            p = json.loads(clean) if clean.startswith('{') else json.loads(re.search(r'\{.*\}', clean, re.DOTALL).group())
                            tech_recs[tp] = p.get("recommendation", "انتظار")
                        except:
                            tech_recs[tp] = "انتظار"
                    except Exception as e:
                        logging.warning(f"Tech AI ({tp}) failed: {e}")
                        continue
            if tech_texts: break
            time.sleep(4)
        # Multi-AI confirmation: if both agree, boost confidence; if disagree, be conservative
        groq_rec = tech_recs.get("groq", "")
        mm_rec = tech_recs.get("minimax", "")
        tech_text = tech_texts.get("groq") or tech_texts.get("minimax") or ""
        multi_agreement = False
        if groq_rec and mm_rec:
            groq_dir = "شراء" if "شراء" in groq_rec else ("بيع" if "بيع" in groq_rec else "انتظار")
            mm_dir = "شراء" if "شراء" in mm_rec else ("بيع" if "بيع" in mm_rec else "انتظار")
            multi_agreement = groq_dir == mm_dir

        # Step 2: Sentiment (try MiniMax, fallback to Groq)
        sent_text = ""
        for attempt2 in range(2):
            for sp in ["minimax", "groq"]:
                if sp in provider_names:
                    try:
                        r2 = _ai_request(sent_prompt, temperature=0.1, provider_idx=provider_names[sp], timeout=ai_timeout)
                        sent_resp = r2.choices[0].message.content.strip()
                        s_match = re.search(r'([+-]?\d+\.?\d*)', sent_resp)
                        if s_match:
                            sentiment_score = max(-1, min(1, float(s_match.group(1))))
                            sentiment_label = "[POS]" if sentiment_score > 0.3 else ("[NEG]" if sentiment_score < -0.3 else "[NEU]")
                        sent_text = sent_resp
                        break
                    except:
                        continue
            if sent_text: break
            time.sleep(3)

        # Step 3: Decision (Groq, with Multi-AI context)
        if "groq" in provider_names and tech_text:
            agreement_note = ""
            if groq_rec and mm_rec:
                if multi_agreement:
                    agreement_note = f"\n✅ كلا المزودين (Groq + MiniMax) متفقان على {groq_dir}"
                else:
                    agreement_note = f"\n⚠️ تعارض: Groq يقول {groq_rec} و MiniMax يقول {mm_rec} — كن حذراً"
            decision_prompt = f"""لديك تحليلين للأصل {symbol}:

التحليل الفني (Groq):
{tech_texts.get('groq','')[:1000]}
التحليل الفني (MiniMax):
{tech_texts.get('minimax','')[:1000]}{agreement_note}

تحليل المشاعر:
{sent_text if sent_text else f"{sentiment_label} ({sentiment_score:+.1f})"}

البيانات الأصلية:
- السعر: {price}
- RSI 15m: {rsi or 'N/A'}
- RSI 1h: {hr_rsi or 'N/A'}
- RSI يومي: {daily_rsi or 'N/A'}
- حالة السوق: {regime['trend']} / {regime['direction']}
- الجلسة: {session_str}

بناءً على التحليلين معاً، اتخذ قراراً نهائياً. أجب بصيغة JSON فقط:
{{"recommendation": "شراء/بيع/انتظار", "confidence": 85, "tp": {price}, "sl": {price}, "risk_reward": 2.0, "support": {price},"resistance": {price},"reason": "..."}}"""
            for attempt3 in range(2):
                try:
                    r3 = _ai_request(decision_prompt, temperature=0.1, provider_idx=provider_names["groq"], timeout=ai_timeout)
                    ai = r3.choices[0].message.content.strip()
                    break
                except:
                    if attempt3 == 0:
                        time.sleep(3)
                        continue
                    ai = tech_text
        else:
            ai = tech_text

        strength = ""
        try:
            clean = re.sub(r'```(?:json)?\s*', '', ai).strip()
            parsed = json.loads(clean) if clean.startswith('{') else json.loads(re.search(r'\{.*\}', clean, re.DOTALL).group())
            rec = parsed.get("recommendation", "انتظار")
            ai_conf = int(parsed.get("confidence", 50))
            tp = str(parsed.get("tp") or "")
            sl = str(parsed.get("sl") or "")
            rr = str(parsed.get("risk_reward") or "")
            sup = str(parsed.get("support") or "")
            res = str(parsed.get("resistance") or "")
            reason = (parsed.get("reason") or "")[:120]
        except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
            logging.warning(f"JSON parse failed for {symbol}, falling back to regex")
            rec, strength, ai_conf, tp, sl, rr, sup, res, reason = parse_signal(ai)

        # Multi-AI: if providers disagree and rec is not WAIT, lower confidence
        if groq_rec and mm_rec and not multi_agreement and rec not in ("انتظار", "WAIT"):
            ai_conf = int(ai_conf * 0.6)
            reason = (reason + " [تعارض المزودين]").strip()

        # Provider-based confidence adjustment
        try:
            d = json.loads(generate_data_json())
            pa = d.get("behavior", {}).get("provider_accuracy", {})
            for pname, pstats in pa.items():
                if pname in provider_names and pstats.get("total", 0) >= 3:
                    rate = pstats.get("rate", 50)
                    if rate < 35:
                        ai_conf = int(ai_conf * 0.7)
                        reason = (reason + f" [{pname}: {rate}%]").strip()
                    elif rate > 65:
                        ai_conf = min(100, ai_conf + 5)
        except:
            pass

        if daily_rsi is not None and rec not in ("انتظار", "WAIT"):
            if "شراء" in rec and daily_rsi > 65:
                ai_conf = int(ai_conf * 0.6)
                reason = (reason + " [تحذير: RSI اليومي مرتفع]").strip()
            elif "بيع" in rec and daily_rsi < 35:
                ai_conf = int(ai_conf * 0.6)
                reason = (reason + " [تحذير: RSI اليومي منخفض]").strip()

        if adx_result and adx_result[0] < 15 and rec not in ("انتظار", "WAIT"):
            ai_conf = int(ai_conf * 0.7)
            reason = (reason + " [سوق متذبذب]").strip()

        if price and tp:
            try: tp_f = float(tp); p_f = float(price)
            except: tp_f = 0
            if tp_f and abs(tp_f/p_f - 1) > 0.5: tp = ""
        if price and sl:
            try: sl_f = float(sl)
            except: sl_f = 0
            if sl_f and abs(sl_f/float(price) - 1) > 0.5: sl = ""
        if price and sup:
            try: sup_f = float(sup)
            except: sup_f = 0
            if sup_f and abs(sup_f/float(price) - 1) > 0.5: sup = ""
        if price and res:
            try: res_f = float(res)
            except: res_f = 0
            if res_f and abs(res_f/float(price) - 1) > 0.5: res = ""
        if (not tp or not sl) and price:
            if rec not in ("انتظار", "WAIT"):
                tp2, sl2, rr2, sup2, res2 = auto_tp_sl(symbol, float(price), rec, ohlcv_bars)
                if not tp: tp = str(tp2)
                if not sl: sl = str(sl2)
                if not rr: rr = str(rr2)
                if not sup: sup = str(sup2)
                if not res: res = str(res2)
            else:
                # WAIT: show ATR-based reference levels
                atr_ref = calc_atr(ohlcv_bars) if ohlcv_bars else None
                if not atr_ref: atr_ref = float(price) * 0.003
                pf = float(price)
                if not tp: tp = str(round(pf + atr_ref * 2, 5))
                if not sl: sl = str(round(pf - atr_ref, 5))
                if not rr: rr = str(round(abs(float(tp) - pf) / abs(pf - float(sl)), 2) if abs(pf - float(sl)) > 0 else 1)
                if not sup: sup = str(round(pf - atr_ref * 2, 5))
                if not res: res = str(round(pf + atr_ref * 2, 5))

        conf = dynamic_confidence(rsi, sentiment_score, ai_conf, rec)
        if regime["trend"] == "trending" and rec not in ("انتظار", "WAIT"):
            conf = min(100, conf + 10)
        elif regime["trend"] == "ranging" and rec not in ("انتظار", "WAIT"):
            conf = max(20, conf - 15)
        if regime["volatility"] == "high":
            conf = max(10, conf - 10)
        if daily_rsi and rsi and rec not in ("انتظار", "WAIT"):
            if (daily_rsi < 40 and "شراء" in rec) or (daily_rsi > 60 and "بيع" in rec):
                conf = min(100, conf + 8)
        if high_vol and rec not in ("انتظار", "WAIT"):
            conf = min(100, conf + 5)

        card = build_signal_card(symbol, price, rsi, rec, strength, conf, tp, sl, rr, sup, res, sentiment_label, sentiment_score, reason, regime=regime, daily_rsi=daily_rsi)
        cp_str = ', '.join(candle_patterns) if candle_patterns else ""
        return card, conf, rec, price, tp, sl, rr, sup, res, rsi, sentiment_label, sentiment_score, reason, cp_str, vol_note, adx_val, session_str
    except Exception as e:
        logging.error(f"AI: {e}")
        return f"[!] Analysis failed {symbol}", 0, "WAIT", None, None, None, None, None, None, None, None, None, None, "", "", None, ""

def fast_analyze(symbol):
    """Quick technical-only analysis for Telegram bot (no AI, instant)."""
    price, err = get_price(symbol)
    if err:
        # Use price cache as fallback
        if symbol in price_cache:
            price = price_cache[symbol]
        else:
            return f"[XX] {err}", 0, "WAIT", None, None, None, None, None, None, None, None, None, None
    sessions = get_active_sessions()
    session_str = " + ".join(sessions)
    if "weekend" in sessions:
        return f"[!] السوق مغلق (عطلة نهاية الأسبوع)", 0, "انتظار", None, None, None, None, None, None, None, None, None, None
    hist = get_historical(symbol, "15min", 15)
    rsi = calc_rsi(hist) if hist else None
    hr_hist = get_historical(symbol, "60min", 14)
    hr_rsi = calc_rsi(hr_hist) if hr_hist else None
    daily_prices, _ = yahoo_daily(symbol, 30)
    daily_rsi = calc_rsi(daily_prices) if daily_prices and len(daily_prices) >= 15 else None
    ohlcv_bars, _ = yahoo_ohlcv(symbol)
    candle_patterns = []; vol_note = ""
    if ohlcv_bars and len(ohlcv_bars) >= 3:
        patterns_list, vol_note, hv, lv = detect_candle_patterns(ohlcv_bars)
        candle_patterns = patterns_list
    adx_result = calc_adx(ohlcv_bars) if ohlcv_bars and len(ohlcv_bars) > 15 else None
    bb_result = calc_bb([b["close"] for b in ohlcv_bars[-30:]]) if ohlcv_bars and len(ohlcv_bars) >= 20 else None
    regime = detect_regime(adx_result, bb_result)
    news = get_news(symbol)
    headlines = [h for h,_ in news]
    high_impact = any(k in " ".join(headlines).upper() for k in ["NFP","CPI","FOMC","NONFARM","INTEREST RATE","FED","INFLATION","GDP","PMI","BANK OF JAPAN","ECB","SNB"])
    # Rule-based recommendation (no AI)
    rec = "انتظار"
    reason_parts = []
    if rsi is not None:
        if rsi < 30: rec = "شراء"; reason_parts.append(f"RSI {rsi} ذروة بيع")
        elif rsi > 70: rec = "بيع"; reason_parts.append(f"RSI {rsi} ذروة شراء")
    if hr_rsi is not None and rec == "انتظار":
        if hr_rsi < 30: rec = "شراء"; reason_parts.append(f"RSI ساعي {hr_rsi} ذروة بيع")
        elif hr_rsi > 70: rec = "بيع"; reason_parts.append(f"RSI ساعي {hr_rsi} ذروة شراء")
    if daily_rsi is not None and rec == "انتظار":
        if daily_rsi < 30: rec = "شراء"; reason_parts.append(f"RSI يومي {daily_rsi} قاع")
        elif daily_rsi > 70: rec = "بيع"; reason_parts.append(f"RSI يومي {daily_rsi} قمة")
    if regime["trend"] == "trending" and rec != "انتظار":
        reason_parts.append(f"اتجاه {regime['direction']}")
    if candle_patterns:
        reason_parts.append(f"نمط: {', '.join(candle_patterns[:2])}")
    if vol_note:
        reason_parts.append(vol_note)
    if high_impact:
        reason_parts.append("⚠️ أخبار قوية اليوم")
    if not reason_parts:
        reason_parts.append(f"RSI {rsi or 'N/A'} محايد | سوق {regime['trend']} {regime['direction']}")
    reason = " | ".join(reason_parts)[:200]
    # ATR-based TP/SL for ALL signals (including WAIT as reference)
    atr_val = None
    if ohlcv_bars and len(ohlcv_bars) > 1:
        atr_val = calc_atr(ohlcv_bars)
    if not atr_val: atr_val = float(price) * 0.003
    atr_val = float(atr_val)
    price_f = float(price)
    if "شراء" in rec:
        tp = round(price_f + atr_val * 2, 5)
        sl = round(price_f - atr_val, 5)
        rr = round((tp - price_f) / (price_f - sl), 2) if (price_f - sl) > 0 else 1
        sup = round(price_f - atr_val * 1.5, 5)
        res = round(price_f + atr_val * 2.5, 5)
    elif "بيع" in rec:
        tp = round(price_f - atr_val * 2, 5)
        sl = round(price_f + atr_val, 5)
        rr = round((price_f - tp) / (sl - price_f), 2) if (sl - price_f) > 0 else 1
        sup = round(price_f - atr_val * 1.5, 5)
        res = round(price_f + atr_val * 2.5, 5)
    else:
        # WAIT: show reference levels
        tp = round(price_f + atr_val * 2, 5)
        sl = round(price_f - atr_val, 5)
        rr = round((tp - price_f) / (price_f - sl), 2) if (price_f - sl) > 0 else 1
        sup = round(price_f - atr_val * 2, 5)
        res = round(price_f + atr_val * 2, 5)
    tp = str(tp); sl = str(sl); rr = str(rr); sup = str(sup); res = str(res)
    # Confidence based on indicators
    rsi_s = rsi_score(rsi) if rsi is not None else 50
    conf = int(rsi_s * 0.5 + 50 * 0.5)
    if rec != "انتظار" and regime["trend"] == "trending": conf = min(95, conf + 15)
    if high_impact: conf = max(30, conf - 10)
    sentiment_score, sentiment_label = 0, "[NEU]"
    card = build_signal_card(symbol, price, rsi, rec, "", conf, tp, sl, rr, sup, res, sentiment_label, sentiment_score, reason, regime=regime, daily_rsi=daily_rsi)
    return card, conf, rec, price, tp, sl, rr, sup, res, rsi, sentiment_label, sentiment_score, reason, ', '.join(candle_patterns), vol_note, adx_result[0] if adx_result else None, session_str

def analyze_web(symbol):
    res = analyze(symbol)
    return {"asset": symbol, "card": res[0], "confidence": res[1], "rec": res[2], "price": res[3]}

def market_scan():
    global _SCAN_CACHE, _SCAN_CACHE_TS
    now = time.time()
    if _SCAN_CACHE and (now - _SCAN_CACHE_TS) < _SCAN_CACHE_TTL:
        return _SCAN_CACHE
    results = []
    # Weekend check: skip heavy indicator calls, just prices
    weekday = datetime.now().weekday()
    is_weekend = weekday >= 5
    for asset in RADAR_ASSETS:
        try:
            price, err = get_price(asset)
            if err: continue
            if is_weekend:
                results.append({"asset": asset, "price": price, "rsi_15m": None, "rsi_1h": None, "adx": None, "pattern": "", "trend": "weekend", "vol_high": False})
                continue
            hist = get_historical(asset, "15min", 15)
            rsi = calc_rsi(hist) if hist else None
            hr_hist = get_historical(asset, "60min", 14)
            hr_rsi = calc_rsi(hr_hist) if hr_hist else None
            ohlcv_bars, _ = yahoo_ohlcv(asset)
            adx_res = calc_adx(ohlcv_bars) if ohlcv_bars and len(ohlcv_bars) > 15 else None
            adx_val = adx_res[0] if adx_res else None
            patterns_list, vol_note, hv, lv = detect_candle_patterns(ohlcv_bars) if ohlcv_bars and len(ohlcv_bars) >= 3 else ([], "", False, False)
            trend = "ranging"
            if adx_res:
                if adx_res[0] >= 25: trend = "trending"
                elif adx_res[0] >= 15: trend = "weak"
            results.append({"asset": asset, "price": price, "rsi_15m": rsi, "rsi_1h": hr_rsi, "adx": adx_val, "pattern": ', '.join(patterns_list) if patterns_list else "", "trend": trend, "vol_high": hv})
        except: continue
    _SCAN_CACHE = results; _SCAN_CACHE_TS = time.time()
    return results

def ai_chat(question, history=None):
    ctx = "\n".join([f"{m['role']}: {m['content']}" for m in (history or [])[-6:]])
    prompt = f"""أنت مساعد AURA CLOUD، خبير في التداول والتحليل الفني للعملات والأسهم.
أجب بالعربية الفصحى وباختصار (جملة إلى 3 جمل).

السياق السابق:
{ctx}

السؤال: {question}

الإجابة:"""
    for attempt in range(3):
        try:
            r = _ai_request(prompt, temperature=0.3, timeout=20.0)
            return r.choices[0].message.content.strip()[:500], None
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "rate limit" in err:
                time.sleep(3)
                continue
            if attempt < 2:
                time.sleep(2)
                continue
            return "", str(e)
    return "", "الذكاء الاصطناعي مشغول حالياً، حاول مرة أخرى"

def generate_journal():
    checked = [s for s in signal_log if s["status"] == "checked"]
    if not checked:
        return "لا توجد صفقات مكتملة بعد لعمل تقرير."
    wins = sum(1 for s in checked if s["result"] == "win")
    losses = len(checked) - wins
    wr = round(wins/len(checked)*100,1)
    entered = [s for s in signal_log if s.get("user_entered")]
    best = max(entered, key=lambda s: abs(float(s.get("rr",0) or 0))) if entered else None
    worst = min(entered, key=lambda s: abs(float(s.get("rr",0) or 0))) if entered else None
    prompt = f"""أكتب تقرير أسبوعي قصير (3-4 جمل) بالعربية عن أداء التداول:
إجمالي الصفقات: {len(checked)}
ربح: {wins} | خسارة: {losses}
Win Rate: {wr}%
أفضل صفقة: {best['asset'] if best else 'N/A'} (R:R {best['rr'] if best else 'N/A'})
أسوأ صفقة: {worst['asset'] if worst else 'N/A'} (R:R {worst['rr'] if worst else 'N/A'})
أعط توصيات للتحسين."""
    try:
        r = _ai_request(prompt, temperature=0.3)
        return r.choices[0].message.content.strip()[:500]
    except:
        return f"تقرير {len(checked)} صفقات | {wr}% win rate | أفضل: {best['asset'] if best else 'N/A'}"

def get_economic_calendar():
    try:
        r = requests.get("https://economic-calendar.tradingview.com/events?from=" + datetime.now().strftime("%Y-%m-%d") + "&to=" + (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d"), headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        data = r.json()
        events = []
        for item in data.get("result", [])[:20]:
            events.append({
                "date": item.get("date", ""),
                "title": item.get("title", ""),
                "country": item.get("country", ""),
                "impact": item.get("impact", "low"),
                "forecast": item.get("forecast", ""),
                "previous": item.get("previous", "")
            })
        return events
    except:
        return []

# ─── SIGNAL LOG ───
def calc_pnl(entry_price, exit_price, rec, position_size=None):
    pos_size = position_size or DEFAULT_POSITION_SIZE
    ep = float(entry_price); xp = float(exit_price)
    if "شراء" in rec:
        pnl_pct = (xp - ep) / ep * 100
    else:
        pnl_pct = (ep - xp) / ep * 100
    pnl_usd = round(pnl_pct / 100 * pos_size, 2)
    return round(pnl_pct, 2), pnl_usd

def log_signal(symbol, rec, confidence, price, tp, sl, rr, sup, res, rsi, sentiment_label, sentiment_score, reason, sig_id=None, candle_patterns="", vol_note="", adx=None, session=""):
    entry = {"id": sig_id or f"{symbol}_{int(time.time())}", "time": datetime.now(timezone.utc).isoformat(), "asset": symbol, "rec": rec,
             "confidence": confidence, "price": price,
             "tp": tp or "", "sl": sl or "", "rr": rr or "", "support": sup or "", "resistance": res or "",
             "rsi": rsi, "sentiment_label": sentiment_label or "", "sentiment_score": sentiment_score or 0, "reason": reason or "",
             "status": "pending", "result_price": None, "result": None, "checked_at": None,
             "user_entered": False, "user_skipped": False, "user_confirmed_at": None,
             "notes": "", "tags": [], "entry_price": None, "exit_price": None, "exit_reason": "",
             "ai_review": "", "pnl_pct": None, "pnl_usd": None, "position_size": DEFAULT_POSITION_SIZE,
             "candle_patterns": candle_patterns,
             "vol_note": vol_note,
             "breakeven_set": False,
             "provider_used": AI_PROVIDERS[_current_provider]["name"] if AI_PROVIDERS else "unknown",
             "adx": adx,
             "session": session}
    with signal_lock:
        signal_log.append(entry)
        if len(signal_log) > 200: signal_log[:] = signal_log[-200:]
        save_json('signals.json', signal_log); _invalidate_cache()
    archive_append(entry)
    return entry["id"]

def check_pending_signals():
    now = datetime.now(timezone.utc); checked = 0
    with signal_lock:
        for s in signal_log:
            if s["status"] != "pending": continue
            try:
                t = datetime.fromisoformat(s["time"])
                age_hours = (now - t).total_seconds() / 3600
                checked_sig = False
                if s.get("tp") and s.get("sl") and s.get("price") and age_hours > 0.15:
                    try:
                        tp_f, sl_f, entry = float(s["tp"]), float(s["sl"]), float(s["price"])
                    except: tp_f = sl_f = 0
                    if tp_f and sl_f:
                        bars, err = yahoo_ohlcv(s["asset"])
                        if bars and not err:
                            for b in bars:
                                if s["rec"] == "شراء":
                                    if b["high"] >= tp_f:
                                        s["status"] = "checked"; s["result"] = "win"; s["result_price"] = round(b["high"],5); s["checked_at"] = now.isoformat(); checked += 1; checked_sig = True; break
                                    if b["low"] <= sl_f:
                                        s["status"] = "checked"; s["result"] = "loss"; s["result_price"] = round(b["low"],5); s["checked_at"] = now.isoformat(); checked += 1; checked_sig = True; break
                                elif s["rec"] == "بيع":
                                    if b["low"] <= tp_f:
                                        s["status"] = "checked"; s["result"] = "win"; s["result_price"] = round(b["low"],5); s["checked_at"] = now.isoformat(); checked += 1; checked_sig = True; break
                                    if b["high"] >= sl_f:
                                        s["status"] = "checked"; s["result"] = "loss"; s["result_price"] = round(b["high"],5); s["checked_at"] = now.isoformat(); checked += 1; checked_sig = True; break
                if not checked_sig and age_hours > 1:
                    price, err = get_price(s["asset"])
                    if not err and s["price"]:
                        current = float(price); entry = float(s["price"])
                        if s["rec"] == "شراء": result = "win" if current > entry else "loss"
                        elif s["rec"] == "بيع": result = "win" if current < entry else "loss"
                        else: result = "neutral"
                        s["status"] = "checked"; s["result"] = result; s["result_price"] = current; s["checked_at"] = now.isoformat()
                        checked += 1
            except Exception as e:
                logging.warning(f"check_pending: {s.get('id','?')}: {e}")
                continue
        if checked:
            save_json('signals.json', signal_log); _invalidate_cache()
            for s in signal_log:
                if s.get("status") == "checked" and s.get("result"):
                    archive_append(s)
    return checked

def _send_tg(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": RADAR_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=5)
    except Exception as e:
        logging.warning(f"_send_tg: {e}")

def track_active_trades():
    """Monitor entered trades and auto-close when TP/SL hit."""
    now = datetime.now(timezone.utc)
    closed = 0
    with signal_lock:
        for s in signal_log:
            if not s.get("user_entered") or s.get("exit_price"): continue
            if s.get("status") == "checked": continue
            asset = s.get("asset","")
            tp = s.get("tp",""); sl = s.get("sl","")
            if not tp and not sl: continue
            try:
                price_now, err = get_price(asset)
                if err: continue
                p = float(price_now)
                tp_f = float(tp) if tp else None
                sl_f = float(sl) if sl else None
                entry = float(s.get("entry_price") or s.get("price", 0))
                rec = s.get("rec","")
                hit = False; hit_type = ""; result = ""

                # ─── Dynamic Risk Management ───
                ohlcv_bars_curr, _ = yahoo_ohlcv(asset)
                atr = calc_atr(ohlcv_bars_curr) if ohlcv_bars_curr else None
                if atr and entry > 0:
                    atr_dist = (p - entry) / atr if "شراء" in rec else (entry - p) / atr
                    # Breakeven if moved 1 ATR in favor
                    if atr_dist >= 1.0 and not s.get("breakeven_set"):
                        if "شراء" in rec:
                            s["sl"] = str(round(entry, 5))
                        else:
                            s["sl"] = str(round(entry, 5))
                        s["breakeven_set"] = True
                        s["notes"] = (s.get("notes","") + " [BE: وقف التعادل]").strip()
                        logging.info(f"Breakeven set for {s.get('id','?')} at {entry}")
                    # Trailing stop if moved 2 ATR in favor
                    if atr_dist >= 2.0:
                        trail_sl = p - atr if "شراء" in rec else p + atr
                        new_sl = float(s.get("sl", 0) or entry)
                        if ("شراء" in rec and trail_sl > new_sl) or ("بيع" in rec and trail_sl < new_sl):
                            s["sl"] = str(round(trail_sl, 5))
                            s["notes"] = (s.get("notes","") + " [TS: وقف متحرك]").strip()
                            logging.info(f"Trailing stop updated for {s.get('id','?')} to {trail_sl}")
                    sl_f = float(s.get("sl", sl or "0")) or None

                if "شراء" in rec:
                    if tp_f and p >= tp_f:
                        s["exit_price"] = str(round(tp_f,5)); s["exit_reason"] = "tp_hit"; s["result"] = "win"; hit = True; hit_type = "🎯 TP"; result = "🏆 *ربح*"
                    elif sl_f and p <= sl_f:
                        s["exit_price"] = str(round(sl_f,5)); s["exit_reason"] = "sl_hit"; s["result"] = "loss"; hit = True; hit_type = "🛑 SL"; result = "💥 *خسارة*"
                elif "بيع" in rec:
                    if tp_f and p <= tp_f:
                        s["exit_price"] = str(round(tp_f,5)); s["exit_reason"] = "tp_hit"; s["result"] = "win"; hit = True; hit_type = "🎯 TP"; result = "🏆 *ربح*"
                    elif sl_f and p >= sl_f:
                        s["exit_price"] = str(round(sl_f,5)); s["exit_reason"] = "sl_hit"; s["result"] = "loss"; hit = True; hit_type = "🛑 SL"; result = "💥 *خسارة*"
                if hit:
                    ep = float(s.get("entry_price") or s.get("price", 0))
                    pnl_pct, pnl_usd = calc_pnl(ep, s["exit_price"], rec, s.get("position_size"))
                    s["pnl_pct"] = pnl_pct; s["pnl_usd"] = pnl_usd
                    s["status"] = "checked"; s["result_price"] = s["exit_price"]; s["checked_at"] = now.isoformat(); closed += 1
                    sign = "+" if pnl_usd >= 0 else ""
                    msg = f"{result}\n━━━━━━━━━━\n📊 *{asset}*\n💰 الدخول: {s.get('price','')}\n🚪 الخروج: {s['exit_price']}\n{hit_type}\n📊 P&L: {sign}${pnl_usd} ({sign}{pnl_pct}%)\n📈 الثقة: {s.get('confidence','')}%"
                    _send_tg(msg)
                    lesson = extract_lesson(s)
                    if lesson:
                        lessons_log.append(lesson)
                        if len(lessons_log) > MAX_LESSONS: lessons_log[:] = lessons_log[-MAX_LESSONS:]
                        save_json('lessons.json', lessons_log)
            except Exception as e:
                logging.warning(f"track_active: {s.get('id','?')}: {e}")
                continue
        if closed:
            save_json('signals.json', signal_log); _invalidate_cache()
            for s in signal_log:
                if s.get("status") == "checked" and s.get("result"):
                    archive_append(s)
            logging.info(f"Auto-closed {closed} active trade(s)")
    return closed

# ─── SMART RADAR ───
async def radar_task(context):
    global RADAR_ENABLED, last_signals
    if not RADAR_ENABLED: return
    for asset in RADAR_ASSETS:
        try:
            res = await asyncio.get_event_loop().run_in_executor(None, analyze, asset)
            result, confidence, rec, price = res[0], res[1], res[2], res[3]
            sig = {"time": datetime.now(timezone.utc).strftime("%H:%M:%S"), "asset": asset,
                   "analysis": result, "confidence": confidence, "rec": rec}
            last_signals.append(sig)
            if len(last_signals) > 50: last_signals[:] = last_signals[-50:]
            if confidence >= MIN_CONFIDENCE and rec not in ("WAIT", "انتظار") and price and price != "--":
                sig_id = log_signal(asset, rec, confidence, price, res[4], res[5], res[6], res[7], res[8], res[9], res[10], res[11], res[12], candle_patterns=res[13] if len(res)>13 else "", vol_note=res[14] if len(res)>14 else "", adx=res[15] if len(res)>15 else None, session=res[16] if len(res)>16 else "")
                if RADAR_CHAT_ID:
                    kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ دخلت الصفقة", callback_data=f"enter_{sig_id}"),
                         InlineKeyboardButton("❌ تخطيت", callback_data=f"skip_{sig_id}")]
                    ])
                    await context.bot.send_message(chat_id=RADAR_CHAT_ID, text=result, reply_markup=kb)
            await asyncio.sleep(10)
        except Exception as e: logging.error(f"Radar: {asset} {e}")
    await asyncio.get_event_loop().run_in_executor(None, check_pending_signals)

# ─── PRICE ALERTS ───
async def check_alerts(context):
    global price_alerts
    if not price_alerts: return
    to_remove = []
    with alerts_lock:
        for alert in price_alerts:
            price, err = get_price(alert["asset"])
            if err: continue
            target = alert["price"]; is_above = alert.get("above", True)
            if (price >= target) if is_above else (price <= target):
                try:
                    await context.bot.send_message(chat_id=alert["user_id"],
                        text=f"[BELL] *تنبيه سعري*\n{'[UP]' if is_above else '[DOWN]'} {alert['asset']} وصل `{price}`\n[TARGET] الهدف: `{target}`", parse_mode="Markdown")
                except Exception as e:
                    logging.warning(f"check_alerts send: {e}")
                to_remove.append(alert)
        for a in to_remove: price_alerts.remove(a)
        if to_remove: save_json('alerts.json', price_alerts)

# ─── BACKTEST ───
def backtest(symbol, days=7, rsi_period=14, oversold=30, overbought=70, tp_mult=2, sl_mult=1):
    f = symbol[:3]+'/'+symbol[3:] if len(symbol)==6 and '/' not in symbol else symbol
    d, err = twelvedata_get('time_series', {'symbol': f, 'interval': '1day', 'outputsize': days})
    if err or 'values' not in d: return None, err or "No data"
    vals = d['values']
    if len(vals) < rsi_period + 2: return None, "بيانات غير كافية"
    closes = [float(v['close']) for v in vals]
    highs = [float(v['high']) for v in vals]
    lows = [float(v['low']) for v in vals]
    rsi_vals = []
    for i in range(len(closes)):
        if i < rsi_period: rsi_vals.append(None)
        else:
            window = closes[i-rsi_period:i+1]
            rsi_vals.append(calc_rsi(window, rsi_period))
    trades = []
    balance = 1000
    in_trade = False
    entry_price = 0
    entry_idx = 0
    for i in range(rsi_period, len(closes)):
        r = rsi_vals[i]
        if r is None: continue
        if not in_trade:
            if r <= oversold:
                in_trade = True
                entry_price = closes[i]
                entry_idx = i
            elif r >= overbought:
                in_trade = True
                entry_price = closes[i]
                entry_idx = i
        else:
            is_long = rsi_vals[entry_idx] <= oversold if entry_idx < len(rsi_vals) and rsi_vals[entry_idx] is not None else True
            tp = entry_price + (entry_price * tp_mult * 0.01) if is_long else entry_price - (entry_price * tp_mult * 0.01)
            sl = entry_price - (entry_price * sl_mult * 0.01) if is_long else entry_price + (entry_price * sl_mult * 0.01)
            hit_tp = highs[i] >= tp if is_long else lows[i] <= tp
            hit_sl = lows[i] <= sl if is_long else highs[i] >= sl
            exit_reason = ""
            if hit_tp:
                pnl_pct = tp_mult
                balance *= (1 + pnl_pct/100)
                exit_reason = "TP"
            elif hit_sl:
                pnl_pct = -sl_mult
                balance *= (1 + pnl_pct/100)
                exit_reason = "SL"
            elif i == len(closes) - 1:
                pnl_pct = (closes[i] - entry_price) / entry_price * 100 if is_long else (entry_price - closes[i]) / entry_price * 100
                balance *= (1 + pnl_pct/100)
                exit_reason = "timeout"
            else: continue
            trades.append({"entry_idx": entry_idx, "exit_idx": i, "entry_price": round(entry_price,5), "exit_price": round(closes[i],5), "pnl_pct": round(pnl_pct,2), "reason": exit_reason, "direction": "buy" if is_long else "sell"})
            in_trade = False
    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    losses = len(trades) - wins
    return {"asset": symbol, "days": days, "trades": len(trades), "wins": wins, "losses": losses, "win_rate": round(wins/len(trades)*100,1) if trades else 0, "final_balance": round(balance,2), "roi": round((balance-1000)/1000*100,1), "trade_log": trades[-20:]}, None

# ─── MONITORING PAGE ───
def generate_monitor_html():
    try:
        with open('monitor.html', 'r', encoding='utf-8') as f:
            return f.read()
    except:
        return """<!DOCTYPE html><html lang="ar" dir="rtl"><head><meta charset="UTF-8"><title>AURA CLOUD</title></head><body><h1>= AURA CLOUD</h1><p style="color:#666">صفحة المراقبة قيد التحميل...</p><script>window.location.href="https://mohammedbesmouch-byte.github.io/aura-monitor/"</script></body></html>"""

# ─── GITHUB PUSH ───
def push_to_github(filename, content_bytes, commit_msg):
    if not GITHUB_TOKEN:
        return False
    try:
        b64 = base64.b64encode(content_bytes).decode()
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        sha = None
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            sha = r.json().get("sha")
        body = {"message": commit_msg, "content": b64, "branch": "main"}
        if sha:
            body["sha"] = sha
        r = requests.put(url, json=body, headers=headers, timeout=15)
        ok = r.status_code in (200, 201)
        if ok:
            logging.info(f"GitHub: {filename} pushed")
        else:
            logging.warning(f"GitHub push {filename}: {r.status_code} {r.text[:200]}")
        return ok
    except Exception as e:
        logging.error(f"GitHub push error: {e}")
        return False

def generate_data_json(force=False):
    global _DATA_CACHE, _DATA_CACHE_TS, PRICE_CACHE
    now = time.time()
    if not force and _DATA_CACHE and (now - _DATA_CACHE_TS) < _DATA_CACHE_TTL:
        return _DATA_CACHE
    total = len(signal_log)
    checked = [s for s in signal_log if s["status"] == "checked"]
    wins = sum(1 for s in checked if s["result"] == "win")
    losses = len(checked) - wins
    win_rate = round(wins / (wins + losses) * 100, 1) if (wins + losses) else 0
    avg_conf = 0
    if signal_log:
        confs = [s.get("confidence", 0) for s in signal_log if s.get("confidence")]
        avg_conf = round(sum(confs) / len(confs), 1) if confs else 0
    signals_out = []
    for s in signal_log[-50:]:
        if s.get("rec") in ("WAIT", "انتظار") and s.get("confidence", 0) < 20: continue
        signals_out.append({
            "id": s.get("id", ""),
            "asset": s.get("asset", ""), "recommendation": s.get("rec", ""),
            "price": s.get("price", ""), "confidence": s.get("confidence", 0),
            "result": s.get("result", "pending"), "timestamp": s.get("time", ""),
            "tp": s.get("tp", ""), "sl": s.get("sl", ""),
            "risk_reward": s.get("rr", ""), "support": s.get("support", ""),
            "resistance": s.get("resistance", ""), "rsi": s.get("rsi", ""),
            "sentiment_label": s.get("sentiment_label", ""),
            "sentiment_score": s.get("sentiment_score", 0),
            "reason": s.get("reason", ""),
            "user_entered": s.get("user_entered", False),
            "user_skipped": s.get("user_skipped", False),
            "notes": s.get("notes", ""),
            "entry_price": s.get("entry_price"), "exit_price": s.get("exit_price"),
            "exit_reason": s.get("exit_reason", ""),
            "ai_review": s.get("ai_review", ""),
            "candle_patterns": s.get("candle_patterns", ""),
            "vol_note": s.get("vol_note", ""),
            "breakeven_set": s.get("breakeven_set", False),
            "provider_used": s.get("provider_used", ""),
            "adx": s.get("adx"),
            "session": s.get("session", ""),
        })
    sentiment_out = []
    for s in signal_log[-5:]:
        if s.get("sentiment_label"):
            sentiment_out.append({
                "headline": (s.get("analysis", "") or "")[:80],
                "score": s.get("sentiment_score", 0),
                "label": s.get("sentiment_label", "[NEU]")
            })
    alerts_out = [{"asset": a.get("asset",""), "target": str(a.get("price","")), "direction": "above" if a.get("above",True) else "below"} for a in price_alerts]
    entered = [s for s in signal_log if s.get("user_entered")]
    total_entered = len(entered)
    won_entered = sum(1 for s in entered if s.get("result") == "win")
    lost_entered = sum(1 for s in entered if s.get("result") == "loss")
    entered_wr = round(won_entered / total_entered * 100, 1) if total_entered else 0
    # Advanced stats
    pf = round(wins / losses, 2) if losses else (wins if wins else 0)
    total_r = sum(abs(float(s.get("rr",0) or 0)) for s in checked if s.get("rr"))
    avg_rr = round(total_r / len([s for s in checked if s.get("rr")]), 2) if any(s.get("rr") for s in checked) else 0
    returns = [s.get("pnl_usd", 0) or 0 for s in checked if s.get("result")]
    avg_ret = sum(returns)/len(returns) if returns else 0
    std_ret = (sum((r-avg_ret)**2 for r in returns)/len(returns))**0.5 if len(returns)>1 else 1
    sharpe = round(avg_ret/std_ret*252**0.5, 2) if std_ret else 0
    dd = 0; peak = 0; equity = 0
    for r in returns:
        equity += r
        if equity > peak: peak = equity
        dd = min(dd, equity - peak)
    max_dd = round(abs(dd), 2)
    eval_stats = {"total_signals": total, "win_rate": win_rate, "entered": total_entered,
                  "won": won_entered, "lost": lost_entered, "entered_win_rate": entered_wr,
                  "profit_factor": pf, "avg_rr": avg_rr, "sharpe": sharpe, "max_drawdown": max_dd,
                  "avg_confidence": avg_conf}
    # Performance by hour/day
    perf_hour = {}
    for s in checked:
        try:
            h = datetime.fromisoformat(s.get("time","")).hour
        except: h = 0
        if h not in perf_hour: perf_hour[h] = {"wins":0,"total":0}
        perf_hour[h]["total"] += 1
        if s.get("result") == "win": perf_hour[h]["wins"] += 1
    perf_hour_out = {str(h):{"wr":round(d["wins"]/d["total"]*100,1) if d["total"] else 0,"total":d["total"]} for h,d in perf_hour.items()}
    # Behavior analysis
    behavior = {}
    # Best/worst asset
    asset_stats = {}
    for s in entered:
        a = s.get("asset","?")
        if a not in asset_stats: asset_stats[a] = {"wins":0,"total":0}
        asset_stats[a]["total"] += 1
        if s.get("result") == "win": asset_stats[a]["wins"] += 1
    best_asset = max(asset_stats, key=lambda a: asset_stats[a]["wins"]/max(asset_stats[a]["total"],1) if asset_stats[a]["total"] else 0) if asset_stats else "?"
    worst_asset = min(asset_stats, key=lambda a: asset_stats[a]["wins"]/max(asset_stats[a]["total"],1) if asset_stats[a]["total"] else 1) if asset_stats else "?"
    behavior["best_asset"] = best_asset
    behavior["worst_asset"] = worst_asset
    # Skip vs enter
    behavior["total_skipped"] = sum(1 for s in signal_log if s.get("user_skipped"))
    behavior["skipped_winners"] = sum(1 for s in signal_log if s.get("user_skipped") and s.get("result") == "win")
    # Streaks
    streaks = {"win_streak":0,"loss_streak":0,"max_win_streak":0,"max_loss_streak":0}
    cur_w=0; cur_l=0
    for s in checked:
        if s.get("result")=="win": cur_w+=1;cur_l=0;streaks["max_win_streak"]=max(streaks["max_win_streak"],cur_w)
        elif s.get("result")=="loss": cur_l+=1;cur_w=0;streaks["max_loss_streak"]=max(streaks["max_loss_streak"],cur_l)
    behavior["streaks"] = streaks
    # Skip behavior - does user skip winners or losers more?
    behavior["skip_accuracy"] = round(behavior["skipped_winners"]/max(behavior["total_skipped"],1)*100,1)
    behavior["enter_accuracy"] = round(won_entered/max(total_entered,1)*100,1)
    # AI accuracy per asset
    ai_acc = {}
    for s in signal_log:
        a = s.get("asset","?")
        if a not in ai_acc: ai_acc[a] = {"correct":0,"wrong":0,"total":0}
        if s.get("status") == "checked":
            ai_acc[a]["total"] += 1
            if s.get("result") == "win": ai_acc[a]["correct"] += 1
            elif s.get("result") == "loss": ai_acc[a]["wrong"] += 1
    behavior["ai_accuracy"] = {a: {"correct":d["correct"],"total":d["total"],"rate":round(d["correct"]/d["total"]*100,1) if d["total"] else 0} for a,d in ai_acc.items()}
    # AI provider accuracy tracking
    prov_stats = {}
    for s in signal_log:
        prov = s.get("provider_used", "unknown")
        if prov not in prov_stats: prov_stats[prov] = {"wins":0,"losses":0,"total":0}
        if s.get("status") == "checked":
            prov_stats[prov]["total"] += 1
            if s.get("result") == "win": prov_stats[prov]["wins"] += 1
            elif s.get("result") == "loss": prov_stats[prov]["losses"] += 1
    behavior["provider_accuracy"] = {p: {"wins":d["wins"],"losses":d["losses"],"total":d["total"],
        "rate":round(d["wins"]/d["total"]*100,1) if d["total"] else 0} for p,d in prov_stats.items()}
    # P&L per asset (real USD)
    pnl = {}
    for s in signal_log:
        a = s.get("asset","?")
        if a not in pnl: pnl[a] = {"wins":0,"losses":0,"total_pnl":0.0,"trades":0}
        if s.get("status") == "checked" and s.get("result"):
            pnl[a]["trades"] += 1
            pnl_usd = s.get("pnl_usd") or 0
            if s.get("result") == "win":
                pnl[a]["wins"] += 1
                pnl[a]["total_pnl"] += abs(pnl_usd)
            elif s.get("result") == "loss":
                pnl[a]["losses"] += 1
                pnl[a]["total_pnl"] -= abs(pnl_usd)
    behavior["pnl"] = {a: {"wins":d["wins"],"losses":d["losses"],"pnl":round(d["total_pnl"],2),"trades":d["trades"]} for a,d in pnl.items()}
    # P&L cumulative history (real USD)
    pnl_history = []
    cum = 0
    for s in signal_log:
        if s.get("status") == "checked" and s.get("result"):
            pnl_usd = s.get("pnl_usd") or 0
            cum += pnl_usd
            try:
                t = datetime.fromisoformat(s.get("time","")).timestamp() * 1000
            except:
                t = int(time.time()) * 1000
            pnl_history.append({"time": t, "pnl": round(cum, 2)})
    data = {
        "bot_status": "online",
        "last_update": datetime.now(timezone.utc).isoformat(),
        "stats": {"total_signals": total, "win_rate": win_rate, "active_alerts": len(price_alerts), "avg_confidence": avg_conf},
        "checked": {"wins": wins, "losses": losses, "win_rate": win_rate},
        "eval": eval_stats,
        "radar_enabled": RADAR_ENABLED,
        "min_confidence": MIN_CONFIDENCE,
        "signals": signals_out, "sentiment": sentiment_out, "alerts": alerts_out,
        "price_history": PRICE_CACHE.get("history", {}),
        "ohlcv_history": PRICE_CACHE.get("ohlcv", {}),
        "pnl_history": pnl_history,
        "perf_hour": perf_hour_out,
        "behavior": behavior
    }
    _DATA_CACHE = j = json.dumps(data, ensure_ascii=False)
    _DATA_CACHE_TS = time.time()
    return j
async def web_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id); save_json('users.json', list(authorized_users))
    await update.message.reply_text("[WAIT] جاري إنشاء الصفحة ونشرها على GitHub Pages...")
    html = await asyncio.get_event_loop().run_in_executor(None, generate_monitor_html)
    data_json = await asyncio.get_event_loop().run_in_executor(None, generate_data_json)
    try:
        with open('monitor.html', 'w', encoding='utf-8') as f: f.write(html)
    except Exception as e:
        logging.warning(f"web_cmd: monitor.html write failed: {e}")
    gh_ok = await asyncio.get_event_loop().run_in_executor(None, lambda: push_to_github("index.html", html.encode('utf-8'), "تحديث صفحة المراقبة"))
    await asyncio.get_event_loop().run_in_executor(None, lambda: push_to_github("data.json", data_json.encode('utf-8'), "تحديث بيانات الإشارات"))
    gh_url = "https://mohammedbesmouch-byte.github.io/aura-monitor/"
    msg_parts = []
    if gh_ok:
        msg_parts.append(f"[OK] *تم النشر على GitHub Pages!*\n{gh_url}")
    await update.message.reply_text("\n\n".join(msg_parts), parse_mode="Markdown")

# ─── UI ───
def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("[>] تحليل فوري", callback_data="analyze")],
        [InlineKeyboardButton("[>] الرادار الذكي", callback_data="radar_status")],
        [InlineKeyboardButton("[>] الإشارات", callback_data="signals")],
        [InlineKeyboardButton("[>] التنبيهات", callback_data="alerts_menu")],
        [InlineKeyboardButton("[>] باك تيست", callback_data="backtest_menu")],
        [InlineKeyboardButton("[>] صفحة المراقبة", callback_data="web_menu")],
        [InlineKeyboardButton("[>] الإعدادات", callback_data="settings")],
        [InlineKeyboardButton("[?] مساعدة", callback_data="help")]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id)
        save_json('users.json', list(authorized_users))
    total_sigs = len(signal_log)
    checked = [s for s in signal_log if s["status"] == "checked"]
    wins = sum(1 for s in checked if s["result"] == "win")
    losses = len(checked) - wins
    wr = round(wins / (wins + losses) * 100, 1) if (wins + losses) else 0
    radar_status = "[ON]" if RADAR_ENABLED else "[OFF]"
    await update.message.reply_text(
        "[*] AURA CLOUD\n"
        "=============\n"
        "AI Signal Analysis Platform\n"
        "-------------\n"
        f"Signals: {total_sigs}  |  Win: {wr}%\n"
        f"Radar: {radar_status}  |  Min: {MIN_CONFIDENCE}%\n"
        f"Interval: {RADAR_INTERVAL//60}min\n"
        "-------------\n"
        "Select from menu:\n"
        "=============",
        parse_mode="Markdown", reply_markup=main_menu_kb())

async def analyze_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id)
        save_json('users.json', list(authorized_users))
    args = context.args
    if not args:
        await update.message.reply_text("📝 استخدم: /analyze EURUSD\nالرموز: " + ", ".join(RADAR_ASSETS))
        return
    symbol = args[0].upper()
    if symbol not in RADAR_ASSETS:
        await update.message.reply_text(f"❌ رمز غير صالح: {symbol}\nالرموز: " + ", ".join(RADAR_ASSETS))
        return
    msg = await update.message.reply_text(f"🌀 جاري تحليل {symbol}...")
    try:
        # Fast single-AI analysis for Telegram (avoids Multi-AI timeout)
        res = await asyncio.get_event_loop().run_in_executor(None, fast_analyze, symbol)
        if not res or len(res) < 4:
            await msg.edit_text(f"❌ فشل تحليل {symbol}")
            return
        sig_id = log_signal(symbol, res[2], res[1], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10], res[11], res[12], candle_patterns=res[13] if len(res)>13 else "", vol_note=res[14] if len(res)>14 else "", adx=res[15] if len(res)>15 else None, session=res[16] if len(res)>16 else "")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ دخلت الصفقة", callback_data=f"enter_{sig_id}"),
             InlineKeyboardButton("❌ تخطيت", callback_data=f"skip_{sig_id}"),
             InlineKeyboardButton("🚪 خرجت", callback_data=f"exit_{sig_id}")]
        ])
        await msg.edit_text(res[0], parse_mode="Markdown", reply_markup=kb)
    except Exception as e:
        await msg.edit_text(f"❌ خطأ: {e}")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id); save_json('users.json', list(authorized_users))
    total = len(signal_log)
    checked = [s for s in signal_log if s["status"] == "checked"]
    wins = sum(1 for s in checked if s["result"] == "win")
    losses = len(checked) - wins
    wr = round(wins / (wins + losses) * 100, 1) if (wins + losses) else 0
    await update.message.reply_text(
        "[?] *AURA CLOUD V4 | Help*\n"
        "──────────────────────\n"
        "[>] *Quick Analysis* — Signal card: TP, SL, R/R, Sentiment\n"
        "[>] *Radar* — Auto-scan ≥70% confidence\n"
        "[>] *Signals* — History with Win/Loss\n"
        "[>] *Alerts* — /alert EURUSD 1.09\n"
        "[>] *Backtest* — /backtest BTCUSD 30\n"
        "[>] *Web Monitor* — /web — view signals online\n"
        "──────────────────────\n"
        f"[OK] Wins: {wins}  [XX] Losses: {losses}  Win Rate: {wr}%\n"
        "──────────────────────\n"
        "/unlock <password> — Unlock\n"
        "/web — Dashboard link\n"
        "/alert EURUSD 1.10 — Price alert\n"
        "/backtest BTCUSD 14 — Backtest\n"
        "/provider — Current AI provider",
        parse_mode="Markdown")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    err = str(context.error); logging.error(f"Error: {err}")
    if update and update.effective_message:
        if "401" in err or "token" in err.lower(): await update.effective_message.reply_text("[XX] خطأ في توكن البوت.")
        elif "429" in err: await update.effective_message.reply_text("[WAIT] حد الطلبات. انتظر دقيقة.")
        elif "api_key" in err.lower(): await update.effective_message.reply_text("[XX] خطأ في مفتاح API.")

async def alert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global price_alerts
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id); save_json('users.json', list(authorized_users))
    args = context.args
    if len(args) < 2: return await update.message.reply_text("[XX] /alert EURUSD 1.0900\nأو /alert USDJPY 150 below")
    symbol = args[0].upper()
    if symbol not in RADAR_ASSETS: return await update.message.reply_text(f"[XX] الأصل غير مدعوم.")
    try:
        target = float(args[1].replace(',',''))
        is_above = not (len(args) > 2 and args[2].lower() in ('below','تحت'))
        with alerts_lock:
            price_alerts.append({"asset":symbol,"price":target,"above":is_above,"user_id":update.effective_user.id,"created_at":datetime.now(timezone.utc).isoformat()})
            save_json('alerts.json', price_alerts)
        await update.message.reply_text(f"[OK] تنبيه!\n{'[UP] فوق' if is_above else '[DOWN] تحت'} {symbol} `{target}`", parse_mode="Markdown")
    except Exception as e:
        logging.warning(f"alert_cmd: {e}")
        await update.message.reply_text("[XX] السعر غير صحيح.")

async def backtest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id); save_json('users.json', list(authorized_users))
    args = context.args; symbol = args[0].upper() if args else "EURUSD"; days = int(args[1]) if len(args)>1 else 14
    if symbol not in RADAR_ASSETS: return await update.message.reply_text("[XX] غير مدعوم")
    await update.message.reply_text(f"[WAIT] اختبار {symbol} {days} يوم...")
    result, err = await asyncio.get_event_loop().run_in_executor(None, backtest, symbol, days)
    if err: return await update.message.reply_text(f"[XX] {err}")
    await update.message.reply_text(
        f"[CHART] *باك تيست | {result['asset']}*\n───────────────\n"
        f"[OK] صفقات: `{result['trades']}` | ربح: `{result['wins']}` | خسارة: `{result['losses']}`\n"
        f"[UP] دقة: *{result['win_rate']}%*\n[SAFE] الرصيد النهائي: `{result['final_balance']}`\n[NOTE] ROI: `{result['roi']}%`\n───\n[NOTE] اختبار تاريخي ليس توصية", parse_mode="Markdown")

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global RADAR_ENABLED, MIN_CONFIDENCE, last_signals
    q = update.callback_query; await q.answer(); d = q.data

    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id)
        save_json('users.json', list(authorized_users))

    if d.startswith("enter_"):
        sid = d.split("_",1)[1]
        found = False
        with signal_lock:
            for s in signal_log:
                if s.get("id") == sid:
                    s["user_entered"] = True; s["user_confirmed_at"] = datetime.now(timezone.utc).isoformat()
                    s["entry_price"] = s.get("price", "")
                    found = True; break
            if found:
                save_json('signals.json', signal_log)
        if found:
            await q.answer("✅ تم تسجيل الدخول! 🚪 للخروج استخدم زر الخروج", show_alert=True)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🚪 خرجت من الصفقة", callback_data=f"exit_{sid}")]])
            await q.edit_message_reply_markup(reply_markup=kb)
        else: await q.answer("⚠️ الإشارة غير موجودة في السجل", show_alert=True)
        return

    if d.startswith("skip_"):
        sid = d.split("_",1)[1]
        with signal_lock:
            for s in signal_log:
                if s.get("id") == sid:
                    s["user_skipped"] = True; s["user_confirmed_at"] = datetime.now(timezone.utc).isoformat()
                    break
            save_json('signals.json', signal_log)
        await q.answer("❌ تم التخطي", show_alert=True)
        return

    if d.startswith("exit_"):
        sid = d.split("_",1)[1]
        found = False
        price_now = None
        with signal_lock:
            for s in signal_log:
                if s.get("id") == sid and s.get("user_entered"):
                    try:
                        p, err = get_price(s["asset"])
                        if not err: price_now = str(p)
                    except Exception as e:
                        logging.warning(f"button exit get_price: {e}")
                    s["exit_price"] = price_now or s.get("price", "")
                    s["exit_reason"] = "user_exit"
                    s["status"] = "checked"
                    s["checked_at"] = datetime.now(timezone.utc).isoformat()
                    if s.get("entry_price") and price_now:
                        ep = float(s["entry_price"]); cp = float(price_now)
                        if "شراء" in s.get("rec",""):
                            s["result"] = "win" if cp > ep else "loss"
                        elif "بيع" in s.get("rec",""):
                            s["result"] = "win" if cp < ep else "loss"
                        pnl_pct, pnl_usd = calc_pnl(ep, cp, s.get("rec",""), s.get("position_size"))
                        s["pnl_pct"] = pnl_pct; s["pnl_usd"] = pnl_usd
                    found = True; break
            if found:
                save_json('signals.json', signal_log)
        if found:
            save_json('signals.json', signal_log)
            for s in signal_log:
                if s.get("id") == sid and s.get("result"):
                    archive_append(s)
                    break
            await q.answer("🚪 تم تسجيل الخروج ✅", show_alert=True)
            await q.edit_message_reply_markup(reply_markup=None)
        else:
            await q.answer("⚠️ الصفقة غير موجودة أو لم تدخلها بعد", show_alert=True)
        return

    if d == "analyze":
        rows = [[InlineKeyboardButton(RADAR_ASSETS[i],callback_data=f"a_{RADAR_ASSETS[i]}")]+([InlineKeyboardButton(RADAR_ASSETS[i+1],callback_data=f"a_{RADAR_ASSETS[i+1]}")] if i+1<len(RADAR_ASSETS) else []) for i in range(0,len(RADAR_ASSETS),2)]
        rows.append([InlineKeyboardButton("[BACK] رجوع",callback_data="main")])
        await q.edit_message_text("[SRCH] *اختر الأصل*\nتوصية • TP • SL • R/R • مشاعر • ثقة", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(rows))

    elif d.startswith("a_"):
        sym = d.split("_",1)[1]; await q.edit_message_text(f"...")
        res = await asyncio.get_event_loop().run_in_executor(None, analyze, sym)
        line = res[0]; conf = res[1]; rec = res[2]; price = res[3]
        kb = [[InlineKeyboardButton("Back",callback_data="analyze")],[InlineKeyboardButton("Home",callback_data="main")]]
        # Log signal and show buttons if confidence is good
        if conf >= MIN_CONFIDENCE and price and rec not in ("WAIT", "انتظار"):
            sig_id = log_signal(sym, rec, conf, price, res[4], res[5], res[6], res[7], res[8], res[9], res[10], res[11], res[12], candle_patterns=res[13] if len(res)>13 else "", vol_note=res[14] if len(res)>14 else "", adx=res[15] if len(res)>15 else None, session=res[16] if len(res)>16 else "")
            kb = [[InlineKeyboardButton("✅ دخلت الصفقة", callback_data=f"enter_{sig_id}"),
                    InlineKeyboardButton("❌ تخطيت", callback_data=f"skip_{sig_id}")]]
        await q.edit_message_text(line, reply_markup=InlineKeyboardMarkup(kb))

    elif d == "radar_status":
        if not last_signals: return await q.edit_message_text("[RADAR] *الرادار الذكي*\nيبحث عن إشارات ≥ {}%...".format(MIN_CONFIDENCE), parse_mode="Markdown")
        high = [s for s in last_signals if s["confidence"]>=MIN_CONFIDENCE][-8:]
        msg = "[RADAR] *الرادار*\n─────\n"+"\n".join(f"{'[HOT]' if s['confidence']>=85 else '[STAR]'} {s['asset']} | {s['confidence']}% | {s['rec']}" for s in high)
        msg += f"\n─────\nالحد: ≥{MIN_CONFIDENCE}% | كل: {RADAR_INTERVAL//60}د"
        await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("[SYNC]",callback_data="radar_status")],[InlineKeyboardButton("[BACK]",callback_data="main")]]))

    elif d == "signals":
        checked = [s for s in signal_log if s["status"]=="checked"][-10:]
        if not checked: return await q.edit_message_text("[SIGNALS] لا توجد نتائج بعد.", parse_mode="Markdown")
        wins = sum(1 for s in checked if s["result"]=="win"); losses = len(checked)-wins; acc = round(wins/(wins+losses)*100,1) if (wins+losses) else 0
        msg = f"[SIGNALS] *History*\n─────\n[OK] Wins: {wins} | [XX] Losses: {losses} | Rate: {acc}%\n─────\n"+"\n".join(f"{'[OK]' if s['result']=='win' else '[XX]'} {s['asset']} | {s['confidence']}%" for s in checked[-5:])
        await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("[SYNC]",callback_data="signals")],[InlineKeyboardButton("[BACK]",callback_data="main")]]))

    elif d == "alerts_menu":
        active = price_alerts[-5:] if price_alerts else []
        msg = "[BELL] *التنبيهات*\n─────\n`/alert EURUSD 1.09`\n`/alert USDJPY 150 below`\n─────\n"
        msg += "\n".join(f"{'[UP]' if a.get('above',True) else '[DOWN]'} {a['asset']} {a['price']}" for a in active) if active else "لا توجد تنبيهات"
        await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("[BACK]",callback_data="main")]]))

    elif d == "backtest_menu":
        await q.edit_message_text("[CHART] *باك تيست*\n`/backtest EURUSD 14`\n`/backtest BTCUSD 30`", parse_mode="Markdown")

    elif d == "web_menu":
        await q.edit_message_text("[WEB] *صفحة المراقبة*\n───\nالموقع المباشر:\n[https://aura-cloud.koyeb.app/monitor.html](https://aura-cloud.koyeb.app/monitor.html)\n\n[>] تحديث ونشر: `/web`", parse_mode="Markdown", disable_web_page_preview=True)

    elif d == "settings":
        msg = f"[GEAR] *الإعدادات*\n─────\n[RADAR] الرادار: {'مفعل [OK]' if RADAR_ENABLED else 'معطل [XX]'}\n[TARGET] الثقة: ≥{MIN_CONFIDENCE}%\n[CLOCK] الفاصل: {RADAR_INTERVAL//60}د\n─────"
        await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("[SYNC] تبديل الرادار",callback_data="toggle")],[InlineKeyboardButton("[TARGET] -5%",callback_data="conf_dn"),InlineKeyboardButton("[TARGET] +5%",callback_data="conf_up")],[InlineKeyboardButton("[BACK]",callback_data="main")]]))

    elif d == "toggle": RADAR_ENABLED = not RADAR_ENABLED; await q.answer(f"الرادار: {'مفعل [OK]' if RADAR_ENABLED else 'معطل [XX]'}",show_alert=True); await button(update,context)
    elif d == "conf_dn": MIN_CONFIDENCE = max(20,MIN_CONFIDENCE-5); await q.answer(f"≥{MIN_CONFIDENCE}%",show_alert=True); await button(update,context)
    elif d == "conf_up": MIN_CONFIDENCE = min(100,MIN_CONFIDENCE+5); await q.answer(f"≥{MIN_CONFIDENCE}%",show_alert=True); await button(update,context)

    elif d == "help":
        await q.edit_message_text("[?] *Help*\n─────\n[>] Quick Analysis\n[>] Radar Signals\n[>] Signal History\n[>] Price Alerts\n[>] Backtest\n[>] Web Monitor\n─────\n/alert EURUSD 1.09\n/backtest BTCUSD 30\n/web", parse_mode="Markdown")

    elif d == "main":
        total_sigs = len(signal_log)
        checked = [s for s in signal_log if s["status"] == "checked"]
        wins = sum(1 for s in checked if s["result"] == "win")
        losses = len(checked) - wins
        wr = round(wins / (wins + losses) * 100, 1) if (wins + losses) else 0
        radar_status = "[ON]" if RADAR_ENABLED else "[OFF]"
        msg = "[*] AURA CLOUD\n"
        msg += "=============\n"
        msg += "AI Signal Analysis Platform\n"
        msg += "-------------\n"
        msg += f"Signals: {total_sigs} | Win: {wr}%\n"
        msg += f"Radar: {radar_status} | Min: {MIN_CONFIDENCE}%\n"
        msg += f"Wins: {wins} | Losses: {losses}\n"
        msg += "-------------\n"
        msg += "Select from menu:\n"
        msg += "============="
        await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=main_menu_kb())

async def unknown_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id)
        save_json('users.json', list(authorized_users))
    if t.startswith('/alert'): return await alert_cmd(update, context)
    if t.startswith('/backtest'): return await backtest_cmd(update, context)
    if t.startswith('/web'): return await web_cmd(update, context)
    if t.startswith('/approve'): return await approve_cmd(update, context)
    if t.startswith('/users'): return await users_cmd(update, context)
    await update.message.reply_text("عذراً، استخدم /start")

# ─── MAIN ───
# ─── WEB API FOR MONITOR ───
API_PORT = int(os.environ.get("PORT", 10993))

class SignalAPIHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
    def _json(self, data, status=200):
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()
    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/")
        if path == "" or path == "/":
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            try:
                with open("monitor.html", "rb") as _f:
                    self.wfile.write(_f.read())
            except FileNotFoundError:
                self.wfile.write(b"\xef\xbb\xbf<h1>monitor.html not found</h1>")
        elif path == "/data.json":
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            d = generate_data_json()
            self.wfile.write(d.encode("utf-8"))
        elif path == "/monitor.html":
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            try:
                with open("monitor.html", "rb") as _f:
                    self.wfile.write(_f.read())
            except FileNotFoundError:
                self.wfile.write(b"\xef\xbb\xbf<h1>monitor.html not found on server</h1>")
        elif path == "/api/scan":
            self._json({"ok": True, "assets": market_scan()})
        elif path == "/api/prices":
            px = {}
            for a in RADAR_ASSETS:
                if a in price_cache: px[a] = price_cache[a]
            self._json({"ok": True, "prices": px})
        elif path == "/api/journal":
            self._json({"ok": True, "report": generate_journal()})
        elif path == "/api/calendar":
            self._json({"ok": True, "events": get_economic_calendar()})
        elif path == "/health":
            self._json({"ok": True, "status": "alive", "time": datetime.now(timezone.utc).isoformat(), "signals": len(signal_log), "radar": RADAR_ENABLED})
        else:
            self._json({"ok":False, "error":"invalid path"}, 404)
    def do_POST(self):
        global RADAR_ENABLED, MIN_CONFIDENCE
        path = urlparse(self.path).path.rstrip("/")
        parts = path.split("/")
        if len(parts) >= 4 and parts[1] == "api" and parts[2] in ("enter","skip","exit"):
            sig_id = parts[3]
            action = parts[2]
            found = False
            for s in signal_log:
                if s.get("id") == sig_id:
                    if action == "enter":
                        s["user_entered"] = True
                        s["user_skipped"] = False
                        s["entry_price"] = s.get("price", "")
                    elif action == "exit":
                        if not s.get("user_entered"):
                            self._json({"ok":False, "error":"لم تدخل هذه الصفقة"}, 400)
                            return
                        # Get current price
                        try:
                            p, err = get_price(s["asset"])
                            if not err: s["exit_price"] = str(p)
                        except Exception as e: logging.warning(f"API exit get_price: {e}")
                        s["exit_reason"] = "user_exit"
                        s["status"] = "checked"
                        s["checked_at"] = datetime.now(timezone.utc).isoformat()
                        if s.get("entry_price") and s.get("exit_price"):
                            try:
                                ep = float(s["entry_price"]); cp = float(s["exit_price"])
                                if "شراء" in s.get("rec",""):
                                    s["result"] = "win" if cp > ep else "loss"
                                elif "بيع" in s.get("rec",""):
                                    s["result"] = "win" if cp < ep else "loss"
                                pnl_pct, pnl_usd = calc_pnl(ep, cp, s.get("rec",""), s.get("position_size"))
                                s["pnl_pct"] = pnl_pct; s["pnl_usd"] = pnl_usd
                            except Exception as e: logging.warning(f"API exit result: {e}")
                    else:
                        s["user_entered"] = False
                        s["user_skipped"] = True
                    s["user_confirmed_at"] = datetime.now(timezone.utc).isoformat()
                    save_json("signals.json", signal_log); _invalidate_cache()
                    found = True
                    break
            if found:
                self._json({"ok":True, "sig_id":sig_id, "action":action})
            else:
                self._json({"ok":False, "error":"signal not found"}, 404)
        elif path == "/api/radar/toggle":
            RADAR_ENABLED = not RADAR_ENABLED
            self._json({"ok":True, "radar_enabled":RADAR_ENABLED})
        elif path == "/api/radar/conf_up":
            MIN_CONFIDENCE = min(100, MIN_CONFIDENCE+5)
            self._json({"ok":True, "min_confidence":MIN_CONFIDENCE})
        elif path == "/api/radar/conf_down":
            MIN_CONFIDENCE = max(20, MIN_CONFIDENCE-5)
            self._json({"ok":True, "min_confidence":MIN_CONFIDENCE})
        elif path == "/api/radar/status":
            self._json({"ok":True, "radar_enabled":RADAR_ENABLED, "min_confidence":MIN_CONFIDENCE, "interval":RADAR_INTERVAL, "signals":len(signal_log)})
        elif len(parts) >= 4 and parts[1] == "api" and parts[2] == "analyze":
            asset = parts[3].upper()
            if asset not in RADAR_ASSETS:
                self._json({"ok":False, "error":"invalid asset"}, 400)
                return
            try:
                # Return existing signal for this asset from cache
                existing = [s for s in signal_log if s["asset"] == asset]
                sig = existing[-1] if existing else {}
                self._json({"ok":True, "sig_id":sig.get("id",""), "signal":{"asset":asset,"recommendation":sig.get("recommendation","انتظار"),"confidence":float(sig.get("confidence",50)),"price":str(sig.get("price",price_cache.get(asset,""))),"tp":sig.get("tp",""),"sl":sig.get("sl",""),"risk_reward":sig.get("rr",""),"support":sig.get("support",""),"resistance":sig.get("resistance",""),"rsi":sig.get("rsi",""),"sentiment_label":sig.get("sentiment_label",""),"sentiment_score":float(sig.get("sentiment_score",0)),"reason":sig.get("reason","")}})
            except Exception as e:
                self._json({"ok":False, "error":str(e)}, 500)
        elif len(parts) >= 4 and parts[1] == "api" and parts[2] == "analyze_signal":
            sig_id = parts[3]
            found = None
            for s in signal_log:
                if s.get("id") == sig_id:
                    found = s; break
            if not found:
                self._json({"ok":False, "error":"signal not found"}, 404)
                return
            if found.get("ai_review"):
                self._json({"ok":True, "sig_id":sig_id, "ai_review": found["ai_review"]})
                return
            try:
                asset = found.get("asset","?")
                rec = found.get("rec","")
                price = found.get("price","")
                result = found.get("result","pending")
                entry = found.get("entry_price","")
                exit_p = found.get("exit_price","")
                prompt = f"حلل هذه الصفقة بأثر رجعي:\nالأصل: {asset}\nالتوصية: {rec}\nسعر الدخول: {price}\nالنتيجة: {result}\nسعر الخروج: {exit_p if exit_p else 'لم يخرج بعد'}\nأعط تقييماً مختصراً (جملة أو جملتين) عن جودة الصفقة ومدى صحة القرار."
                r = _ai_request(prompt, temperature=0.1)
                review = r.choices[0].message.content.strip()[:300]
                found["ai_review"] = review
                save_json("signals.json", signal_log); _invalidate_cache()
                self._json({"ok":True, "sig_id":sig_id, "ai_review": review})
            except Exception as e:
                self._json({"ok":False, "error":f"AI review failed: {e}"}, 500)
        elif len(parts) >= 3 and parts[1] == "api" and parts[2] == "backtest":
            asset = parts[3] if len(parts) >= 4 else "EURUSD"
            days = int(parts[4]) if len(parts) >= 5 else 30
            result, err = backtest(asset, days)
            if err: self._json({"ok": False, "error": err}, 400)
            else: self._json({"ok": True, "backtest": result})
        elif len(parts) >= 3 and parts[1] == "api" and parts[2] == "chat":
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                body = json.loads(self.rfile.read(content_length).decode()) if content_length else {}
            except: body = {}
            question = body.get("question", "")
            if not question: self._json({"ok": False, "error": "No question"}, 400); return
            # Check if user is asking about a specific symbol (supports Arabic names)
            AR_NAMES = {"ذهب":"XAUUSD","فضة":"XAGUSD","بيتكوين":"BTCUSD","يورو":"EURUSD","باوند":"GBPUSD","أسترالي":"AUDUSD","ين":"JPY","كندي":"USDCAD","فرنك":"USDCHF","نيوزيلندي":"NZDUSD"}
            q_upper = question.upper()
            matched_asset = None
            for a in RADAR_ASSETS:
                if a in q_upper:
                    matched_asset = a
                    break
            if not matched_asset:
                for ar, sym in AR_NAMES.items():
                    if ar in question:
                        matched_asset = sym
                        break
            if matched_asset and any(k in question for k in ["حلل","ANALYZE","حلال","هلال"]):
                try:
                    res = analyze(matched_asset)
                    if res and len(res) >= 4 and "فشل" not in res[0] and "[!]" not in res[0]:
                        sig_id = log_signal(matched_asset, res[2], res[1], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10], res[11], res[12], candle_patterns=res[13] if len(res)>13 else "", vol_note=res[14] if len(res)>14 else "", adx=res[15] if len(res)>15 else None, session=res[16] if len(res)>16 else "")
                        self._json({"ok":True,"type":"analysis","sig_id":sig_id,"answer":res[0],"asset":matched_asset,"rec":res[2],"confidence":res[1],"price":res[3],"tp":res[4],"sl":res[5],"rr":res[6],"support":res[7],"resistance":res[8],"rsi":res[9],"sentiment_label":res[10],"sentiment_score":res[11],"reason":res[12],"candle_patterns":res[13] if len(res)>13 else "","vol_note":res[14] if len(res)>14 else ""})
                        return
                except Exception as e:
                    logging.error(f"Chat analyze {matched_asset}: {e}")
            answer, err = ai_chat(question, body.get("history"))
            if err: self._json({"ok": True, "type":"chat","answer": "عذراً، الذكاء الاصطناعي مشغول حالياً. حاول مرة أخرى بعد قليل." if "ar" in (self.headers.get("Accept-Language","") or "ar") else "Sorry, AI is busy. Please try again later."})
            else: self._json({"ok": True, "type":"chat","answer": answer})
        elif len(parts) >= 4 and parts[1] == "api" and parts[2] == "note":
            sig_id = parts[3]
            try:
                cl = int(self.headers.get('Content-Length', 0))
                body = json.loads(self.rfile.read(cl).decode()) if cl else {}
            except: body = {}
            note = body.get("note", "")
            for s in signal_log:
                if s.get("id") == sig_id:
                    s["notes"] = note
                    save_json("signals.json", signal_log); _invalidate_cache()
                    self._json({"ok": True})
                    return
            self._json({"ok": False, "error": "not found"}, 404)
        else:
            self._json({"ok":False, "error":"invalid path"}, 404)

# ─── DAILY REPORT & NOTES ───
async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id); save_json('users.json', list(authorized_users))
    d, _ = await asyncio.get_event_loop().run_in_executor(None, lambda: (generate_data_json(),None))
    d = json.loads(d) if isinstance(d, str) else d
    e = d.get("eval", {}); s = d.get("stats", {}); b = d.get("behavior", {})
    msg = f"[*] *تقرير AURA*\n───────────────\n📊 الإجمالي: {e.get('total_signals',0)} إشارة\n✅ الربح: {e.get('won',0)} | ❌ الخسارة: {e.get('lost',0)}\n📈 Win Rate: {e.get('win_rate',0)}%\n🏆 Entered WR: {e.get('entered_win_rate',0)}%\n📐 Profit Factor: {e.get('profit_factor',0)}\n⭐ Avg R:R: {e.get('avg_rr',0)}\n📉 Max DD: {e.get('max_drawdown',0)}%\n📊 Sharpe: {e.get('sharpe',0)}\n─── سلوك ───\n🥇 أفضل أصل: {b.get('best_asset','?')}\n🥉 أسوأ أصل: {b.get('worst_asset','?')}\n🔥 أطول فوز: {b.get('streaks',{}).get('max_win_streak',0)} | أطول خسارة: {b.get('streaks',{}).get('max_loss_streak',0)}\n📊 دقة الدخول: {b.get('enter_accuracy',0)}% | دقة التخطي: {b.get('skip_accuracy',0)}%\n───────────────\n🤖 *AURA CLOUD*"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def notes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id); save_json('users.json', list(authorized_users))
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_text("استعمال:\n`/notes <id> <ملاحظة>`\nلعرض الملاحظات:\n`/notes <id>`", parse_mode="Markdown")
    sig_id = args[0]
    for s in signal_log:
        if s.get("id") == sig_id:
            if len(args) == 1:
                n = s.get("notes","") or "لا توجد ملاحظات"
                return await update.message.reply_text(f"📝 *ملاحظات {s.get('asset')}*:\n{n}", parse_mode="Markdown")
            note = " ".join(args[1:])
            s["notes"] = (s.get("notes","") + f"\n[{datetime.now().strftime('%H:%M')}] {note}").strip()
            save_json("signals.json", signal_log)
            return await update.message.reply_text(f"✅ تم حفظ الملاحظة للإشارة {sig_id}")
    await update.message.reply_text("⚠️ الإشارة غير موجودة")

async def provider_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        authorized_users.add(update.effective_user.id); save_json('users.json', list(authorized_users))
    p = AI_PROVIDERS[_current_provider]
    txt = f"[⚡] *مزود AI الحالي:* `{p['name']}`\nموديل: `{p['model']}`\n"
    if len(AI_PROVIDERS) > 1:
        txt += f"\n[*] الاحتياطي: `{AI_PROVIDERS[1]['name']}` ({AI_PROVIDERS[1]['model']})"
        if _current_provider == 0:
            txt += "\n✅ الأساسي يعمل"
        else:
            txt += "\n⚠️ على الاحتياطي — الأساسي مستنفذ"
    await update.message.reply_text(txt, parse_mode="Markdown")

async def daily_report(context):
    d = json.loads(generate_data_json())
    e = d.get("eval", {}); b = d.get("behavior", {})
    msg = f"[📅] *تقرير AURA اليومي*\n───────────────\n📊 الإجمالي: {e.get('total_signals',0)}\n✅ ربح: {e.get('won',0)} | ❌ خسارة: {e.get('lost',0)}\n📈 Win Rate: {e.get('win_rate',0)}%\n🏆 Entered WR: {e.get('entered_win_rate',0)}%\n📐 PF: {e.get('profit_factor',0)} | ⭐ R:R: {e.get('avg_rr',0)}\n📉 Max DD: {e.get('max_drawdown',0)}%\n── سلوك ──\n🥇 {b.get('best_asset','?')} | أطول فوز: {b.get('streaks',{}).get('max_win_streak',0)}\n🤖 *AURA CLOUD*"
    if RADAR_CHAT_ID:
        await context.bot.send_message(chat_id=RADAR_CHAT_ID, text=msg, parse_mode="Markdown")

class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer): pass
def start_api_server():
    httpd = ThreadedHTTPServer(("0.0.0.0", API_PORT), SignalAPIHandler)
    logging.info(f"API server on port {API_PORT}")
    httpd.serve_forever()

# ─── MAIN ───
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("unlock", unlock_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("users", users_cmd))
    app.add_handler(CommandHandler("alert", alert_cmd))
    app.add_handler(CommandHandler("backtest", backtest_cmd))
    app.add_handler(CommandHandler("web", web_cmd))
    app.add_handler(CommandHandler("notes", notes_cmd))
    app.add_handler(CommandHandler("note", notes_cmd))
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("provider", provider_cmd))
    app.add_handler(CommandHandler("analyze", analyze_cmd))
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_cmd))
    app.add_error_handler(error_handler)
    app.job_queue.run_repeating(radar_task, interval=RADAR_INTERVAL, first=10)
    app.job_queue.run_repeating(check_alerts, interval=60, first=30)
    app.job_queue.run_daily(daily_report, time=dt_time(hour=20, minute=0))

    # Start API server and wait for it to be ready
    import urllib.request
    api_ready = threading.Event()
    def start_api_and_signal():
        start_api_server()
        api_ready.set()
    pt = threading.Thread(target=_refresh_price_cache, daemon=True)
    pt.start()
    tt = threading.Thread(target=_track_loop, daemon=True)
    tt.start()
    t = threading.Thread(target=start_api_and_signal, daemon=True)
    t.start()
    for _ in range(20):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{API_PORT}/health", timeout=2)
            logging.info("API server ready")
            break
        except Exception:
            time.sleep(0.5)
    else:
        logging.warning("API server may not be ready")

    logging.info(" AURA CLOUD V4")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    while True:
        try:
            try:
                asyncio.get_event_loop().close()
            except:
                pass
            asyncio.set_event_loop(asyncio.new_event_loop())
            main()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            logging.critical(f"CRASH: {e}\n{traceback.format_exc()}")
            import gc; gc.collect()
            logging.info("Restarting in 5s...")
            time.sleep(5)
