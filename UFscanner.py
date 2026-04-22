import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# =========================
# SECRETS
# =========================
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]
GOOGLE_SHEET_ID  = st.secrets["GOOGLE_SHEET_ID"]

# =========================
# GOOGLE SHEETS — CONNESSIONE
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_gsheet_client():
    try:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
        if isinstance(raw, str):
            service_account_info = json.loads(raw)
        else:
            service_account_info = dict(raw)
        creds  = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.warning(f"⚠️ Google Sheets non disponibile: {e}")
        return None

def get_sheet(sheet_name: str):
    client = get_gsheet_client()
    if client is None:
        return None
    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        st.warning(f"⚠️ Foglio '{sheet_name}' non trovato: {e}")
        return None
def calculate_whale_score(row):
    score = 0    
    try:    
        if float(row.get('voi', 0)) >= 1.0: score += 1
        if float(row.get('ask_hit_val', 0)) >= 70: score += 1
        if row.get('flow') == 'SWEEP': score += 1
        if float(row.get('premium', 0)) >= 100000: score += 1
        price = float(row.get('underlying_price', 0))
        strike = float(row.get('strike', 0))
        if price > 0:
            dist = abs(strike - price) / price
            if 0.02 <= dist <= 0.15: score += 1
    except:
        pass
    return "⭐" * score if score > 0 else "💤"

async def get_ask_hit_value(session, symbol, api_key):
    url = f"https://api.polygon.io/v2/last/nbbo/{symbol}?apiKey={api_key}"
    try:
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                res = data.get("results", {})
                if not res: return 50.0
                return 100.0 if (res.get('p', 0) >= res.get('ap', 0) and res.get('ap', 0) > 0) else 0.0
    except:
        return 50.0
    return 50.0

# =========================
# CONFIG UI
# =========================
st.set_page_config(layout="wide", page_title="Options Flow Scanner PRO")
st.title("🔥 Options Flow Scanner PRO 🔥 by Ugo Fortezze")
st.caption("Powered by Polygon.io — Greeks | Ask Hit | Sweep | Storico Cluster")
_gs_client = get_gsheet_client()
if _gs_client:
    st.caption("📊 Google Sheets: ✅ connesso — storico e watchlist persistenti")
else:
    st.caption("📊 Google Sheets: ⚠️ non connesso — storico salvato localmente")

# =========================
# STORICO SCANSIONI
# =========================
HISTORY_COLS  = ["date","ticker","strike","expiration","type","flow","voi","ask_hit","sweep","iv"]
WATCHLIST_COLS = ["ticker","strike","expiration","type","note","added"]
WATCHLIST_HISTORY_COLS = ["date","ticker","strike","expiration","type","mid","voi","iv","volume","underlying"]

@st.cache_data(ttl=300)
def load_history() -> list:
    sheet = get_sheet("history")
    if sheet:
        try:
            rows = sheet.get_all_records()
            return rows
        except Exception:
            pass
    if os.path.exists("scan_history.json"):
        try:
            with open("scan_history.json", "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []

_history_buffer = []

def save_history_row(entry: dict):
    global _history_buffer
    _history_buffer.append(entry)

def flush_history_buffer():
    global _history_buffer
    if not _history_buffer:
        return
    import time
    sheet = get_sheet("history")
    if sheet:
        for attempt in range(3):
            try:
                existing = sheet.row_values(1)
                if not existing:
                    sheet.append_row(HISTORY_COLS)
                rows = [[str(e.get(c, "")) for c in HISTORY_COLS] for e in _history_buffer]
                sheet.append_rows(rows)
                _history_buffer = []
                load_history.clear()
                return True
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                break
    try:
        history = []
        if os.path.exists("scan_history.json"):
            with open("scan_history.json", "r") as f:
                history = json.load(f)
        history.extend(_history_buffer)
        history = history[-500:]
        with open("scan_history.json", "w") as f:
            json.dump(history, f)
        _history_buffer = []
    except Exception:
        pass
    return False

def add_to_history(ticker, strike, expiration, contract_type, flow_power, voi, ask_hit, sweep, iv=None):
    today_str = datetime.today().strftime("%Y-%m-%d")
    for e in _history_buffer:
        if (str(e.get("date","")) == today_str and
            str(e.get("ticker","")) == str(ticker) and
            str(e.get("strike","")) == str(strike) and
            str(e.get("expiration","")) == str(expiration) and
            str(e.get("type","")) == str(contract_type)):
            return
    entry = {
        "date":       today_str,
        "ticker":     str(ticker),
        "strike":     str(strike),
        "expiration": str(expiration),
        "type":       str(contract_type),
        "flow":       round(float(flow_power), 0),
        "voi":        round(float(voi), 2),
        "ask_hit":    round(float(ask_hit), 1) if ask_hit is not None else None,
        "sweep":      str(sweep),
        "iv":         round(float(iv), 2) if iv is not None else None,
    }
    save_history_row(entry)

def get_cluster_repeat(ticker, strike, expiration, contract_type, history=None) -> int:
    if history is None:
        history = load_history()
    days = set()
    for e in history:
        if (str(e.get("ticker","")) == str(ticker) and
            str(e.get("strike","")) == str(strike) and
            str(e.get("expiration","")) == str(expiration) and
            str(e.get("type","")) == str(contract_type)):
            days.add(str(e.get("date","")))
    return len(days)

@st.cache_data(ttl=3600, show_spinner=False)
def get_next_earnings(ticker: str) -> str | None:
    try:
        url = f"https://api.polygon.io/vX/reference/tickers/{ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY}, timeout=8)
        if r.status_code == 200:
            data = r.json().get("results", {})
            earnings = data.get("next_earnings_date") or data.get("earnings_date")
            if earnings: return str(earnings)
    except Exception: pass
    try:
        url2 = f"https://api.polygon.io/vX/reference/financials"
        r2 = requests.get(url2, params={"apiKey": POLYGON_API_KEY, "ticker": ticker,
            "timeframe": "quarterly", "limit": 1, "order": "desc"}, timeout=8)
        if r2.status_code == 200:
            results = r2.json().get("results", [])
            if results:
                last_period = results[0].get("end_date","")
                if last_period:
                    last_dt = datetime.strptime(last_period, "%Y-%m-%d")
                    next_dt = last_dt + timedelta(days=91)
                    return next_dt.strftime("%Y-%m-%d")
    except Exception: pass
    return None

def earnings_in_dte(ticker: str, dte_max_days: int) -> tuple[bool, str]:
    earnings_date = get_next_earnings(ticker)
    if not earnings_date: return False, ""
    try:
        today = datetime.today().date()
        ed = datetime.strptime(earnings_date, "%Y-%m-%d").date()
        days_to_earnings = (ed - today).days
        if 0 <= days_to_earnings <= dte_max_days:
            return True, earnings_date
    except Exception: pass
    return False, ""

def compute_score(voi, ask_hit, sweep, whale_days, flow_num, has_earn, iv=None):
    score = 0
    try:
        v = float(voi)
        if   v >= 10: score += 30
        elif v >= 5:  score += 22
        elif v >= 3:  score += 15
        elif v >= 2:  score += 10
        elif v >= 1:  score += 5
    except: pass
    try:
        f = float(flow_num)
        if   f >= 1_000_000: score += 20
        elif f >= 500_000:   score += 15
        elif f >= 200_000:   score += 10
        elif f >= 100_000:   score += 5
        elif f >= 50_000:    score += 2
    except: pass
    try:
        if ask_hit is not None:
            ah = float(ask_hit)
            if   ah >= 80: score += 20
            elif ah >= 70: score += 15
            elif ah >= 60: score += 10
            elif ah >= 50: score += 5
            elif ah <= 30: score -= 5
    except: pass
    try:
        d = int(whale_days)
        if   d >= 5: score += 15
        elif d >= 3: score += 10
        elif d >= 2: score += 6
        elif d >= 1: score += 2
    except: pass
    if sweep == "🌊": score += 10
    if has_earn: score -= 15
    return max(0, min(100, score))

def score_label(score: int) -> str:
    if score >= 75: return f"🔥 {score}"
    if score >= 50: return f"⚡ {score}"
    if score >= 30: return f"👀 {score}"
    return f"💤 {score}"

def get_voi_baseline(ticker, strike, expiration, contract_type, history=None) -> dict:
    if history is None:
        history = load_history()
    voi_values = []
    for e in history:
        if (str(e.get("ticker",""))==str(ticker) and str(e.get("strike",""))==str(strike) and
            str(e.get("expiration",""))==str(expiration) and str(e.get("type",""))==str(contract_type)):
            try:
                v = float(e.get("voi",0))
                if v > 0: voi_values.append(v)
            except: pass
    if len(voi_values) < 2:
        return {"mean": None, "count": len(voi_values), "anomaly_pct": None}
    mean_voi = sum(voi_values) / len(voi_values)
    return {"mean": round(mean_voi,2), "count": len(voi_values), "anomaly_pct": None}

def voi_anomaly_label(current_voi: float, baseline: dict) -> str:
    if baseline["mean"] is None or baseline["count"] < 2: return ""
    mean = baseline["mean"]
    if mean <= 0: return ""
    pct = round(((current_voi - mean) / mean) * 100, 0)
    count = baseline["count"]
    if pct >= 100: return f"🚀 +{int(pct)}% vs {mean:.1f} ({count}d)"
    elif pct >= 50: return f"📈 +{int(pct)}% vs {mean:.1f} ({count}d)"
    elif pct >= 0:  return f"➡️ +{int(pct)}% vs {mean:.1f} ({count}d)"
    else:           return f"📉 {int(pct)}% vs {mean:.1f} ({count}d)"

# =========================
# SENTIMENT PER STRIKE
# =========================

def compute_strike_sentiment(df_full: pd.DataFrame) -> pd.DataFrame:
    if df_full.empty: return pd.DataFrame()
    rows = []
    for strike in sorted(df_full["strike"].unique()):
        calls = df_full[(df_full["strike"]==strike) & (df_full["type"]=="CALL")]
        puts  = df_full[(df_full["strike"]==strike) & (df_full["type"]=="PUT")]
        call_vol  = int(calls["volume"].sum()) if not calls.empty else 0
        put_vol   = int(puts["volume"].sum())  if not puts.empty else 0
        call_flow = round(calls["FLOW_POWER_NUM"].sum(),0) if not calls.empty else 0
        put_flow  = round(puts["FLOW_POWER_NUM"].sum(),0)  if not puts.empty else 0
        if call_vol==0 and put_vol==0: continue
        if call_vol>0 and put_vol>0:   ratio = round(call_vol/put_vol,2)
        elif call_vol>0:               ratio = 99.0
        else:                          ratio = 0.01
        if   ratio >= 2.0: sentiment,color = "📈 BULLISH","bull"
        elif ratio <= 0.5: sentiment,color = "📉 BEARISH","bear"
        else:              sentiment,color = "⚖️ NEUTRO","neut"
        rows.append({"Strike":strike,"CALL vol":call_vol,"PUT vol":put_vol,
                     "C/P Ratio":ratio,"Sentiment":sentiment,
                     "Flow CALL":format_k(call_flow) if call_flow>0 else "—",
                     "Flow PUT":format_k(put_flow) if put_flow>0 else "—","_color":color})
    return pd.DataFrame(rows) if rows else pd.DataFrame()

# =========================
# WATCHLIST
# =========================

@st.cache_data(ttl=300)
def load_watchlist() -> list:
    sheet = get_sheet("watchlist")
    if sheet:
        try:
            rows = sheet.get_all_records()
            return rows
        except Exception:
            pass
    if os.path.exists("watchlist.json"):
        try:
            with open("watchlist.json", "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_watchlist(wl: list):
    sheet = get_sheet("watchlist")
    if sheet:
        try:
            sheet.clear()
            sheet.append_row(WATCHLIST_COLS)
            for e in wl:
                row = [str(e.get(c, "")) for c in WATCHLIST_COLS]
                sheet.append_row(row)
            load_watchlist.clear()
            return True
        except Exception:
            pass
    try:
        with open("watchlist.json", "w") as f:
            json.dump(wl, f)
    except Exception:
        pass
    return False

def add_to_watchlist(ticker: str, strike: float, expiration: str, contract_type: str, note: str = ""):
    wl = load_watchlist()
    key = (str(ticker), str(strike), str(expiration), str(contract_type))
    existing = {(str(e.get("ticker","")), str(e.get("strike","")),
                 str(e.get("expiration","")), str(e.get("type",""))) for e in wl}
    if key in existing:
        return False
    entry = {
        "ticker": str(ticker), "strike": str(strike),
        "expiration": str(expiration), "type": str(contract_type),
        "note": str(note), "added": datetime.today().strftime("%Y-%m-%d"),
    }
    sheet = get_sheet("watchlist")
    if sheet:
        try:
            existing_header = sheet.row_values(1)
            if not existing_header:
                sheet.append_row(WATCHLIST_COLS)
            sheet.append_row([str(entry.get(c,"")) for c in WATCHLIST_COLS])
            load_watchlist.clear()
            return True
        except Exception:
            pass
    try:
        wl.append(entry)
        with open("watchlist.json", "w") as f:
            json.dump(wl, f)
        return True
    except Exception:
        return False

def save_watchlist_snapshot(ticker, strike, expiration, contract_type, mid, voi, iv, volume, underlying):
    today_str = datetime.today().strftime("%Y-%m-%d")
    sheet = get_sheet("wl_history")
    if sheet:
        import time
        for attempt in range(3):
            try:
                all_vals = sheet.get_all_values()
                if not all_vals or all_vals[0] != WATCHLIST_HISTORY_COLS:
                    sheet.clear()
                    sheet.append_row(WATCHLIST_HISTORY_COLS)
                existing = sheet.get_all_records()
                for e in existing:
                    if (str(e.get("date",""))==today_str and str(e.get("ticker",""))==str(ticker) and
                        str(e.get("strike",""))==str(strike) and str(e.get("expiration",""))==str(expiration) and
                        str(e.get("type",""))==str(contract_type)):
                        return
                row = [today_str,str(ticker),str(strike),str(expiration),str(contract_type),
                       str(round(float(mid),2)) if mid else "",str(round(float(voi),2)) if voi else "",
                       str(round(float(iv),1)) if iv else "",str(int(volume)) if volume else "",
                       str(round(float(underlying),2)) if underlying else ""]
                sheet.append_row(row)
                return True
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    time.sleep(2**attempt); continue
                break
    try:
        hist = []
        if os.path.exists("wl_history.json"):
            with open("wl_history.json","r") as f: hist = json.load(f)
        hist.append({"date":today_str,"ticker":str(ticker),"strike":str(strike),
                     "expiration":str(expiration),"type":str(contract_type),
                     "mid":mid,"voi":voi,"iv":iv,"volume":volume,"underlying":underlying})
        with open("wl_history.json","w") as f: json.dump(hist,f)
    except Exception: pass

def load_watchlist_history(ticker=None, strike=None, expiration=None, contract_type=None) -> list:
    sheet = get_sheet("wl_history")
    rows = []
    if sheet:
        try: rows = sheet.get_all_records()
        except Exception: pass
    if not rows and os.path.exists("wl_history.json"):
        try:
            with open("wl_history.json","r") as f: rows = json.load(f)
        except Exception: pass
    if ticker:
        rows = [r for r in rows if str(r.get("ticker",""))==str(ticker) and
                str(r.get("strike",""))==str(strike) and str(r.get("expiration",""))==str(expiration) and
                str(r.get("type",""))==str(contract_type)]
    return rows

# =========================
# MODALITÀ
# =========================
mode = st.radio(
    "Modalità Trading",
    ["SMALL CAP", "MID CAP", "BIG CAP", "SNIPER", "HOT ONLY", "SPY SWING"],
    horizontal=True,
    key="mode_radio"
)

PRESETS = {
    "SMALL CAP": {
        "volume_min": 100, "voi_min": 1.5, "dte_max": 60, "dte_min": 2,
        "strike_dist_min": 0, "strike_dist_max": 20, "spread_max": 20.0,
        "delta_min": 0.05, "delta_max": 0.95, "ask_hit_min": 0.0, "flow_min": 0,
        "desc": "Small Cap (<$2B) — bassa liquidità, filtri adattati"
    },
    "MID CAP": {
        "volume_min": 200, "voi_min": 1.2, "dte_max": 90, "dte_min": 2,
        "strike_dist_min": 0, "strike_dist_max": 15, "spread_max": 20.0,
        "delta_min": 0.10, "delta_max": 0.90, "ask_hit_min": 0.0, "flow_min": 0,
        "desc": "Mid Cap ($2B-$10B) — bilanciato tra liquidità e segnale"
    },
    "BIG CAP": {
        "volume_min": 500, "voi_min": 1.0, "dte_max": 120, "dte_min": 1,
        "strike_dist_min": 0, "strike_dist_max": 12, "spread_max": 20.0,
        "delta_min": 0.10, "delta_max": 0.90, "ask_hit_min": 0.0, "flow_min": 0,
        "desc": "Big Cap (>$10B) — alta liquidità, filtri standard"
    },
    "SNIPER": {
        "volume_min": 1000, "voi_min": 2.0, "dte_max": 14, "dte_min": 1,
        "strike_dist_min": 0, "strike_dist_max": 5, "spread_max": 20.0,
        "delta_min": 0.20, "delta_max": 0.80, "ask_hit_min": 55.0, "flow_min": 0,
        "desc": "SNIPER — strike vicino, scadenza breve, alta pressione"
    },
    "HOT ONLY": {
        "volume_min": 2000, "voi_min": 3.0, "dte_max": 7, "dte_min": 1,
        "strike_dist_min": 0, "strike_dist_max": 3, "spread_max": 20.0,
        "delta_min": 0.25, "delta_max": 0.75, "ask_hit_min": 60.0, "flow_min": 0,
        "desc": "HOT ONLY — solo flussi anomali estremi, scadenza imminente"
    },
    "SPY SWING": {
        "volume_min": 100, "voi_min": 0.5, "dte_max": 210, "dte_min": 60,
        "strike_dist_min": 0, "strike_dist_max": 15, "spread_max": 20.0,
        "delta_min": 0.05, "delta_max": 0.80, "ask_hit_min": 0.0, "flow_min": 50000,
        "desc": "SPY SWING — DTE 60-210gg | Flow >$50K | Allarga i filtri, poi stringi manualmente"
    },
}

preset = PRESETS[mode]
st.caption(f"ℹ️ {preset['desc']}")

APP_VERSION = "5.2"
if ("last_mode" not in st.session_state or
    st.session_state.get("last_mode") != mode or
    st.session_state.get("app_version") != APP_VERSION):
    st.session_state["app_version"]     = APP_VERSION
    st.session_state["last_mode"]       = mode
    st.session_state["volume_min"]      = preset["volume_min"]
    st.session_state["voi_min"]         = float(preset["voi_min"])
    st.session_state["dte_max"]         = preset["dte_max"]
    st.session_state["dte_min"]         = preset["dte_min"]
    st.session_state["strike_dist_min"] = preset["strike_dist_min"]
    st.session_state["strike_dist_max"] = preset["strike_dist_max"]
    st.session_state["spread_max"]      = float(preset["spread_max"])
    st.session_state["delta_min"]       = float(preset["delta_min"])
    st.session_state["delta_max"]       = float(preset["delta_max"])
    st.session_state["ask_hit_min"]     = float(preset["ask_hit_min"])
    st.session_state["flow_min"]        = int(preset["flow_min"])

col_s1, col_s2 = st.columns(2)
with col_s1:
    volume_min      = st.slider("Volume minimo (contratti)",  0,     50000,   st.session_state["volume_min"],               key="volume_min")
    voi_min         = st.slider("VOI minimo (vol/OI)",        0.0,   20.0,    st.session_state["voi_min"],    step=0.1,     key="voi_min")
    dte_min         = st.slider("DTE minimo (giorni)",        0,     180,     st.session_state["dte_min"],                  key="dte_min")
    dte_max         = st.slider("DTE massimo (giorni)",       1,     365,     st.session_state["dte_max"],                  key="dte_max")
    strike_dist_min = st.slider("Distanza strike % minima",   0,     30,      st.session_state["strike_dist_min"],          key="strike_dist_min",
                                help="Esclude ITM e ATM. Es: 5 = considera solo contratti OTM oltre il 5%")
with col_s2:
    strike_dist_max = st.slider("Distanza strike % massima",  1,     50,      st.session_state["strike_dist_max"],          key="strike_dist_max")
    spread_max      = st.slider("Spread bid/ask max ($)",     0.01,  20.0,    st.session_state["spread_max"],  step=0.01,   key="spread_max")
    delta_min       = st.slider("Delta minimo",               0.0,   1.0,     st.session_state["delta_min"],   step=0.01,   key="delta_min")
    delta_max       = st.slider("Delta massimo",              0.0,   1.0,     st.session_state["delta_max"],   step=0.01,   key="delta_max")
    flow_min        = st.slider("Flow $ minimo (smart money)",0,     5000000, st.session_state["flow_min"],    step=50000,  key="flow_min",
                                help=">$500K = istituzionale. >$1M = whale.")

ask_hit_min = st.slider(
    "Ask Hit % minimo (0 = mostra tutto)",
    0.0, 100.0, st.session_state["ask_hit_min"], step=5.0, key="ask_hit_min",
    help="≥55% = buyer aggressivo. ≤30% = seller. 30-55% = neutro."
)

col1, col2 = st.columns(2)
with col1:
    option_type = st.radio("Tipo opzione", ["CALL", "PUT", "BOTH"], horizontal=True)
with col2:
    send_telegram = st.checkbox("📲 Attiva Telegram Alerts", value=False)

tickers_input = st.text_input("Ticker (separati da virgola)", "SPY")

# =========================
# HELPERS
# =========================

def format_k(x):
    if x >= 1_000_000: return f"{x/1_000_000:.1f}M"
    elif x >= 1_000:   return f"{x/1_000:.1f}K"
    return str(int(x))

def send_telegram_message(text: str) -> bool:
    """Invia messaggio Telegram. Tronca automaticamente a 4096 caratteri."""
    if len(text) > 4000:
        text = text[:4000] + "\n\n... [troncato]"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        # Rimuove tag HTML per evitare errori di parsing
        import re
        plain = re.sub(r"<[^>]+>", "", text)
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": plain}, timeout=5)
        return r.ok
    except Exception as e:
        st.warning(f"Telegram error: {e}")
        return False

# =========================
# POLYGON — PREZZO SOTTOSTANTE
# =========================

def get_stock_price(ticker: str) -> float | None:
    try:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY}, timeout=8)
        if r.status_code == 200:
            data = r.json().get("ticker", {})
            price = (data.get("day", {}).get("c") or data.get("day", {}).get("o") or
                     data.get("prevDay", {}).get("c") or data.get("lastTrade", {}).get("p"))
            if price and float(price) > 0.5:
                return round(float(price), 2)
    except Exception:
        pass
    try:
        url2 = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
        r2 = requests.get(url2, params={"apiKey": POLYGON_API_KEY}, timeout=8)
        if r2.status_code == 200:
            res = r2.json().get("results", [])
            if res: return round(float(res[0]["c"]), 2)
    except Exception:
        pass
    return None

# =========================
# POLYGON — CATENA OPZIONI
# =========================

def get_options_chain(ticker: str, dte_min_d: int, dte_max_d: int) -> list[dict]:
    today   = datetime.today().date()
    exp_min = (today + timedelta(days=dte_min_d)).isoformat()
    exp_max = (today + timedelta(days=dte_max_d)).isoformat()
    all_rows, url = [], f"https://api.polygon.io/v3/snapshot/options/{ticker}"
    params = {"apiKey": POLYGON_API_KEY, "expiration_date.gte": exp_min,
              "expiration_date.lte": exp_max, "limit": 250}
    page_count = 0
    while url and page_count < 20:
        try:
            r = requests.get(url, params=params, timeout=12)
            if r.status_code != 200: break
            data = r.json()
        except Exception:
            break
        all_rows.extend(data.get("results", []))
        next_url = data.get("next_url")
        url    = next_url + f"&apiKey={POLYGON_API_KEY}" if next_url else None
        params = {}
        page_count += 1
    return all_rows

# =========================
# ASK HIT REALE + SWEEP
# =========================

@st.cache_data(ttl=120, show_spinner=False)
def get_ask_hit_real(contract_ticker: str, bid: float, ask: float) -> tuple[float | None, bool]:
    if not contract_ticker: return None, False
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/trades/{contract_ticker}",
            params={"apiKey": POLYGON_API_KEY, "limit": 50, "order": "desc", "sort": "timestamp"},
            timeout=8
        )
        if r.status_code != 200: return None, False
        trades = r.json().get("results", [])
        if not trades: return None, False

        mid        = (bid + ask) / 2 if (bid > 0 and ask > 0) else None
        ask_hits   = 0
        total_seen = 0
        exchanges  = set()
        skip_cond  = {12, 13, 14, 15, 16, 17, 18, 41}

        for t in trades:
            price = t.get("price", 0)
            if t.get("exchange"): exchanges.add(t["exchange"])
            if any(c in skip_cond for c in (t.get("conditions") or [])): continue
            if price <= 0: continue
            total_seen += 1
            if mid is not None:
                if price >= ask * 0.995: ask_hits += 1
            else:
                if t.get("aggressor_side", "") == "buyer": ask_hits += 1

        if total_seen == 0: return None, False
        return round((ask_hits / total_seen) * 100, 1), len(exchanges) >= 2
    except Exception:
        return None, False

# =========================
# PARSE + FILTRI
# =========================

def parse_and_filter(raw: list[dict], underlying: float, ticker: str) -> pd.DataFrame:
    rows  = []
    today = pd.Timestamp.today().normalize()

    for item in raw:
        details = item.get("details", {})
        greeks  = item.get("greeks", {})
        day     = item.get("day", {})
        quotes  = item.get("last_quote", {})

        contract_type = details.get("contract_type", "").upper()
        strike        = details.get("strike_price")
        exp_str       = details.get("expiration_date", "")
        ticker_sym    = details.get("ticker", "")

        if not strike or not exp_str: continue
        try:    exp_dt = pd.to_datetime(exp_str)
        except: continue

        volume    = day.get("volume") or 0
        oi        = item.get("open_interest") or 0
        day_close = day.get("close") or 0
        day_vwap  = day.get("vwap")  or 0
        mid = day_close if day_close > 0 else (day_vwap if day_vwap > 0 else 0)

        if mid > 0:
            half_spread = max(0.01, round(mid * 0.015, 2))
            bid    = round(mid - half_spread, 2)
            ask    = round(mid + half_spread, 2)
            spread = round(ask - bid, 2)
        else:
            bid = ask = spread = None

        iv    = item.get("implied_volatility") or 0
        delta = greeks.get("delta")
        gamma = greeks.get("gamma")
        theta = greeks.get("theta")
        vega  = greeks.get("vega")

        rows.append({
            "ticker_sym": ticker_sym, "type": contract_type,
            "strike": strike, "expiration": exp_dt, "exp_str": exp_str,
            "volume": int(volume), "OI": int(oi),
            "MID": round(mid, 2),
            "bid": bid, "ask": ask, "SPREAD": spread,
            "IV":    round(iv * 100, 1) if iv else None,
            "delta": round(abs(delta), 3) if delta is not None else None,
            "gamma": round(gamma, 4)      if gamma is not None else None,
            "theta": round(theta, 3)      if theta is not None else None,
            "vega":  round(vega, 3)       if vega is not None else None,
        })

    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)

    df["VOI"]            = (df["volume"] / df["OI"].replace(0, 1)).round(2)
    df["DTE"]            = (df["expiration"] - today).dt.days
    df["DIST_STRIKE"]    = ((df["strike"] - underlying).abs() / underlying * 100).round(1)
    df["UNDER"]          = underlying
    df["FLOW_POWER_NUM"] = (df["volume"] * df["MID"]).round(0)

    df = df[df["volume"]         >= volume_min]
    df = df[df["VOI"]            >= voi_min]
    df = df[df["DTE"]            >= dte_min]
    df = df[df["DTE"]            <= dte_max]
    df = df[df["DIST_STRIKE"]    >= strike_dist_min]
    df = df[df["DIST_STRIKE"]    <= strike_dist_max]
    df = df[df["FLOW_POWER_NUM"] >= flow_min]

    if spread_max < 20.0:
        df = df[(df["SPREAD"].isna()) | (df["SPREAD"] <= spread_max)]
    df = df[df["delta"].isna() | ((df["delta"] >= delta_min) & (df["delta"] <= delta_max))]

    calls_all = df[df["type"] == "CALL"]["volume"].sum()
    puts_all  = df[df["type"] == "PUT"]["volume"].sum()
    df.attrs["pc_ratio"]  = round(puts_all / calls_all, 2) if calls_all > 0 else 0
    df.attrs["calls_vol"] = int(calls_all)
    df.attrs["puts_vol"]  = int(puts_all)

    if option_type != "BOTH":
        df = df[df["type"] == option_type]
    if df.empty: return df

    df["OPZIONE"] = (
        ticker + " " + df["expiration"].dt.strftime("%b").str.upper() + " " +
        df["expiration"].dt.strftime("%d") + " '" + df["expiration"].dt.strftime("%y") + " " +
        df["strike"].apply(lambda x: str(int(x)) if x == int(x) else str(x)) + " " +
        df["type"].str[0]
    )
    df["CLUSTER"] = df.groupby("strike")["volume"].transform("sum").apply(format_k)
    df["FLOW $"]  = df["FLOW_POWER_NUM"].apply(format_k)

    def signal(row):
        try:    voi = float(row["VOI"])
        except: voi = 0
        if voi >= 5.0: return "🟢 GO"
        if voi >= 2.0: return "🟡 HOLD"
        return "🔴 STOP"

    df["SIG"]  = df.apply(signal, axis=1)
    df["BIAS"] = df["type"].apply(lambda x: "📈 L" if x == "CALL" else "📉 S")

    def check_earnings(row):
        has_earn, earn_date = earnings_in_dte(ticker, int(row["DTE"]))
        if has_earn: return f"⚠️ {earn_date}"
        return ""
    df["EARN"] = df.apply(check_earnings, axis=1)
    df["ITM"]  = df.apply(lambda r: "✅" if (r["type"]=="CALL" and r["strike"]<underlying)
                                         or (r["type"]=="PUT"  and r["strike"]>underlying) else "", axis=1)
    df["ATM"]  = df["DIST_STRIKE"].apply(lambda x: "🎯" if x <= 1.0 else "")
    df["OTM"]  = df.apply(lambda r: "⬆️" if (r["type"]=="CALL" and r["strike"]>underlying)
                                         or (r["type"]=="PUT"  and r["strike"]<underlying) else "", axis=1)
    return df.sort_values("FLOW_POWER_NUM", ascending=False)

# =========================
# ARRICCHIMENTO ASK HIT + SWEEP + STORICO
# =========================

def enrich_with_flow_data(df: pd.DataFrame, ticker: str, top_n: int = 15) -> pd.DataFrame:
    df = df.copy()
    df["ASK_HIT"]  = None
    df["SWEEP"]    = ""
    df["🐋 DAYS"]  = 0
    df["SCORE"]    = ""
    df["VOI_ANOM"] = ""

    cached_history = load_history()
    idx_list = df.head(top_n).index.tolist()
    progress = st.progress(0, text="📡 Analisi flusso in corso...")

    for i, idx in enumerate(idx_list):
        row = df.loc[idx]
        ask_hit_pct, is_sweep = get_ask_hit_real(
            row.get("ticker_sym",""), row.get("bid",0), row.get("ask",0)
        )
        sweep_str = "🌊" if is_sweep else ""
        df.at[idx, "ASK_HIT"] = ask_hit_pct
        df.at[idx, "SWEEP"]   = sweep_str

        add_to_history(ticker, row.get("strike"), row.get("exp_str",""),
                       row.get("type",""), row.get("FLOW_POWER_NUM",0),
                       row.get("VOI",0), ask_hit_pct, sweep_str)

        whale_days = get_cluster_repeat(
            ticker, row.get("strike"), row.get("exp_str",""), row.get("type",""),
            history=cached_history
        )
        df.at[idx, "🐋 DAYS"] = whale_days

        baseline = get_voi_baseline(
            ticker, row.get("strike"), row.get("exp_str",""), row.get("type",""),
            history=cached_history
        )
        try:    current_voi = float(row.get("VOI",0))
        except: current_voi = 0
        df.at[idx, "VOI_ANOM"] = voi_anomaly_label(current_voi, baseline)

        has_earn_flag = bool(row.get("EARN",""))
        score = compute_score(
            voi=current_voi, ask_hit=ask_hit_pct, sweep=sweep_str,
            whale_days=whale_days, flow_num=row.get("FLOW_POWER_NUM",0),
            has_earn=has_earn_flag, iv=row.get("IV")
        )
        df.at[idx, "SCORE"] = score_label(score)
        progress.progress((i+1)/len(idx_list), text=f"📡 Flow analysis: {i+1}/{len(idx_list)}")

    progress.empty()
    flush_history_buffer()

    if ask_hit_min > 0:
        df = df[df["ASK_HIT"].isna() | (df["ASK_HIT"] >= ask_hit_min)]
    return df

# =========================
# WRAPPER
# =========================

def scan_ticker(ticker: str) -> pd.DataFrame:
    underlying = get_stock_price(ticker)
    if underlying is None:
        st.warning(f"⚠️ Impossibile ottenere prezzo per {ticker}")
        return pd.DataFrame()

    st.caption(f"📌 {ticker} — prezzo sottostante: **${underlying}**")
    has_earn, earn_date = earnings_in_dte(ticker, dte_max)
    if has_earn:
        days_to = (datetime.strptime(earn_date, "%Y-%m-%d").date() - datetime.today().date()).days
        st.warning(f"⚠️ **EARNINGS ALERT** — {ticker} ha earnings il **{earn_date}** ({days_to} giorni). "
                   f"Valuta se il segnale è da earnings o da flusso reale.")
    with st.spinner(f"📡 Scaricando opzioni {ticker}..."):
        raw = get_options_chain(ticker, dte_min, dte_max)
    if not raw:
        st.warning(f"⚠️ Nessun dato opzioni da Polygon per {ticker}")
        return pd.DataFrame()

    df = parse_and_filter(raw, underlying, ticker)
    if df.empty: return df

    pc = df.attrs.get("pc_ratio", 0)
    cv = df.attrs.get("calls_vol", 0)
    pv = df.attrs.get("puts_vol", 0)
    bias_label = "📈 BULLISH" if pc < 0.8 else ("📉 BEARISH" if pc > 1.2 else "⚖️ NEUTRO")
    st.caption(f"Put/Call Ratio: **{pc}** | CALL vol: {cv:,} | PUT vol: {pv:,} | Bias: {bias_label}")

    df_enriched = enrich_with_flow_data(df, ticker, top_n=15)

    if not df_enriched.empty:
        sentiment_df = compute_strike_sentiment(df)
        if not sentiment_df.empty:
            with st.expander(f"📊 Sentiment per Strike — {ticker}", expanded=False):
                def hl_sent(val):
                    if "BULLISH" in str(val): return "background-color:#1a3a1a; color:#00ff88"
                    if "BEARISH" in str(val): return "background-color:#3a0a0a; color:#ff4444"
                    return "background-color:#2a2a0a; color:#ffdd00"
                display_sent = sentiment_df.drop(columns=["_color"]).copy()
                st.dataframe(
                    display_sent.style.map(hl_sent, subset=["Sentiment"])
                    .format({"Strike": lambda x: str(int(x)) if x==int(x) else f"{x:.2f}",
                             "C/P Ratio": lambda x: f"{x:.2f}"}),
                    use_container_width=True, hide_index=True
                )
                bulls = sentiment_df[sentiment_df["_color"]=="bull"].nlargest(3,"C/P Ratio")
                bears = sentiment_df[sentiment_df["_color"]=="bear"].nsmallest(3,"C/P Ratio")
                col_b, col_s = st.columns(2)
                with col_b:
                    if not bulls.empty:
                        st.markdown("**🎯 Strike più BULLISH:**")
                        for _, r in bulls.iterrows():
                            st.markdown(f"Strike **{int(r['Strike']) if r['Strike']==int(r['Strike']) else r['Strike']}** — C/P {r['C/P Ratio']:.2f} | CALL {r['CALL vol']:,} vs PUT {r['PUT vol']:,}")
                with col_s:
                    if not bears.empty:
                        st.markdown("**🎯 Strike più BEARISH:**")
                        for _, r in bears.iterrows():
                            st.markdown(f"Strike **{int(r['Strike']) if r['Strike']==int(r['Strike']) else r['Strike']}** — C/P {r['C/P Ratio']:.2f} | CALL {r['CALL vol']:,} vs PUT {r['PUT vol']:,}")

    return df_enriched

# =========================
# LEGENDA / MANUALE IN-APP
# =========================
with st.expander("📖 Manuale — Options Flow Scanner PRO v5.1"):
    st.markdown("""
## 🎯 Obiettivo del Tool
Scanner di flussi istituzionali sulle opzioni USA. Identifica contratti con volumi anomali rispetto all'open interest, con focus su **smart money** e **accumulo balena**. Nessuna esecuzione automatica — il controllo finale è sempre tuo.

---

## 🗂️ Modalità Operative

| Modalità | Descrizione | DTE | VOI min | Flow min |
|---|---|---|---|---|
| **SMALL CAP** | Titoli <$2B, bassa liquidità | 2–60gg | >1.5 | — |
| **MID CAP** | Titoli $2B–$10B, bilanciato | 2–90gg | >1.2 | — |
| **BIG CAP** | Titoli >$10B, alta liquidità | 1–120gg | >1.0 | — |
| **SNIPER** | Strike vicino, scadenza breve | 1–14gg | >2.0 | — |
| **HOT ONLY** | Flussi estremi, scadenza imminente | 1–7gg | >3.0 | — |
| **SPY SWING** | Solo SPY, DTE medio-lungo, smart money | 60–210gg | >1.2 | >$500K |

---

## 📊 Colonne della Griglia

| Colonna | Come leggerla |
|---|---|
| **SCORE** | 🔥≥75 · ⚡≥50 · 👀≥30 · 💤<30 |
| **SIG** | 🟢 GO = VOI≥5 · 🟡 HOLD = VOI≥2 · 🔴 STOP = VOI<2 |
| **FLOW $** | >$500K = istituzionale · >$1M = whale |
| **SWEEP** | 🌊 = ordine su ≥2 exchange simultaneamente |
| **ASK_HIT %** | ≥70% 🟢 buyer aggressivo · ≤30% 🔴 vendita |
| **🐋 DAYS** | Giorni distinti nello storico — ≥2 = accumulo |
| **VOI** | Volume / OI — >1 = soldi freschi |
| **DTE** | Giorni alla scadenza |

---

## ⚠️ Note Operative
- Combinazione più forte: **VOI alto + ASK_HIT ≥55% + SWEEP 🌊 + 🐋 DAYS ≥2**
- Paper trading consigliato per le prime settimane.
- Nessun ordine viene eseguito automaticamente.
""")

# =========================
# STORICO
# =========================
with st.expander("📅 Storico Scansioni — Tracker Accumulo 🐋"):
    history = load_history()
    if not history:
        st.info("Nessuna scansione salvata. Esegui una scansione per iniziare a costruire lo storico.")
    else:
        df_hist = pd.DataFrame(history)
        df_agg = (
            df_hist.groupby(["ticker", "strike", "expiration", "type"])
            .agg(giorni=("date","nunique"), ultimo_flow=("flow","last"),
                 ultimo_voi=("voi","last"), ultima_data=("date","max"))
            .reset_index()
            .sort_values("giorni", ascending=False)
        )
        df_agg["ultimo_flow"] = df_agg["ultimo_flow"].apply(format_k)
        def clean_strike(v):
            try:
                f = float(v)
                return str(int(f)) if f == int(f) else f"{f:.2f}"
            except: return str(v)
        def clean_voi(v):
            try: return f"{float(v):.2f}"
            except: return str(v)
        df_agg["strike"]     = df_agg["strike"].apply(clean_strike)
        df_agg["ultimo_voi"] = df_agg["ultimo_voi"].apply(clean_voi)
        df_agg.columns = ["Ticker","Strike","Scadenza","Tipo","🐋 Giorni","Ultimo Flow","Ultimo VOI","Ultima Data"]

        def hl_whale(val):
            try:
                v = int(val)
                if v >= 3: return "background-color:#1a1a3a; color:#88aaff"
                if v >= 2: return "background-color:#1a3a1a; color:#00ff88"
            except: pass
            return ""

        st.dataframe(df_agg.style.map(hl_whale, subset=["🐋 Giorni"]),
                     use_container_width=True, hide_index=True)
        col_h1, col_h2 = st.columns(2)
        with col_h1:
            st.caption(f"📊 Record totali: {len(df_hist)} | Contratti unici: {len(df_agg)}")
        with col_h2:
            if st.button("🗑️ Cancella storico", type="secondary"):
                sheet = get_sheet("history")
                if sheet:
                    try:
                        sheet.clear()
                        sheet.append_row(HISTORY_COLS)
                        load_history.clear()
                    except Exception:
                        pass
                if os.path.exists("scan_history.json"):
                    os.remove("scan_history.json")
                st.success("Storico cancellato ✅")
                st.rerun()

# =========================
# WATCHLIST
# =========================
with st.expander("⭐ Watchlist — Monitora contratti specifici"):
    wl = load_watchlist()
    st.markdown("**Aggiungi contratto da monitorare:**")
    wl_col1, wl_col2, wl_col3, wl_col4, wl_col5 = st.columns([2,1,2,1,2])
    with wl_col1:
        wl_ticker = st.text_input("Ticker", value="SPY", key="wl_ticker").upper()
    with wl_col2:
        wl_type = st.selectbox("Tipo", ["C", "P"], key="wl_type")
    with wl_col3:
        wl_strike = st.number_input("Strike", min_value=1.0, value=735.0, step=1.0, key="wl_strike")
    with wl_col4:
        wl_exp = st.text_input("Scadenza (YYYY-MM-DD)", value="2026-08-21", key="wl_exp")
    with wl_col5:
        wl_note = st.text_input("Nota (opzionale)", value="", key="wl_note")

    if st.button("➕ Aggiungi alla Watchlist", key="wl_add"):
        ok = add_to_watchlist(wl_ticker, wl_strike, wl_exp, wl_type, wl_note)
        if ok:
            st.success(f"✅ {wl_ticker} {wl_exp} {wl_strike}{wl_type} aggiunto!")
            wl = load_watchlist()
        else:
            st.warning("⚠️ Contratto già in watchlist.")

    if wl:
        st.markdown("---")
        st.markdown("**Contratti monitorati:**")

        if st.button("🔄 Aggiorna tutti i contratti in watchlist", key="wl_refresh"):
            wl_results = []
            prog = st.progress(0, text="📡 Aggiornamento watchlist...")
            for i, entry in enumerate(wl):
                t      = entry.get("ticker","")
                strike = float(entry.get("strike", 0))
                exp    = entry.get("expiration","")
                ctype  = entry.get("type","")
                note   = entry.get("note","")
                added  = entry.get("added","")
                underlying = get_stock_price(t)
                if underlying is None:
                    wl_results.append({"Ticker":t,"Contratto":f"{t} {exp} {strike}{ctype}",
                        "Nota":note,"MID":"—","VOI":"—","IV":"—","🐋 DAYS":"—","Aggiunto":added,"Underlying":"—"})
                    continue
                try:
                    r = requests.get(
                        f"https://api.polygon.io/v3/snapshot/options/{t}",
                        params={"apiKey":POLYGON_API_KEY,"strike_price":strike,
                                "expiration_date":exp,"contract_type":"call" if ctype=="C" else "put","limit":5},
                        timeout=10
                    )
                    results = r.json().get("results",[]) if r.status_code==200 else []
                    match = next((x for x in results
                                  if abs(x.get("details",{}).get("strike_price",0)-strike)<0.01
                                  and x.get("details",{}).get("expiration_date","")==exp), None)
                    if match:
                        day    = match.get("day",{})
                        mid    = day.get("close") or day.get("vwap") or 0
                        oi     = match.get("open_interest") or 0
                        vol    = day.get("volume") or 0
                        iv_raw = match.get("implied_volatility") or 0
                        iv_pct = round(iv_raw*100,1) if iv_raw else None
                        voi    = round(vol/oi,2) if oi>0 else 0
                        days_r = get_cluster_repeat(t, strike, exp, ctype)
                        save_watchlist_snapshot(t, strike, exp, ctype, mid, voi, iv_pct, vol, underlying)
                        wl_results.append({"Ticker":t,"Contratto":f"{t} {exp} {strike}{ctype}",
                            "Nota":note,"Underlying":f"${underlying}","MID":f"${mid:.2f}" if mid else "—",
                            "VOI":f"{voi:.2f}","IV":f"{iv_pct:.1f}%" if iv_pct else "—",
                            "🐋 DAYS":days_r,"Aggiunto":added})
                    else:
                        wl_results.append({"Ticker":t,"Contratto":f"{t} {exp} {strike}{ctype}",
                            "Nota":note,"MID":"n/d","VOI":"n/d","IV":"n/d","🐋 DAYS":"—",
                            "Aggiunto":added,"Underlying":f"${underlying}"})
                except Exception:
                    wl_results.append({"Ticker":t,"Contratto":f"{t} {exp} {strike}{ctype}",
                        "Nota":note,"MID":"err","VOI":"err","IV":"err","🐋 DAYS":"—",
                        "Aggiunto":added,"Underlying":"err"})
                prog.progress((i+1)/len(wl), text=f"📡 {i+1}/{len(wl)} contratti aggiornati")
            prog.empty()
            if wl_results:
                st.dataframe(pd.DataFrame(wl_results), use_container_width=True, hide_index=True)
        else:
            wl_display = [{"Contratto":f"{e.get('ticker','')} {e.get('expiration','')} {e.get('strike','')}{e.get('type','')}",
                "Nota":e.get("note",""),"Aggiunto":e.get("added",""),
                "🐋 DAYS":get_cluster_repeat(e.get("ticker",""),e.get("strike",""),e.get("expiration",""),e.get("type",""))}
                for e in wl]
            st.dataframe(pd.DataFrame(wl_display), use_container_width=True, hide_index=True)

        st.markdown("---")
        wl_labels = [f"{e.get('ticker','')} {e.get('expiration','')} {e.get('strike','')}{e.get('type','')}" for e in wl]
        to_remove = st.selectbox("Rimuovi dalla watchlist:", ["— seleziona —"] + wl_labels, key="wl_remove")
        if st.button("🗑️ Rimuovi", key="wl_remove_btn") and to_remove != "— seleziona —":
            wl = [e for e in wl if f"{e.get('ticker','')} {e.get('expiration','')} {e.get('strike','')}{e.get('type','')}" != to_remove]
            save_watchlist(wl)
            st.success(f"Rimosso: {to_remove}")
            st.rerun()
    else:
        st.info("Nessun contratto in watchlist. Aggiungine uno sopra.")

# =========================
# SCAN BUTTON
# =========================
if st.button("🚀 Scansiona mercato", type="primary", use_container_width=True):

    tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]
    if not tickers:
        st.error("Inserisci almeno un ticker.")
        st.stop()

    final_df, telegram_text = pd.DataFrame(), ""

    for ticker in tickers:
        st.markdown(f"### 🔍 {ticker}")
        df = scan_ticker(ticker)
        if df.empty:
            st.info(f"Nessuna opportunità trovata per {ticker} con i filtri correnti.")
            continue

        top = df.head(10)
        final_df = pd.concat([final_df, top], ignore_index=True)

        if send_telegram:
            telegram_text += f"🔥 <b>TOP FLOW — {ticker}</b> [{mode}]\n\n"
            for _, row in top.head(3).iterrows():  # max 3 per ticker per Telegram
                ask_hit_val = row.get("ASK_HIT")
                sweep_val   = row.get("SWEEP", "")
                whale_days  = row.get("🐋 DAYS", 0)
                hit_emoji   = ""
                if ask_hit_val is not None:
                    hit_emoji = "🟢" if ask_hit_val>=70 else ("🔴" if ask_hit_val<=30 else "🟡")
                telegram_text += (
                    f"{row.get('SCORE','')}  {row['SIG']}  {row['BIAS']}\n"
                    f"<b>{row['OPZIONE']}</b>\n"
                    f"Mid: ${row['MID']}  VOI: {row['VOI']}  Vol: {row['volume']}\n"
                    f"Flow: <b>{row['FLOW $']}</b>"
                )
                if ask_hit_val is not None:
                    telegram_text += f"  Ask Hit: {hit_emoji}{ask_hit_val:.0f}%"
                if sweep_val:
                    telegram_text += f"  {sweep_val}"
                if whale_days >= 2:
                    telegram_text += f"  🐋{whale_days}d"
                telegram_text += "\n\n"

    if not final_df.empty:
        scan_records = []
        for _, r in final_df.iterrows():
            scan_records.append({
                "OPZIONE":  str(r.get("OPZIONE", "")),
                "ticker":   str(r.get("OPZIONE","")).split()[0],
                "strike":   float(r.get("strike", 0)),
                "exp_str":  str(r.get("exp_str", "")),
                "type":     str(r.get("type", "")),
                "flow":     str(r.get("FLOW $", "")),
                "voi":      str(r.get("VOI", "")),
            })
        st.session_state["scan_records"] = scan_records
        st.success(f"✅ Trovate {len(final_df)} opportunità totali")

        display_cols = [
            "SCORE", "SIG", "FLOW $", "CLUSTER", "BIAS", "SWEEP", "🐋 DAYS",
            "OPZIONE", "UNDER", "MID", "volume", "OI", "VOI", "VOI_ANOM", "DTE", "IV",
            "ASK_HIT", "EARN", "ITM", "ATM", "OTM",
        ]
        display_cols = [c for c in display_cols if c in final_df.columns]

        final_df = final_df.rename(columns={"bid": "~bid", "ask": "~ask", "SPREAD": "~SPREAD"})
        greeks_cols = ["OPZIONE", "delta", "gamma", "theta", "vega", "~bid", "~ask", "~SPREAD"]
        greeks_cols = [c for c in greeks_cols if c in final_df.columns]

        def hl_score(val):
            try:
                v = int(str(val).split()[-1])
                if v >= 75: return "background-color:#0a2a0a; color:#00ff44; font-weight:bold"
                if v >= 50: return "background-color:#1a2a00; color:#aaff00; font-weight:bold"
                if v >= 30: return "background-color:#2a2a00; color:#ffdd00"
                return "background-color:#1a1a1a; color:#888888"
            except: return ""
        def hl_earn(val):
            if val and "⚠️" in str(val): return "background-color:#3a1a00; color:#ffaa00"
            return ""
        def hl_voi_anom(val):
            s = str(val)
            if "🚀" in s: return "background-color:#0a1a3a; color:#00aaff; font-weight:bold"
            if "📈" in s: return "background-color:#0a2a1a; color:#00ff88"
            if "📉" in s: return "background-color:#2a0a0a; color:#ff6666"
            return ""
        def hl_sig(val):
            if "GO"   in str(val): return "background-color:#1a3a1a; color:#00ff88"
            if "HOLD" in str(val): return "background-color:#3a3a0a; color:#ffdd00"
            if "STOP" in str(val): return "background-color:#3a0a0a; color:#ff4444"
            return ""
        def hl_ask(val):
            try:
                v = float(val)
                if v >= 70: return "background-color:#1a3a1a; color:#00ff88"
                if v <= 30: return "background-color:#3a0a0a; color:#ff4444"
                return "background-color:#3a3a0a; color:#ffdd00"
            except: return ""
        def hl_sweep(val):
            return "background-color:#1a1a3a; color:#88aaff" if val=="🌊" else ""
        def hl_whale(val):
            try:
                v = int(val)
                if v >= 3: return "background-color:#1a1a3a; color:#88aaff"
                if v >= 2: return "background-color:#1a3a1a; color:#00ff88"
            except: pass
            return ""

        fmt = {
            "UNDER":   lambda x: f"${x:.2f}"        if pd.notna(x) else "—",
            "MID":     lambda x: f"${x:.2f}"        if pd.notna(x) else "—",
            "VOI":     lambda x: f"{float(x):.2f}"  if pd.notna(x) else "—",
            "ASK_HIT": lambda x: f"{float(x):.0f}%" if pd.notna(x) else "—",
            "IV":      lambda x: f"{x:.1f}%"        if pd.notna(x) else "—",
            "delta":   lambda x: f"{x:.3f}"         if pd.notna(x) else "—",
            "gamma":   lambda x: f"{x:.4f}"         if pd.notna(x) else "—",
            "theta":   lambda x: f"{x:.3f}"         if pd.notna(x) else "—",
            "vega":    lambda x: f"{x:.3f}"         if pd.notna(x) else "—",
        }

        styled = (
            final_df[display_cols].reset_index(drop=True).style
            .map(hl_score,    subset=["SCORE"]    if "SCORE"    in final_df.columns else [])
            .map(hl_sig,      subset=["SIG"])
            .map(hl_ask,      subset=["ASK_HIT"]  if "ASK_HIT"  in final_df.columns else [])
            .map(hl_sweep,    subset=["SWEEP"]     if "SWEEP"    in final_df.columns else [])
            .map(hl_whale,    subset=["🐋 DAYS"]   if "🐋 DAYS"  in final_df.columns else [])
            .map(hl_earn,     subset=["EARN"]      if "EARN"     in final_df.columns else [])
            .map(hl_voi_anom, subset=["VOI_ANOM"]  if "VOI_ANOM" in final_df.columns else [])
            .format(fmt, na_rep="—")
        )
# ==========================================
# PATCH 2: CALCOLO SCORE (CHIRURGICO)
# ==========================================
if not final_df.empty:
    # Creiamo la colonna SCORE usando la funzione della Patch 1
    final_df['SCORE'] = final_df.apply(calculate_whale_score, axis=1)
    
    # Portiamo lo SCORE come primissima colonna a sinistra
    cols = final_df.columns.tolist()
    if 'SCORE' in cols:
        cols.insert(0, cols.pop(cols.index('SCORE')))
        final_df = final_df[cols]
    
    # Aggiorniamo l'oggetto 'styled' per includere la nuova colonna
    styled = final_df.style.map(hl_whale, subset=["🕒 Giorni"])
# ==========================================
        st.dataframe(styled, use_container_width=True, hide_index=True)

        with st.expander("📐 Dettaglio Greeks & Prezzi stimati"):
            fmt_greeks = {
                "delta":   lambda x: f"{x:.3f}"   if pd.notna(x) else "—",
                "gamma":   lambda x: f"{x:.4f}"   if pd.notna(x) else "—",
                "theta":   lambda x: f"{x:.3f}"   if pd.notna(x) else "—",
                "vega":    lambda x: f"{x:.3f}"   if pd.notna(x) else "—",
                "~bid":    lambda x: f"~${x:.2f}" if pd.notna(x) else "—",
                "~ask":    lambda x: f"~${x:.2f}" if pd.notna(x) else "—",
                "~SPREAD": lambda x: f"~${x:.2f}" if pd.notna(x) else "—",
            }
            st.dataframe(
                final_df[greeks_cols].reset_index(drop=True).style.format(fmt_greeks, na_rep="—"),
                use_container_width=True, hide_index=True
            )
            st.caption("~bid / ~ask / ~SPREAD = stime da MID ±1.5% (quote live richiedono piano Advanced)")

        if send_telegram and telegram_text:
            ok = send_telegram_message(telegram_text)
            if ok:
                st.success("📲 Alert Telegram inviato!")
            else:
                st.error("❌ Errore invio Telegram")
    else:
        st.warning("⚠️ Nessuna opportunità trovata. Prova ad allargare i filtri.")

# =========================
# AGGIUNGI A WATCHLIST
# =========================
if st.session_state.get("scan_records"):
    with st.expander("⭐ Aggiungi alla Watchlist", expanded=True):
        opzioni = [r["OPZIONE"] for r in st.session_state["scan_records"]]
        sel = st.multiselect("Seleziona opzioni da aggiungere:", options=opzioni, key="wl_multisel")
        if st.button("➕ Aggiungi alla Watchlist", key="wl_add_btn", type="secondary"):
            added = 0
            for opzione in sel:
                rec = next((r for r in st.session_state["scan_records"] if r["OPZIONE"]==opzione), None)
                if rec:
                    type_wl = "C" if rec["type"] == "CALL" else "P"
                    note_wl = f"Flow {rec['flow']} | VOI {rec['voi']}"
                    ok = add_to_watchlist(rec["ticker"], rec["strike"], rec["exp_str"], type_wl, note_wl)
                    if ok: added += 1
            if added > 0:
                st.success(f"✅ {added} contratt{'o' if added==1 else 'i'} aggiunt{'o' if added==1 else 'i'}!")
                st.balloons()
            elif not sel:
                st.warning("⚠️ Seleziona almeno un'opzione.")
            else:
                st.info("ℹ️ Già in watchlist.")

# =========================
# FOOTER
# =========================
st.divider()
st.caption(
    "⚠️ Questo tool è uno screener di primo livello. "
    "L'analisi finale (grafico, contesto macro, greche) va completata su IBKR. "
    "Nessun ordine viene eseguito automaticamente. — v5.1"
)
