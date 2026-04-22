import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import asyncio
import aiohttp

# ==========================================
# 1. SECRETS & CONFIGURAZIONE GOOGLE SHEETS
# ==========================================
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]
GOOGLE_SHEET_ID  = st.secrets["GOOGLE_SHEET_ID"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_gsheet_client():
    try:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
        service_account_info = json.loads(raw) if isinstance(raw, str) else dict(raw)
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        st.warning(f"⚠️ Google Sheets non disponibile: {e}")
        return None

def get_sheet(sheet_name: str):
    client = get_gsheet_client()
    if client is None: return None
    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        st.warning(f"⚠️ Foglio '{sheet_name}' non trovato: {e}")
        return None

# ==========================================
# 2. LOGICA WHALE SCORE & STYLES
# ==========================================
def calculate_whale_score(row):
    score = 0    
    try:    
        if float(row.get('VOI', 0)) >= 1.0: score += 1
        if float(row.get('ask_hit', 0)) >= 70: score += 1
        if row.get('SWEEP') == "🌊": score += 1
        if float(row.get('FLOW_POWER_NUM', 0)) >= 100000: score += 1
        price = float(row.get('UNDER', 0))
        strike = float(row.get('strike', 0))
        if price > 0:
            dist = abs(strike - price) / price
            if 0.02 <= dist <= 0.15: score += 1
    except: pass
    return "⭐" * score if score > 0 else "💤"

def hl_whale(val):
    try:
        v = float(val)
        if v <= 7: return "background-color: #002b36; color: #268bd2; font-weight: bold;"
    except: pass
    return ""

def format_k(x):
    if x >= 1_000_000: return f"{x/1_000_000:.1f}M"
    elif x >= 1_000:   return f"{x/1_000:.1f}K"
    return str(int(x))

# ==========================================
# 3. FUNZIONI POLYGON & ANALISI
# ==========================================
@st.cache_data(ttl=3600)
def get_next_earnings(ticker: str) -> str | None:
    try:
        url = f"https://api.polygon.io/vX/reference/tickers/{ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY}, timeout=8).json()
        return r.get("results", {}).get("next_earnings_date")
    except: return None

def earnings_in_dte(ticker: str, dte_max: int) -> tuple[bool, str]:
    ed = get_next_earnings(ticker)
    if not ed: return False, ""
    try:
        days = (datetime.strptime(ed, "%Y-%m-%d").date() - datetime.today().date()).days
        if 0 <= days <= dte_max: return True, ed
    except: pass
    return False, ""

def get_stock_price(ticker: str) -> float | None:
    try:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY}, timeout=8).json()
        data = r.get("ticker", {})
        price = data.get("day", {}).get("c") or data.get("lastTrade", {}).get("p")
        return round(float(price), 2) if price else None
    except: return None

def get_options_chain(ticker: str, dte_min: int, dte_max: int) -> list[dict]:
    today = datetime.today().date()
    exp_min, exp_max = (today + timedelta(days=dte_min)).isoformat(), (today + timedelta(days=dte_max)).isoformat()
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker}"
    params = {"apiKey": POLYGON_API_KEY, "expiration_date.gte": exp_min, "expiration_date.lte": exp_max, "limit": 250}
    all_results = []
    for _ in range(10):
        r = requests.get(url, params=params, timeout=12).json()
        all_results.extend(r.get("results", []))
        next_url = r.get("next_url")
        if not next_url: break
        url = next_url + f"&apiKey={POLYGON_API_KEY}"
        params = {}
    return all_results

@st.cache_data(ttl=120)
def get_ask_hit_real(contract_ticker: str, mid: float) -> tuple[float | None, bool]:
    try:
        url = f"https://api.polygon.io/v2/trades/{contract_ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY, "limit": 50}).json()
        trades = r.get("results", [])
        if not trades: return None, False
        hits, exchanges = 0, set()
        for t in trades:
            if t.get("exchange"): exchanges.add(t["exchange"])
            if t.get("price", 0) >= mid * 1.005: hits += 1
        return round((hits / len(trades)) * 100, 1), len(exchanges) >= 2
    except: return None, False

# ==========================================
# 4. INTERFACCIA UTENTE (UI)
# ==========================================
st.set_page_config(layout="wide", page_title="Options Flow Scanner PRO")
st.title("🔥 Options Flow Scanner PRO 🔥")

mode = st.radio("Modalità Trading", ["SMALL CAP", "MID CAP", "BIG CAP", "SNIPER", "HOT ONLY", "SPY SWING"], horizontal=True)

PRESETS = {
    "SMALL CAP": {"vol": 100, "voi": 1.5, "dte": [2, 60], "dist": [0, 20]},
    "MID CAP":   {"vol": 200, "voi": 1.2, "dte": [2, 90], "dist": [0, 15]},
    "BIG CAP":   {"vol": 500, "voi": 1.0, "dte": [1, 120], "dist": [0, 12]},
    "SNIPER":    {"vol": 1000, "voi": 2.0, "dte": [1, 14], "dist": [0, 5]},
    "HOT ONLY":  {"vol": 2000, "voi": 3.0, "dte": [1, 7], "dist": [0, 3]},
    "SPY SWING": {"vol": 100, "voi": 0.5, "dte": [60, 210], "dist": [0, 15]},
}

p = PRESETS[mode]
col_s1, col_s2 = st.columns(2)
with col_s1:
    volume_min = st.slider("Volume min", 0, 10000, p["vol"])
    voi_min    = st.slider("VOI min", 0.0, 10.0, p["voi"])
    dte_min    = st.slider("DTE min", 0, 180, p["dte"][0])
    dte_max    = st.slider("DTE max", 1, 365, p["dte"][1])
with col_s2:
    dist_min   = st.slider("Dist Strike % min", 0, 30, p["dist"][0])
    dist_max   = st.slider("Dist Strike % max", 1, 50, p["dist"][1])
    option_type = st.radio("Tipo", ["CALL", "PUT", "BOTH"], horizontal=True)
    send_telegram = st.checkbox("📲 Telegram Alerts")

tickers_input = st.text_input("Tickers (es: SPY, TSLA)", "SPY")

# ==========================================
# 5. CORE ENGINE (SCANSIONE)
# ==========================================
if st.button("🚀 AVVIA SCANSIONE"):
    all_rows = []
    tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]
    
    with st.spinner("Analisi di mercato in corso..."):
        for t in tickers:
            underlying = get_stock_price(t)
            if not underlying: continue
            
            raw_data = get_options_chain(t, dte_min, dte_max)
            today = pd.Timestamp.today().normalize()
            
            for item in raw_data:
                det = item.get("details", {})
                day = item.get("day", {})
                grk = item.get("greeks", {})
                
                vol = day.get("volume", 0)
                oi = item.get("open_interest", 1)
                strike = det.get("strike_price", 0)
                exp_str = det.get("expiration_date", "")
                mid = day.get("close", 0) or day.get("vwap", 0)
                
                dte = (pd.to_datetime(exp_str) - today).days
                dist = round(abs(strike - underlying) / underlying * 100, 1)
                
                if vol >= volume_min and (vol/oi) >= voi_min and dist_min <= dist <= dist_max:
                    # Ask Hit & Sweep
                    c_ticker = f"O:{t}{exp_str.replace('-','')}{det.get('contract_type')[0]}{int(strike*1000):08d}"
                    ah, sweep = get_ask_hit_real(c_ticker, mid)
                    has_earn, earn_date = earnings_in_dte(t, dte)
                    
                    all_rows.append({
                        "ticker": t, "type": det.get("contract_type"), "strike": strike,
                        "expiration": exp_str, "DTE": dte, "volume": vol, "OI": oi,
                        "VOI": round(vol/oi, 2), "MID": mid, "UNDER": underlying,
                        "FLOW_POWER_NUM": vol * mid * 100, "ask_hit": ah, 
                        "SWEEP": "🌊" if sweep else "", "EARN": f"⚠️ {earn_date}" if has_earn else "",
                        "delta": grk.get("delta"), "gamma": grk.get("gamma"), 
                        "theta": grk.get("theta"), "vega": grk.get("vega"),
                        "OPZIONE": f"{t} {exp_str} {strike} {det.get('contract_type')[0][0]}"
                    })

    if all_rows:
        df = pd.DataFrame(all_rows)
        if option_type != "BOTH": df = df[df["type"] == option_type]
        
        # Iniezione WHALE SCORE
        df["SCORE"] = df.apply(calculate_whale_score, axis=1)
        
        # Formattazione Colonne per visualizzazione
        df["FLOW $"] = df["FLOW_POWER_NUM"].apply(format_k)
        df["CLUSTER"] = df.groupby("strike")["volume"].transform("sum").apply(format_k)
        
        cols_final = ["SCORE", "OPZIONE", "DTE", "volume", "VOI", "ask_hit", "SWEEP", "FLOW $", "CLUSTER", "EARN"]
        
        st.subheader("📊 Flussi Opzioni Rilevati")
        st.dataframe(df[cols_final].style.map(hl_whale, subset=["DTE"]), use_container_width=True, hide_index=True)
        
        # Salvataggio sessione
        st.session_state["scan_records"] = df.to_dict('records')
        
        # Expander Dettagli
        with st.expander("🕒 Greeks & Sentiment"):
            st.dataframe(df[["OPZIONE", "delta", "gamma", "theta", "vega"]], use_container_width=True, hide_index=True)

        if send_telegram:
            msg = f"🚀 *Whale Alert ({mode})*\n\n"
            for _, r in df.head(5).iterrows():
                msg += f"{r['SCORE']} {r['OPZIONE']} | VOI {r['VOI']} | AskHit {r['ask_hit']}%\n"
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"})

    else:
        st.info("Nessun segnale rilevato con i filtri correnti.")

# ==========================================
# 6. WATCHLIST (GOOGLE SHEETS)
# ==========================================
if st.session_state.get("scan_records"):
    with st.expander("⭐ Aggiungi alla Watchlist", expanded=True):
        opts = [r["OPZIONE"] for r in st.session_state["scan_records"]]
        selected = st.multiselect("Seleziona contratti:", opts)
        
        if st.button("➕ Salva Selezionati nel Foglio"):
            sheet = get_sheet("watchlist")
            if sheet:
                for opt in selected:
                    rec = next(r for r in st.session_state["scan_records"] if r["OPZIONE"] == opt)
                    sheet.append_row([datetime.now().strftime("%Y-%m-%d"), rec['ticker'], rec['strike'], rec['expiration'], rec['type'], "Aggiunto da Scanner"])
                st.success(f"✅ {len(selected)} contratti salvati su Google Sheets!")
            else:
                st.error("Impossibile connettersi al Foglio 'watchlist'.")
