import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ==========================================
# 1. SECRETS & CONFIGURAZIONE
# ==========================================
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]
GOOGLE_SHEET_ID  = st.secrets["GOOGLE_SHEET_ID"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def get_gsheet_client():
    try:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
        info = json.loads(raw) if isinstance(raw, str) else dict(raw)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)
    except: return None

def get_sheet(sheet_name: str):
    client = get_gsheet_client()
    if not client: return None
    try:
        return client.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
    except: return None

# ==========================================
# 2. LOGICA WHALE SCORE (STELLE)
# ==========================================
def calculate_whale_score(row):
    score = 0    
    try:    
        # 1. Volume vs Open Interest (Aggressività)
        if float(row.get('VOI', 0)) >= 1.5: score += 1
        # 2. Ask Hit (Pressione in acquisto)
        if float(row.get('ask_hit_val', 0)) >= 70: score += 1
        # 3. Sweep (Esecuzione veloce su più exchange)
        if row.get('sweep_found') is True: score += 1
        # 4. Flow Power (Dimensione monetaria)
        if float(row.get('FLOW_POWER_NUM', 0)) >= 150000: score += 1
        # 5. Prossimità al prezzo (ATM/Near OTM)
        price = float(row.get('UNDER', 0))
        strike = float(row.get('strike', 0))
        if price > 0:
            dist = abs(strike - price) / price
            if dist <= 0.10: score += 1
    except: pass
    return "⭐" * score if score > 0 else "💤"

def hl_dte(val):
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
# 3. FUNZIONI DATA FETCHING (POLYGON)
# ==========================================
def get_stock_price(ticker):
    try:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY}, timeout=8).json()
        data = r.get("ticker", {})
        return data.get("day", {}).get("c") or data.get("lastTrade", {}).get("p")
    except: return None

def get_options_chain(ticker, dte_min, dte_max):
    today = datetime.today().date()
    exp_min = (today + timedelta(days=dte_min)).isoformat()
    exp_max = (today + timedelta(days=dte_max)).isoformat()
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker}"
    params = {"apiKey": POLYGON_API_KEY, "expiration_date.gte": exp_min, "expiration_date.lte": exp_max, "limit": 250}
    results = []
    try:
        for _ in range(5):
            r = requests.get(url, params=params, timeout=10).json()
            results.extend(r.get("results", []))
            if not r.get("next_url"): break
            url = r.get("next_url") + f"&apiKey={POLYGON_API_KEY}"
            params = {}
    except: pass
    return results

@st.cache_data(ttl=60)
def get_trade_details(contract_ticker, mid):
    try:
        url = f"https://api.polygon.io/v2/trades/{contract_ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY, "limit": 50}).json()
        trades = r.get("results", [])
        if not trades: return 0, False
        hits, exchanges = 0, set()
        for t in trades:
            if t.get("exchange"): exchanges.add(t["exchange"])
            if t.get("price", 0) >= mid: hits += 1
        return round((hits/len(trades))*100, 1), len(exchanges) > 1
    except: return 0, False

# ==========================================
# 4. UI - SLIDERS E PRESET
# ==========================================
st.set_page_config(layout="wide", page_title="Scanner Whale Flow")
st.title("🐋 Options Whale Flow Scanner")

mode = st.radio("Modalità", ["SMALL CAP", "MID CAP", "BIG CAP", "SNIPER", "HOT ONLY", "SPY SWING"], horizontal=True)

PRESETS = {
    "SMALL CAP": {"vol": 100, "voi": 1.5, "dte": [2, 60], "dist": [0, 20], "flow": 10000},
    "MID CAP":   {"vol": 200, "voi": 1.2, "dte": [2, 90], "dist": [0, 15], "flow": 50000},
    "BIG CAP":   {"vol": 500, "voi": 1.0, "dte": [1, 120], "dist": [0, 12], "flow": 100000},
    "SNIPER":    {"vol": 1000, "voi": 2.0, "dte": [1, 14], "dist": [0, 5], "flow": 200000},
    "HOT ONLY":  {"vol": 2000, "voi": 3.0, "dte": [1, 7], "dist": [0, 3], "flow": 500000},
    "SPY SWING": {"vol": 100, "voi": 0.5, "dte": [60, 210], "dist": [0, 15], "flow": 50000},
}

p = PRESETS[mode]
col1, col2, col3 = st.columns(3)
with col1:
    vol_min = st.slider("Volume Minimo", 0, 10000, p["vol"])
    voi_min = st.slider("VOI Minimo (Vol/OI)", 0.0, 10.0, p["voi"])
with col2:
    dte_min = st.slider("DTE Min", 0, 30, p["dte"][0])
    dte_max = st.slider("DTE Max", 1, 365, p["dte"][1])
with col3:
    flow_min = st.number_input("Flow Power $ Min", 0, 1000000, p["flow"])
    dist_max = st.slider("Distanza Strike Max %", 1, 50, p["dist"][1])

tickers_in = st.text_input("Tickers (separati da virgola)", "SPY, TSLA, NVDA")
opt_type = st.radio("Tipo Opzione", ["CALL", "PUT", "BOTH"], horizontal=True)

# ==========================================
# 5. ESECUZIONE SCANSIONE
# ==========================================
if st.button("🚀 AVVIA SCANSIONE WHALE"):
    results = []
    tickers = [t.strip().upper() for t in tickers_in.split(",") if t.strip()]
    
    with st.spinner("Scansione dei flussi in corso..."):
        for t in tickers:
            price = get_stock_price(t)
            if not price: continue
            
            chain = get_options_chain(t, dte_min, dte_max)
            today = pd.Timestamp.today().normalize()
            
            for item in chain:
                d = item.get("details", {})
                day = item.get("day", {})
                
                v = day.get("volume", 0)
                oi = item.get("open_interest", 1)
                strike = d.get("strike_price", 0)
                mid = day.get("close", 0) or day.get("vwap", 0)
                
                dist = abs(strike - price) / price * 100
                flow_val = v * mid * 100
                
                if v >= vol_min and (v/oi) >= voi_min and flow_val >= flow_min and dist <= dist_max:
                    exp = d.get("expiration_date", "")
                    dte = (pd.to_datetime(exp) - today).days
                    
                    # Dettagli Aggressività
                    c_ticker = f"O:{t}{exp.replace('-','')}{d.get('contract_type')[0]}{int(strike*1000):08d}"
                    ah, sweep = get_trade_details(c_ticker, mid)
                    
                    results.append({
                        "ticker": t, "type": d.get("contract_type"), "strike": strike,
                        "DTE": dte, "volume": v, "VOI": round(v/oi, 2), "FLOW_POWER_NUM": flow_val,
                        "ask_hit_val": ah, "sweep_found": sweep, "UNDER": price,
                        "OPZIONE": f"{t} {exp} {strike} {d.get('contract_type')[0]}",
                        "MID": mid, "expiration": exp
                    })

    if results:
        df = pd.DataFrame(results)
        if opt_type != "BOTH": df = df[df["type"] == opt_type]
        
        # CALCOLO WHALE SCORE (STELLE)
        df["SCORE"] = df.apply(calculate_whale_score, axis=1)
        
        # Riordino Colonne (SCORE PER PRIMA)
        df["FLOW $"] = df["FLOW_POWER_NUM"].apply(format_k)
        df["SWEEP"] = df["sweep_found"].map({True: "🌊", False: ""})
        
        cols = ["SCORE", "OPZIONE", "DTE", "volume", "VOI", "ask_hit_val", "SWEEP", "FLOW $"]
        
        st.subheader("📊 Risultati Whale Flow")
        st.dataframe(df[cols].style.map(hl_dte, subset=["DTE"]), use_container_width=True, hide_index=True)
        
        st.session_state["last_scan"] = df.to_dict('records')
    else:
        st.info("Nessun flusso rilevato con i parametri attuali.")

# ==========================================
# 6. WATCHLIST
# ==========================================
if st.session_state.get("last_scan"):
    with st.expander("⭐ Salva in Watchlist"):
        opts = [r["OPZIONE"] for r in st.session_state["last_scan"]]
        sel = st.multiselect("Seleziona da aggiungere:", opts)
        if st.button("Salva nel Foglio Google"):
            sheet = get_sheet("watchlist")
            if sheet:
                for s in sel:
                    data = next(item for item in st.session_state["last_scan"] if item["OPZIONE"] == s)
                    sheet.append_row([datetime.now().strftime("%Y-%m-%d"), data['ticker'], data['strike'], data['expiration'], data['type']])
                st.success("✅ Salvataggio completato!")
