import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# =========================
# SECRETS & CONFIG
# =========================
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]
GOOGLE_SHEET_ID  = st.secrets["GOOGLE_SHEET_ID"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# =========================
# FUNZIONI CORE (SCORE & UTILS)
# =========================
def calculate_whale_score(row):
    score = 0    
    try:    
        if float(row.get('VOI', 0)) >= 1.0: score += 1
        # Se non abbiamo l'ask_hit_val reale, usiamo un placeholder o 0
        if float(row.get('ask_hit_val', 0)) >= 70: score += 1
        if row.get('flow') == 'SWEEP': score += 1
        if float(row.get('FLOW_POWER_NUM', 0)) >= 100000: score += 1
        price = float(row.get('UNDER', 0))
        strike = float(row.get('strike', 0))
        if price > 0:
            dist = abs(strike - price) / price
            if 0.02 <= dist <= 0.15: score += 1
    except:
        pass
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

# =========================
# GOOGLE SHEETS CONNESSIONE
# =========================
@st.cache_resource
def get_gsheet_client():
    try:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
        service_account_info = json.loads(raw) if isinstance(raw, str) else dict(raw)
        creds  = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        return None

def get_sheet(sheet_name: str):
    client = get_gsheet_client()
    if not client: return None
    try:
        return client.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
    except: return None

# =========================
# UI & PRESETS
# =========================
st.set_page_config(layout="wide", page_title="Options Flow Scanner PRO")
st.title("🔥 Options Flow Scanner PRO 🔥")

mode = st.radio("Modalità Trading", ["SMALL CAP", "MID CAP", "BIG CAP", "SNIPER", "HOT ONLY", "SPY SWING"], horizontal=True)

PRESETS = {
    "SMALL CAP": {"vol": 100, "voi": 1.5, "dte": [2, 60], "flow": 0},
    "MID CAP":   {"vol": 200, "voi": 1.2, "dte": [2, 90], "flow": 0},
    "BIG CAP":   {"vol": 500, "voi": 1.0, "dte": [1, 120], "flow": 0},
    "SNIPER":    {"vol": 1000, "voi": 2.0, "dte": [1, 14], "flow": 0},
    "HOT ONLY":  {"vol": 2000, "voi": 3.0, "dte": [1, 7], "flow": 0},
    "SPY SWING": {"vol": 100, "voi": 0.5, "dte": [60, 210], "flow": 50000},
}

p = PRESETS[mode]
col_s1, col_s2 = st.columns(2)
with col_s1:
    volume_min = st.slider("Volume min", 0, 10000, p["vol"])
    voi_min    = st.slider("VOI min", 0.0, 10.0, p["voi"])
with col_s2:
    dte_range  = st.slider("DTE Range", 0, 365, p["dte"])
    flow_min   = st.slider("Flow $ min", 0, 1000000, p["flow"])

tickers_input = st.text_input("Ticker (es: SPY, TSLA, AAPL)", "SPY")
send_telegram = st.checkbox("📲 Invia a Telegram")

# =========================
# LOGICA DATI (POLYGON)
# =========================
def get_stock_price(ticker):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?apiKey={POLYGON_API_KEY}"
    r = requests.get(url).json()
    return r.get("results", [{}])[0].get("c")

def get_options_chain(ticker, dte_m, dte_M):
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker}?apiKey={POLYGON_API_KEY}&limit=250"
    r = requests.get(url).json()
    return r.get("results", [])

def parse_data(raw, underlying, ticker):
    rows = []
    today = pd.Timestamp.today().normalize()
    for item in raw:
        d = item.get("details", {})
        g = item.get("greeks", {})
        vol = item.get("day", {}).get("volume", 0)
        oi = item.get("open_interest", 1)
        mid = item.get("day", {}).get("close", 0)
        
        exp_dt = pd.to_datetime(d.get("expiration_date"))
        dte = (exp_dt - today).days
        
        if vol >= volume_min and dte >= dte_range[0] and dte <= dte_range[1]:
            rows.append({
                "ticker": ticker,
                "strike": d.get("strike_price"),
                "type": d.get("contract_type"),
                "expiration": d.get("expiration_date"),
                "DTE": dte,
                "volume": vol,
                "OI": oi,
                "VOI": round(vol/oi, 2),
                "MID": mid,
                "FLOW_POWER_NUM": vol * mid * 100,
                "UNDER": underlying,
                "delta": g.get("delta"),
                "OPZIONE": f"{ticker} {d.get('expiration_date')} {d.get('strike_price')} {d.get('contract_type')[0]}"
            })
    return pd.DataFrame(rows)

# =========================
# ESECUZIONE
# =========================
if st.button("🚀 AVVIA SCANSIONE"):
    all_dfs = []
    tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]
    
    for t in tickers:
        u_price = get_stock_price(t)
        if u_price:
            raw = get_options_chain(t, dte_range[0], dte_range[1])
            df = parse_data(raw, u_price, t)
            if not df.empty: all_dfs.append(df)
            
    if all_dfs:
        final_df = pd.concat(all_dfs)
        final_df["SCORE"] = final_df.apply(calculate_whale_score, axis=1)
        
        # Riordino Colonne
        cols = ["SCORE", "OPZIONE", "DTE", "volume", "VOI", "FLOW_POWER_NUM"]
        display_df = final_df[cols].copy()
        display_df["FLOW_POWER_NUM"] = display_df["FLOW_POWER_NUM"].apply(format_k)
        
        st.dataframe(display_df.style.map(hl_whale, subset=["DTE"]), use_container_width=True, hide_index=True)
        st.session_state["scan_records"] = final_df.to_dict('records')
        
        if send_telegram:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", 
                          data={"chat_id": TELEGRAM_CHAT_ID, "text": f"Scanner {mode} completato!"})
    else:
        st.warning("Nessun risultato trovato.")

# =========================
# WATCHLIST
# =========================
if st.session_state.get("scan_records"):
    with st.expander("⭐ Watchlist Quick Add"):
        opt_list = [r["OPZIONE"] for r in st.session_state["scan_records"]]
        sel = st.multiselect("Seleziona:", opt_list)
        if st.button("Salva in Watchlist"):
            st.success(f"Aggiunti {len(sel)} contratti!")
