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

# =========================
# LOGICA WHALE SCORE & STYLES
# =========================
def calculate_whale_score(row):
    score = 0    
    try:    
        if float(row.get('VOI', 0)) >= 1.0: score += 1
        if float(row.get('ask_hit_val', 0)) >= 70: score += 1
        if row.get('flow') == 'SWEEP' or row.get('sweep_found') == True: score += 1
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

# =========================
# HELPERS & POLYGON API
# =========================
def format_k(x):
    if x >= 1_000_000: return f"{x/1_000_000:.1f}M"
    elif x >= 1_000:   return f"{x/1_000:.1f}K"
    return str(int(x))

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
    except Exception: pass
    return None

def get_options_chain(ticker: str, dte_min_d: int, dte_max_d: int) -> list[dict]:
    today   = datetime.today().date()
    exp_min = (today + timedelta(days=dte_min_d)).isoformat()
    exp_max = (today + timedelta(days=dte_max_d)).isoformat()
    all_rows, url = [], f"https://api.polygon.io/v3/snapshot/options/{ticker}"
    params = {"apiKey": POLYGON_API_KEY, "expiration_date.gte": exp_min, "expiration_date.lte": exp_max, "limit": 250}
    page_count = 0
    while url and page_count < 15:
        try:
            r = requests.get(url, params=params, timeout=12)
            if r.status_code != 200: break
            data = r.json()
            all_rows.extend(data.get("results", []))
            next_url = data.get("next_url")
            url = next_url + f"&apiKey={POLYGON_API_KEY}" if next_url else None
            params = {}
            page_count += 1
        except: break
    return all_rows

@st.cache_data(ttl=120)
def get_ask_hit_real(contract_ticker: str, bid: float, ask: float) -> tuple[float | None, bool]:
    try:
        r = requests.get(f"https://api.polygon.io/v2/trades/{contract_ticker}",
                         params={"apiKey": POLYGON_API_KEY, "limit": 50}, timeout=5)
        if r.status_code != 200: return None, False
        trades = r.json().get("results", [])
        if not trades: return None, False
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else None
        ask_hits, total, exchanges = 0, 0, set()
        for t in trades:
            price = t.get("price", 0)
            if t.get("exchange"): exchanges.add(t["exchange"])
            total += 1
            if mid and price >= ask * 0.995: ask_hits += 1
        return round((ask_hits / total) * 100, 1), len(exchanges) >= 2
    except: return None, False

# =========================
# CONFIG UI
# =========================
st.set_page_config(layout="wide", page_title="Options Flow Scanner PRO")
st.title("🔥 Options Flow Scanner PRO 🔥")

mode = st.radio("Modalità Trading", ["SMALL CAP", "MID CAP", "BIG CAP", "SNIPER", "HOT ONLY", "SPY SWING"], horizontal=True)

PRESETS = {
    "SMALL CAP": {"vol": 100, "voi": 1.5, "dte": [2, 60], "flow": 0, "dist": [0, 20]},
    "MID CAP":   {"vol": 200, "voi": 1.2, "dte": [2, 90], "flow": 0, "dist": [0, 15]},
    "BIG CAP":   {"vol": 500, "voi": 1.0, "dte": [1, 120], "flow": 0, "dist": [0, 12]},
    "SNIPER":    {"vol": 1000, "voi": 2.0, "dte": [1, 14], "flow": 0, "dist": [0, 5]},
    "HOT ONLY":  {"vol": 2000, "voi": 3.0, "dte": [1, 7], "flow": 0, "dist": [0, 3]},
    "SPY SWING": {"vol": 100, "voi": 0.5, "dte": [60, 210], "flow": 50000, "dist": [0, 15]},
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
    flow_min   = st.slider("Flow $ min", 0, 1000000, p["flow"])
    option_type = st.radio("Tipo", ["CALL", "PUT", "BOTH"], horizontal=True)

tickers_input = st.text_input("Tickers (es: SPY, TSLA)", "SPY")
send_telegram = st.checkbox("📲 Telegram Alerts")

# =========================
# PARSE & FILTER
# =========================
def parse_and_filter(raw: list[dict], underlying: float, ticker: str) -> pd.DataFrame:
    rows = []
    today = pd.Timestamp.today().normalize()
    for item in raw:
        details = item.get("details", {})
        day = item.get("day", {})
        greeks = item.get("greeks", {})
        
        strike = details.get("strike_price")
        exp_str = details.get("expiration_date", "")
        if not strike or not exp_str: continue
        
        vol = day.get("volume", 0)
        oi = item.get("open_interest", 1)
        mid = day.get("close", 0) or day.get("vwap", 0)
        
        exp_dt = pd.to_datetime(exp_str)
        dte = (exp_dt - today).days
        dist = round(abs(strike - underlying) / underlying * 100, 1)
        flow_num = vol * mid * 100
        
        if vol >= volume_min and (vol/oi) >= voi_min and dte_min <= dte <= dte_max and dist_min <= dist <= dist_max and flow_num >= flow_min:
            rows.append({
                "ticker": ticker, "type": details.get("contract_type"),
                "strike": strike, "expiration": exp_dt, "exp_str": exp_str,
                "volume": int(vol), "OI": int(oi), "VOI": round(vol/oi, 2),
                "MID": round(mid, 2), "FLOW_POWER_NUM": flow_num, "UNDER": underlying,
                "DTE": dte, "DIST_STRIKE": dist,
                "delta": greeks.get("delta"), "gamma": greeks.get("gamma"),
                "theta": greeks.get("theta"), "vega": greeks.get("vega"),
                "OPZIONE": f"{ticker} {exp_str} {strike} {details.get('contract_type')[0]}"
            })
    return pd.DataFrame(rows)

# =========================
# ESECUZIONE SCANSIONE
# =========================
if st.button("🚀 AVVIA SCANSIONE"):
    all_results = []
    tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]
    
    with st.spinner("Scansione in corso..."):
        for t in tickers:
            price = get_stock_price(t)
            if price:
                raw_data = get_options_chain(t, dte_min, dte_max)
                df = parse_and_filter(raw_data, price, t)
                if not df.empty:
                    # Calcolo Ask Hit Reale per ogni riga
                    ah_list, sweep_list = [], []
                    for _, row in df.iterrows():
                        ah, sw = get_ask_hit_real(f"O:{row['ticker']}{row['exp_str'].replace('-','')}{row['type'][0]}{int(row['strike']*1000):08d}", row['MID']*0.98, row['MID']*1.02)
                        ah_list.append(ah or 0)
                        sweep_list.append(sw)
                    df["ask_hit_val"] = ah_list
                    df["sweep_found"] = sweep_list
                    all_results.append(df)
    
    if all_results:
        final_df = pd.concat(all_results).reset_index(drop=True)
        if option_type != "BOTH":
            final_df = final_df[final_df["type"] == option_type]
            
        # CALCOLO SCORE E STELLE
        final_df["SCORE"] = final_df.apply(calculate_whale_score, axis=1)
        
        # Riordino per visualizzazione
        cols_order = ["SCORE", "OPZIONE", "DTE", "volume", "VOI", "ask_hit_val", "FLOW_POWER_NUM", "DIST_STRIKE"]
        final_df["FLOW $"] = final_df["FLOW_POWER_NUM"].apply(format_k)
        
        st.subheader("📊 Risultati")
        st.dataframe(final_df[cols_order].style.map(hl_whale, subset=["DTE"]), use_container_width=True, hide_index=True)
        
        st.session_state["scan_records"] = final_df.to_dict('records')
        
        with st.expander("🕒 Dettaglio Greeks"):
            st.dataframe(final_df[["OPZIONE", "delta", "gamma", "theta", "vega"]], use_container_width=True, hide_index=True)

        if send_telegram:
            msg = f"🟢 Scanner {mode}\n"
            for _, r in final_df.head(3).iterrows():
                msg += f"{r['SCORE']} {r['OPZIONE']} | VOI: {r['VOI']}\n"
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

# =========================
# WATCHLIST
# =========================
if st.session_state.get("scan_records"):
    with st.expander("⭐ Aggiungi alla Watchlist", expanded=True):
        opts = [r["OPZIONE"] for r in st.session_state["scan_records"]]
        sel = st.multiselect("Seleziona opzioni:", opts)
        if st.button("➕ Salva Selezionati"):
            st.success(f"Aggiunti {len(sel)} contratti alla watchlist (Simulazione GSheet ok)")
