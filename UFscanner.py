import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import json
import requests
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# =================================================================
# CONFIGURAZIONE E VERSIONE
# =================================================================
VERSION = "5.5"

# Recupero Secrets da Streamlit Cloud
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]
GOOGLE_SHEET_ID  = st.secrets["GOOGLE_SHEET_ID"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# =================================================================
# GOOGLE SHEETS
# =================================================================
@st.cache_resource
def get_gsheet_client():
    try:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
        service_account_info = json.loads(raw) if isinstance(raw, str) else dict(raw)
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        return gspread.authorize(creds)
    except: return None

def add_to_watchlist(ticker, strike, expiry, op_type, note):
    client = get_gsheet_client()
    if not client: return False
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("watchlist")
        sheet.append_row([ticker, strike, expiry, op_type, datetime.now().strftime("%Y-%m-%d"), note])
        return True
    except: return False

# =================================================================
# ENGINE ASINCRONO PER ASK HIT % (CORRETTO v5.5)
# =================================================================
async def fetch_ask_hit_async(session, symbol):
    # Usiamo NBBO (National Best Bid and Offer) per l'ultimo prezzo reale
    url = f"https://api.polygon.io/v2/last/nbbo/{symbol}?apiKey={POLYGON_API_KEY}"
    try:
        await asyncio.sleep(0.05) # Protezione Rate Limit
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                res = data.get("results", {})
                if not res: return 50.0
                last_p = res.get('p', 0)
                ask_p = res.get('ap', 0)
                return 100.0 if (last_p >= ask_p and ask_p > 0) else 0.0
    except: return 50.0
    return 50.0

async def enrich_data_async(df):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ask_hit_async(session, row['SYMBOL']) for _, row in df.iterrows()]
        df['ASK_HIT_VAL'] = await asyncio.gather(*tasks)
    return df

# =================================================================
# LOGICA WHALE SCORE
# =================================================================
def calculate_whale_score(row):
    score = 0
    if row.get('VOI', 0) >= 1.0: score += 1
    if row.get('ASK_HIT_VAL', 50) >= 70: score += 1
    if row.get('FLOW_TYPE') == 'SWEEP': score += 1
    if row.get('PREMIO_VAL', 0) >= 100000: score += 1
    try:
        dist = abs(row['STRIKE'] - row['UNDERLYING_PRICE']) / row['UNDERLYING_PRICE']
        if 0.02 <= dist <= 0.15: score += 1
    except: pass
    return score

# =================================================================
# INTERFACCIA STREAMLIT
# =================================================================
st.set_page_config(page_title=f"Whale Scanner v{VERSION}", layout="wide")

with st.sidebar:
    st.title(f"⚙️ Filtri v{VERSION}")
    min_premio = st.number_input("Premio Minimo ($)", value=50000)
    min_dte = st.slider("DTE Minime", 0, 365, 60)
    strike_range = st.slider("Strike Range (%)", 0, 100, 20)

st.title(f"🐋 Whale Scanner Pro v{VERSION}")

if st.button("🚀 Scansiona Mercato", type="primary"):
    with st.spinner("Filtrando flussi istituzionali..."):
        url = f"https://api.polygon.io/v3/snapshot/options/us?limit=1000&apiKey={POLYGON_API_KEY}"
        r = requests.get(url).json()
        results = r.get("results", [])
        
        extracted = []
        for res in results:
            try:
                # Estrazione dati mirata (No 1200 colonne)
                det = res.get('details', {})
                last_q = res.get('last_quote', {})
                price = res.get('underlying_asset', {}).get('price', 0)
                strike = det.get('strike_price', 0)
                
                # Filtri
                if price > 0 and (abs(strike - price) / price) > (strike_range / 100): continue
                exp_s = det.get('expiration_date', "")
                dte = (datetime.strptime(exp_s, "%Y-%m-%d") - datetime.now()).days
                if dte < min_dte: continue
                
                vol = res.get('day', {}).get('volume', 0)
                premium = vol * last_q.get('p', 0) * 100
                if premium < min_premio: continue

                # Costruzione Simbolo
                fmt_exp = exp_s.replace("-", "")[2:]
                fmt_stk = str(int(strike * 1000)).zfill(8)
                sym = f"O:{det.get('underlying_ticker')}{fmt_exp}{det.get('contract_type')[0].upper()}{fmt_stk}"

                extracted.append({
                    "SYMBOL": sym, "TICKER": det.get('underlying_ticker'),
                    "TIPO": det.get('contract_type').upper(), "STRIKE": strike,
                    "SCADENZA": exp_s, "DTE": dte, "PREMIO_VAL": premium,
                    "VOI": round(vol / (res.get('open_interest', 1) or 1), 2),
                    "IV": round(res.get('implied_volatility', 0), 2),
                    "UNDERLYING_PRICE": price,
                    "FLOW_TYPE": "SWEEP" if vol > 300 else "BLOCK"
                })
            except: continue

        if extracted:
            df = pd.DataFrame(extracted)
            df = asyncio.run(enrich_data_async(df))
            df['SCORE_NUM'] = df.apply(calculate_whale_score, axis=1)
            df = df.sort_values(by="SCORE_NUM", ascending=False)

            # Formattazione
            df['SCORE'] = df['SCORE_NUM'].apply(lambda x: "⭐" * x)
            df['PREMIO_TOT'] = df['PREMIO_VAL'].apply(lambda x: f"${x:,.0f}")
            df['ASK_HIT'] = df['ASK_HIT_VAL'].apply(lambda x: f"{int(x)}%")
            
            cols = ["SCORE", "TICKER", "TIPO", "STRIKE", "SCADENZA", "DTE", "PREMIO_TOT", "VOI", "ASK_HIT", "IV"]
            st.dataframe(df[cols], use_container_width=True)
            st.session_state["scan_records"] = df.to_dict('records')
        else:
            st.info("Nessun segnale trovato.")

# Watchlist integrata
if st.session_state.get("scan_records"):
    with st.expander("⭐ Watchlist"):
        sel = st.multiselect("Seleziona:", [f"{r['TICKER']} {r['STRIKE']} {r['TIPO']}" for r in st.session_state["scan_records"]])
        if st.button("Salva"):
            st.success("Dati inviati a Google Sheets!")
