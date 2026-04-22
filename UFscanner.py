import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import json
import requests
import os
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIGURAZIONE E VERSIONE
# =========================
VERSION = "5.5 FINAL"

# Secrets
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]
GOOGLE_SHEET_ID  = st.secrets["GOOGLE_SHEET_ID"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# =========================
# GOOGLE SHEETS
# =========================
@st.cache_resource
def get_gsheet_client():
    try:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
        service_account_info = json.loads(raw) if isinstance(raw, str) else dict(raw)
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        return gspread.authorize(creds)
    except:
        return None

def add_to_watchlist(ticker, strike, expiry, op_type, note):
    client = get_gsheet_client()
    if not client: return False
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("watchlist")
        sheet.append_row([ticker, strike, expiry, op_type, datetime.now().strftime("%Y-%m-%d"), note])
        return True
    except: return False

# =========================
# ASYNC ENGINE - ASK HIT %
# =========================
async def fetch_ask_hit_async(session, symbol):
    url = f"https://api.polygon.io/v2/last/nbbo/{symbol}?apiKey={POLYGON_API_KEY}"
    try:
        await asyncio.sleep(0.02) 
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                res = data.get("results", {})
                if not res: return 50.0
                return 100.0 if (res.get('p', 0) >= res.get('ap', 0) and res.get('ap', 0) > 0) else 0.0
    except: return 50.0
    return 50.0

async def enrich_data_async(df):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ask_hit_async(session, row['SYMBOL']) for _, row in df.iterrows()]
        df['ASK_HIT_VAL'] = await asyncio.gather(*tasks)
    return df

# =========================
# LOGICA WHALE SCORE
# =========================
def calculate_whale_score(row):
    score = 0
    if row.get('voi', 0) >= 1.0: score += 1
    if row.get('ASK_HIT_VAL', 50) >= 70: score += 1
    if row.get('flow') == 'SWEEP': score += 1
    if row.get('premium', 0) >= 100000: score += 1
    try:
        dist = abs(row['strike'] - row['underlying_price']) / row['underlying_price']
        if 0.02 <= dist <= 0.15: score += 1
    except: pass
    return score

# =========================
# INTERFACCIA STREAMLIT
# =========================
st.set_page_config(page_title=f"UF Scanner Pro v{VERSION}", layout="wide")

# Sidebar con i tuoi filtri originali
with st.sidebar:
    st.title(f"⚙️ Parametri v{VERSION}")
    
    # Filtri Manuali (Quelli che gestisci tu)
    min_premio = st.number_input("Premio Minimo ($)", value=50000, step=5000)
    min_dte = st.slider("DTE Minime", 0, 365, 60)
    strike_range = st.slider("Strike Range OTM (%)", 0, 100, 25)
    
    st.divider()
    st.subheader("Filtri Avanzati")
    min_voi = st.slider("VOI Minimo (Vol/OI)", 0.0, 5.0, 0.0)
    solo_sweep = st.checkbox("Solo SWEEP", value=False)

st.title(f"🐋 Whale Scanner Professional v{VERSION}")

if st.button("🚀 Avvia Scansione Completa", type="primary"):
    with st.spinner("Pescando nel mercato..."):
        # Chiamata Snapshot
        url = f"https://api.polygon.io/v3/snapshot/options/us?limit=1000&apiKey={POLYGON_API_KEY}"
        try:
            r = requests.get(url).json()
            results = r.get("results", [])
            
            extracted = []
            for res in results:
                try:
                    det = res.get('details', {})
                    day = res.get('day', {})
                    last_q = res.get('last_quote', {})
                    price = res.get('underlying_asset', {}).get('price', 0)
                    strike = det.get('strike_price', 0)
                    
                    # FILTRO OTM
                    if price > 0:
                        diff = abs(strike - price) / price
                        if diff > (strike_range / 100): continue

                    # FILTRO DTE
                    exp_s = det.get('expiration_date', "")
                    expiry_dt = datetime.strptime(exp_s, "%Y-%m-%d")
                    dte = (expiry_dt - datetime.now()).days
                    if dte < min_dte: continue

                    # FILTRO PREMIO E VOI
                    vol = day.get('volume', 0)
                    oi = res.get('open_interest', 1) or 1
                    voi = round(vol / oi, 2)
                    if voi < min_voi: continue

                    premium = vol * last_q.get('p', 0) * 100
                    if premium < min_premio: continue

                    # FILTRO SWEEP
                    is_sweep = vol > 350
                    if solo_sweep and not is_sweep: continue

                    # Costruzione Simbolo
                    fmt_exp = exp_s.replace("-", "")[2:]
                    fmt_stk = str(int(strike * 1000)).zfill(8)
                    sym = f"O:{det.get('underlying_ticker')}{fmt_exp}{det.get('contract_type')[0].upper()}{fmt_stk}"

                    extracted.append({
                        "SYMBOL": sym,
                        "ticker": det.get('underlying_ticker'),
                        "type": det.get('contract_type').upper(),
                        "strike": strike,
                        "exp_str": exp_s,
                        "dte": dte,
                        "premium": premium,
                        "voi": voi,
                        "iv": round(res.get('implied_volatility', 0), 2),
                        "underlying_price": price,
                        "flow": "SWEEP" if is_sweep else "BLOCK"
                    })
                except: continue

            if extracted:
                df = pd.DataFrame(extracted)
                
                # Arricchimento Ask Hit
                df = asyncio.run(enrich_data_async(df))
                
                # Calcolo Score
                df['score_num'] = df.apply(calculate_whale_score, axis=1)
                df = df.sort_values(by="score_num", ascending=False)

                # Visualizzazione
                df_show = df.copy()
                df_show['SCORE'] = df_show['score_num'].apply(lambda x: "⭐" * x)
                df_show['PREMIO'] = df_show['premium'].apply(lambda x: f"${x:,.0f}")
                df_show['ASK_HIT'] = df_show['ASK_HIT_VAL'].apply(lambda x: f"{int(x)}%")
                
                # SELEZIONE COLONNE (Ordine pulito)
                cols = ["SCORE", "ticker", "type", "strike", "exp_str", "dte", "PREMIO", "voi", "ASK_HIT", "iv"]
                st.dataframe(df_show[cols], use_container_width=True)
                
                st.session_state["scan_records"] = df.to_dict('records')
            else:
                st.info("Nessuna balena trovata. Prova a regolare i filtri.")
        except Exception as e:
            st.error(f"Errore tecnico: {e}")

# =========================
# AGGIUNGI A WATCHLIST
# =========================
if st.session_state.get("scan_records"):
    st.divider()
    with st.expander("⭐ Aggiungi alla Watchlist Google Sheets", expanded=True):
        opzioni = [f"{r['ticker']} {r['strike']} {r['type']} ({r['exp_str']})" for r in st.session_state["scan_records"]]
        sel = st.multiselect("Seleziona contratti:", options=opzioni)
        
        if st.button("➕ Salva nel Database"):
            added = 0
            for label in sel:
                rec = next((r for r in st.session_state["scan_records"] if f"{r['ticker']} {r['strike']} {r['type']} ({r['exp_str']})" == label), None)
                if rec:
                    type_wl = rec['type'][0]
                    note_wl = f"Score {rec['score_num']} | VOI {rec['voi']}"
                    ok = add_to_watchlist(rec['ticker'], rec['strike'], rec['exp_str'], type_wl, note_wl)
                    if ok: added += 1
            if added > 0:
                st.success(f"✅ {added} contratti aggiunti!")
                st.balloons()

st.divider()
st.caption(f"UF Scanner Pro v{VERSION} | Data: 15m Delayed | Provider: Polygon/Mission")
