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
    except Exception as e:
        st.warning(f"⚠️ Google Sheets non disponibile: {e}")
        return None

def add_to_watchlist(ticker, strike, expiry, op_type, note):
    client = get_gsheet_client()
    if client is None: return False
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("watchlist")
        sheet.append_row([ticker, strike, expiry, op_type, datetime.now().strftime("%Y-%m-%d"), note])
        return True
    except: return False

# =========================
# ENGINE ASINCRONO ASK HIT
# =========================
async def fetch_ask_hit_async(session, symbol):
    """Recupera l'ultimo prezzo NBBO per determinare l'aggressività"""
    url = f"https://api.polygon.io/v2/last/nbbo/{symbol}?apiKey={POLYGON_API_KEY}"
    try:
        await asyncio.sleep(0.02) # Anti-block
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                res = data.get("results", {})
                if not res: return 50.0
                last_p = res.get('p', 0)
                ask_p  = res.get('ap', 0)
                # Se l'ultimo prezzo è >= all'ask, è aggressivo (100%)
                return 100.0 if (last_p >= ask_p and ask_p > 0) else 0.0
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
    if row.get('VOI', 0) >= 1.0: score += 1
    if row.get('ASK_HIT_VAL', 50) >= 70: score += 1
    if row.get('FLOW_TYPE') == 'SWEEP': score += 1
    if row.get('PREMIO_VAL', 0) >= 100000: score += 1
    try:
        dist = abs(row['STRIKE'] - row['UNDERLYING_PRICE']) / row['UNDERLYING_PRICE']
        if 0.02 <= dist <= 0.15: score += 1
    except: pass
    return score

# =========================
# INTERFACCIA STREAMLIT
# =========================
st.set_page_config(page_title=f"UF Scanner Pro v{VERSION}", layout="wide")

# SIDEBAR CON TUTTI I TUOI FILTRI ORIGINALI
with st.sidebar:
    st.title(f"⚙️ Parametri v{VERSION}")
    
    st.subheader("Filtri Base")
    min_premio = st.number_input("Premio Minimo ($)", value=50000, step=5000)
    min_dte = st.slider("DTE Minime", 0, 365, 60)
    strike_range = st.slider("Strike Range OTM (%)", 0, 100, 25)
    
    st.subheader("Filtri Avanzati")
    min_voi = st.slider("VOI Minimo (Vol/OI)", 0.0, 5.0, 0.0, 0.1)
    solo_sweep = st.checkbox("Solo SWEEP", value=False)
    
    st.divider()
    st.info("Scanner tarato per Swing Trading. Cerca le stelle ⭐.")

st.title(f"🐋 Whale Scanner Professional v{VERSION}")

# PULSANTE DI SCANSIONE
if st.button("🚀 Avvia Scansione Completa", type="primary"):
    with st.spinner("Analisi dei flussi in corso..."):
        url = f"https://api.polygon.io/v3/snapshot/options/us?limit=1000&apiKey={POLYGON_API_KEY}"
        try:
            r = requests.get(url).json()
            results = r.get("results", [])
            
            extracted = []
            for res in results:
                try:
                    # 1. Estrazione mirata dei dati
                    det = res.get('details', {})
                    day = res.get('day', {})
                    last_q = res.get('last_quote', {})
                    price = res.get('underlying_asset', {}).get('price', 0)
                    strike = det.get('strike_price', 0)
                    
                    # 2. Applicazione Filtri (OTM)
                    if price > 0:
                        diff = abs(strike - price) / price
                        if diff > (strike_range / 100): continue

                    # 3. Filtro DTE
                    exp_s = det.get('expiration_date', "")
                    expiry_dt = datetime.strptime(exp_s, "%Y-%m-%d")
                    dte = (expiry_dt - datetime.now()).days
                    if dte < min_dte: continue

                    # 4. Filtro Premio e Volume
                    vol = day.get('volume', 0)
                    oi = res.get('open_interest', 1) or 1
                    voi = round(vol / oi, 2)
                    if voi < min_voi: continue

                    last_p = last_q.get('p', 0)
                    premium = vol * last_p * 100
                    if premium < min_premio: continue

                    # 5. Filtro Sweep (Solo se selezionato)
                    is_sweep = vol > 350 # Logica di rilevamento sweep semplificata
                    if solo_sweep and not is_sweep: continue

                    # Costruzione Simbolo e Record
                    fmt_exp = exp_s.replace("-", "")[2:]
                    fmt_stk = str(int(strike * 1000)).zfill(8)
                    sym = f"O:{det.get('underlying_ticker')}{fmt_exp}{det.get('contract_type')[0].upper()}{fmt_stk}"

                    extracted.append({
                        "SYMBOL": sym,
                        "TICKER": det.get('underlying_ticker'),
                        "TIPO": det.get('contract_type').upper(),
                        "STRIKE": strike,
                        "SCADENZA": exp_s,
                        "DTE": dte,
                        "PREMIO_VAL": premium,
                        "VOI": voi,
                        "IV": round(res.get('implied_volatility', 0), 2),
                        "UNDERLYING_PRICE": price,
                        "FLOW_TYPE": "SWEEP" if is_sweep else "BLOCK"
                    })
                except: continue

            if extracted:
                df = pd.DataFrame(extracted)
                
                # Arricchimento Ask Hit (Async)
                df = asyncio.run(enrich_data_async(df))
                
                # Calcolo Score
                df['SCORE_NUM'] = df.apply(calculate_whale_score, axis=1)
                df = df.sort_values(by="SCORE_NUM", ascending=False)

                # Visualizzazione Formattata
                df_show = df.copy()
                df_show['SCORE'] = df_show['SCORE_NUM'].apply(lambda x: "⭐" * x)
                df_show['PREMIO_TOT'] = df_show['PREMIO_VAL'].apply(lambda x: f"${x:,.0f}")
                df_show['ASK_HIT'] = df_show['ASK_HIT_VAL'].apply(lambda x: f"{int(x)}%")
                
                # Selezione colonne definitive
                cols_to_display = ["SCORE", "TICKER", "TIPO", "STRIKE", "SCADENZA", "DTE", "PREMIO_TOT", "VOI", "ASK_HIT", "IV"]
                st.dataframe(df_show[cols_to_display], use_container_width=True)
                
                # Salvataggio sessione per Watchlist
                st.session_state["scan_records"] = df.to_dict('records')
            else:
                st.info("Nessuna balena trovata con questi filtri.")
        except Exception as e:
            st.error(f"Errore: {e}")

# =========================
# WATCHLIST GOOGLE SHEETS
# =========================
if st.session_state.get("scan_records"):
    st.divider()
    with st.expander("⭐ Aggiungi Segnali alla Watchlist", expanded=True):
        # Usiamo il ticker e lo strike per identificare l'opzione
        opzioni = [f"{r['TICKER']} {r['STRIKE']} {r['TIPO']} ({r['SCADENZA']})" for r in st.session_state["scan_records"]]
        sel = st.multiselect("Seleziona:", options=opzioni)
        
        if st.button("➕ Salva in Watchlist"):
            added = 0
            for label in sel:
                rec = next((r for r in st.session_state["scan_records"] if f"{r['TICKER']} {r['STRIKE']} {r['TIPO']} ({r['SCADENZA']})" == label), None)
                if rec:
                    nota = f"Score: {rec['SCORE_NUM']} | VOI: {rec['VOI']}"
                    ok = add_to_watchlist(rec['TICKER'], rec['STRIKE'], rec['SCADENZA'], rec['TIPO'][0], nota)
                    if ok: added += 1
            if added > 0:
                st.success(f"✅ {added} contratti aggiunti!")
                st.balloons()

st.divider()
st.caption(f"UF Scanner Pro v{VERSION} | Swing Strategy | 15m Delayed Data")
