import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import json
import requests
import os
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# =================================================================
# CONFIGURAZIONE E VERSIONE
# =================================================================
VERSION = "5.4"

# Secrets (Caricati da Streamlit Cloud)
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]
GOOGLE_SHEET_ID  = st.secrets["GOOGLE_SHEET_ID"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# =================================================================
# GOOGLE SHEETS — CONNESSIONE & FUNZIONI
# =================================================================
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
    if not client: return False
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("watchlist")
        # Evita duplicati base
        existing = sheet.get_all_values()
        for row in existing:
            if row[0] == ticker and row[1] == str(strike) and row[2] == expiry:
                return False
        sheet.append_row([ticker, strike, expiry, op_type, datetime.now().strftime("%Y-%m-%d"), note])
        return True
    except: return False

# =================================================================
# LOGICA WHALE SCORE (MANUALE DEL PADRE DI FAMIGLIA)
# =================================================================
def calculate_whale_score(row):
    """
    Assegna un punteggio da 1 a 5 stelle basato sui 5 pilastri Swing.
    """
    score = 0
    # 1. VOI (Volume > Open Interest)
    if row.get('VOI', 0) >= 1.0: score += 1
    # 2. Aggressività (Ask Hit >= 70%)
    ask_hit_val = row.get('ASK_HIT_VAL', 50)
    if ask_hit_val >= 70: score += 1
    # 3. Urgenza (Sweep Detection)
    if row.get('FLOW_TYPE') == 'SWEEP': score += 1
    # 4. Size (Premio > 100k USD)
    if row.get('PREMIO_VAL', 0) >= 100000: score += 1
    # 5. Distanza Strike (OTM 2% - 15%)
    try:
        dist = abs(row['STRIKE'] - row['UNDERLYING_PRICE']) / row['UNDERLYING_PRICE']
        if 0.02 <= dist <= 0.15: score += 1
    except: pass
    return score

# =================================================================
# ENGINE ASINCRONO PER ASK HIT %
# =================================================================
async def fetch_ask_hit_async(session, symbol):
    url = f"https://api.polygon.io/v3/quotes/{symbol}?limit=15&apiKey={POLYGON_API_KEY}"
    try:
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                res = data.get("results", [])
                if not res: return 50.0
                hits = sum(1 for q in res if q.get("p", 0) >= q.get("ap", 0.01))
                return round((hits / len(res)) * 100, 1)
    except: return 50.0
    return 50.0

async def enrich_data_async(df):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ask_hit_async(session, row['SYMBOL']) for _, row in df.iterrows()]
        df['ASK_HIT_VAL'] = await asyncio.gather(*tasks)
    return df

# =================================================================
# INTERFACCIA STREAMLIT
# =================================================================
st.set_page_config(page_title=f"Whale Scanner Pro v{VERSION}", layout="wide")

# Sidebar - Parametri di Filtro
with st.sidebar:
    st.title(f"⚙️ Parametri v{VERSION}")
    min_premio = st.number_input("Premio Minimo ($)", value=50000, step=5000)
    min_dte = st.slider("DTE Minime (Swing)", 0, 365, 60)
    strike_range = st.slider("Strike Range OTM (%)", 0, 100, 20)
    st.divider()
    st.info("I dati sono ritardati di 15 min. Lo Score valuta la qualità istituzionale.")

st.title(f"🐋 Whale Scanner Professional v{VERSION}")
st.markdown("---")

if st.button("🚀 Avvia Scansione Istituzionale", type="primary"):
    with st.spinner("Analisi dei flussi in corso..."):
        # Chiamata Snapshot a Polygon
        url = f"https://api.polygon.io/v3/snapshot/options/us?limit=1000&apiKey={POLYGON_API_KEY}"
        try:
            response = requests.get(url)
            r = response.json()
            results = r.get("results", [])
            
            extracted = []
            for res in results:
                try:
                    # Estrazione mirata per evitare le 1279 colonne
                    details = res.get('details', {})
                    day = res.get('day', {})
                    last_quote = res.get('last_quote', {})
                    price = res.get('underlying_asset', {}).get('price', 0)
                    strike = details.get('strike_price', 0)
                    
                    # Filtro Distanza Strike
                    if price > 0:
                        diff = abs(strike - price) / price
                        if diff > (strike_range / 100): continue

                    # Filtro DTE
                    exp_str = details.get('expiration_date', "")
                    expiry_dt = datetime.strptime(exp_str, "%Y-%m-%d")
                    dte = (expiry_dt - datetime.now()).days
                    if dte < min_dte: continue

                    # Calcolo Premio e VOI
                    vol = day.get('volume', 0)
                    oi = res.get('open_interest', 1) or 1
                    premium = vol * last_quote.get('p', 0) * 100
                    if premium < min_premio: continue

                    # Costruzione Simbolo per Async Ask Hit
                    fmt_exp = exp_str.replace("-", "")[2:]
                    fmt_stk = str(int(strike * 1000)).zfill(8)
                    symbol = f"O:{details.get('underlying_ticker')}{fmt_exp}{details.get('contract_type')[0].upper()}{fmt_stk}"
                    
                    # Caricamento dati puliti
                    extracted.append({
                        "SYMBOL": symbol,
                        "TICKER": details.get('underlying_ticker'),
                        "TIPO": details.get('contract_type').upper(),
                        "STRIKE": strike,
                        "SCADENZA": exp_str,
                        "DTE": dte,
                        "PREMIO_VAL": premium,
                        "VOI": round(vol / oi, 2),
                        "IV": round(res.get('implied_volatility', 0), 2),
                        "UNDERLYING_PRICE": price,
                        "FLOW_TYPE": "SWEEP" if vol > 300 else "BLOCK"
                    })
                except: continue

            if extracted:
                df = pd.DataFrame(extracted)
                
                # Arricchimento Asincrono (Ask Hit %)
                df = asyncio.run(enrich_data_async(df))
                
                # Calcolo Score
                df['SCORE_NUM'] = df.apply(calculate_whale_score, axis=1)
                df = df.sort_values(by="SCORE_NUM", ascending=False)

                # Formattazione per la tabella finale
                df_display = df.copy()
                df_display['SCORE'] = df_display['SCORE_NUM'].apply(lambda x: "⭐" * x)
                df_display['PREMIO_TOT'] = df_display['PREMIO_VAL'].apply(lambda x: f"${x:,.0f}")
                df_display['ASK_HIT'] = df_display['ASK_HIT_VAL'].apply(lambda x: f"{x}%")
                
                # SELEZIONE COLONNE (Protezione contro le mille colonne)
                col_order = ["SCORE", "TICKER", "TIPO", "STRIKE", "SCADENZA", "DTE", "PREMIO_TOT", "VOI", "ASK_HIT", "IV"]
                st.dataframe(df_display[col_order], use_container_width=True)
                
                # Salvataggio in session_state per Watchlist
                st.session_state["scan_records"] = df.to_dict('records')
            else:
                st.info("Nessuna balena trovata con i filtri attuali.")
        except Exception as e:
            st.error(f"Errore durante la scansione: {e}")

# =================================================================
# AGGIUNGI A WATCHLIST
# =================================================================
if st.session_state.get("scan_records"):
    st.divider()
    with st.expander("⭐ Aggiungi Segnali alla Watchlist Google Sheets", expanded=True):
        opzioni_lista = [f"{r['TICKER']} {r['STRIKE']} {r['TIPO']} ({r['SCADENZA']})" for r in st.session_state["scan_records"]]
        sel = st.multiselect("Seleziona opzioni:", options=opzioni_lista)
        
        if st.button("➕ Salva in Watchlist"):
            added = 0
            for label in sel:
                # Trova il record corrispondente
                rec = next((r for r in st.session_state["scan_records"] if f"{r['TICKER']} {r['STRIKE']} {r['TIPO']} ({r['SCADENZA']})" == label), None)
                if rec:
                    nota = f"Score {rec['SCORE_NUM']} | VOI {rec['VOI']} | AskHit {rec['ASK_HIT_VAL']}%"
                    ok = add_to_watchlist(rec['TICKER'], rec['STRIKE'], rec['SCADENZA'], rec['TIPO'][0], nota)
                    if ok: added += 1
            
            if added > 0:
                st.success(f"✅ {
