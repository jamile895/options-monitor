import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

# =========================
# SECRETS
# =========================
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]

# =========================
# CONFIG UI
# =========================
st.set_page_config(layout="wide", page_title="Options Flow Scanner PRO")
st.title("🔥 Options Flow Scanner PRO 🔥 by Ugo Fortezze")
st.caption("Powered by Polygon.io — Greeks | Ask Hit | Sweep | Storico Cluster")

# =========================
# STORICO SCANSIONI
# =========================
HISTORY_FILE = "scan_history.json"

def load_history() -> list:
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_history(history: list):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f)
    except Exception:
        pass

def add_to_history(ticker, strike, expiration, contract_type, flow_power, voi, ask_hit, sweep):
    history = load_history()
    entry = {
        "date":       datetime.today().strftime("%Y-%m-%d"),
        "ticker":     ticker,
        "strike":     strike,
        "expiration": expiration,
        "type":       contract_type,
        "flow":       round(float(flow_power), 0),
        "voi":        round(float(voi), 2),
        "ask_hit":    round(float(ask_hit), 1) if ask_hit is not None else None,
        "sweep":      sweep,
    }
    key = (entry["date"], ticker, str(strike), expiration, contract_type)
    existing_keys = {
        (e["date"], e["ticker"], str(e["strike"]), e["expiration"], e["type"])
        for e in history
    }
    if key not in existing_keys:
        history.append(entry)
        history = history[-500:]
        save_history(history)

def get_cluster_repeat(ticker, strike, expiration, contract_type) -> int:
    history = load_history()
    days = set()
    for e in history:
        if (e["ticker"] == ticker and
            str(e["strike"]) == str(strike) and
            e["expiration"] == expiration and
            e["type"] == contract_type):
            days.add(e["date"])
    return len(days)

# =========================
# MODALITÀ
# =========================
mode = st.radio(
    "Modalità Trading",
    ["SMALL CAP", "MID CAP", "BIG CAP", "SNIPER", "HOT ONLY", "SPY SWING"],
    horizontal=True,
    key="mode_radio"
)

# =========================
# PRESET
# =========================
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
        "volume_min": 200, "voi_min": 1.2, "dte_max": 210, "dte_min": 60,
        "strike_dist_min": 5, "strike_dist_max": 10, "spread_max": 20.0,
        "delta_min": 0.10, "delta_max": 0.70, "ask_hit_min": 55.0, "flow_min": 500000,
        "desc": "SPY SWING — DTE 60-210gg | OTM 5-10% | Flow >$500K | Smart Money only"
    },
}

preset = PRESETS[mode]
st.caption(f"ℹ️ {preset['desc']}")

APP_VERSION = "4.1"
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

# =========================
# SLIDERS
# =========================
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

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=5)
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

        volume = day.get("volume") or 0
        oi     = item.get("open_interest") or 0
        bid    = quotes.get("bid") or 0
        ask    = quotes.get("ask") or 0

        if   bid > 0 and ask > 0: mid = (bid + ask) / 2
        elif ask > 0:              mid = ask
        elif bid > 0:              mid = bid
        else:                      mid = day.get("close") or day.get("vwap") or 0

        iv    = item.get("implied_volatility") or 0
        delta = greeks.get("delta")
        gamma = greeks.get("gamma")
        theta = greeks.get("theta")
        vega  = greeks.get("vega")
        spread = (ask - bid) if (ask > 0 and bid > 0) else None

        rows.append({
            "ticker_sym": ticker_sym, "type": contract_type,
            "strike": strike, "expiration": exp_dt, "exp_str": exp_str,
            "volume": int(volume), "OI": int(oi),
            "MID": round(mid, 2), "bid": round(bid, 2), "ask": round(ask, 2),
            "SPREAD": round(spread, 2) if spread is not None else None,
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
    df["ASK_HIT"] = None
    df["SWEEP"]   = ""
    df["🐋 DAYS"] = 0

    idx_list = df.head(top_n).index.tolist()
    progress = st.progress(0, text="📡 Analisi flusso in corso...")

    for i, idx in enumerate(idx_list):
        row          = df.loc[idx]
        ask_hit_pct, is_sweep = get_ask_hit_real(
            row.get("ticker_sym", ""), row.get("bid", 0), row.get("ask", 0)
        )
        sweep_str = "🌊" if is_sweep else ""
        df.at[idx, "ASK_HIT"] = ask_hit_pct
        df.at[idx, "SWEEP"]   = sweep_str

        add_to_history(ticker, row.get("strike"), row.get("exp_str",""),
                       row.get("type",""), row.get("FLOW_POWER_NUM",0),
                       row.get("VOI",0), ask_hit_pct, sweep_str)
        df.at[idx, "🐋 DAYS"] = get_cluster_repeat(
            ticker, row.get("strike"), row.get("exp_str",""), row.get("type","")
        )
        progress.progress((i+1)/len(idx_list), text=f"📡 Flow analysis: {i+1}/{len(idx_list)}")

    progress.empty()

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

    return enrich_with_flow_data(df, ticker, top_n=15)

# =========================
# LEGENDA / MANUALE IN-APP
# =========================
with st.expander("📖 Manuale — Options Flow Scanner PRO v4.1"):
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

| Colonna | Formula | Come leggerla |
|---|---|---|
| **SIG** | VOI ≥5→GO, ≥2→HOLD, <2→STOP | 🟢 GO = flusso anomalo forte · 🟡 HOLD = interessante · 🔴 STOP = rumore |
| **FLOW $** | Volume × MID price | Notional totale. >$500K = istituzionale. >$1M = whale |
| **CLUSTER** | Volume totale per strike | Concentrazione su uno strike specifico |
| **BIAS** | CALL=📈L / PUT=📉S | Direzione del flusso |
| **SWEEP** | Trade su ≥2 exchange | 🌊 = ordine istituzionale eseguito su tutti i market maker simultaneamente |
| **ASK_HIT %** | % trades at/near ask | ≥70% 🟢 buyer aggressivo · ≤30% 🔴 vendita · 30–70% 🟡 neutro |
| **🐋 DAYS** | Giorni distinti nello storico | ≥2 = accumulo · ≥3 = balena che costruisce posizione |
| **VOI** | Volume / Open Interest | >1 = soldi freschi (nuova apertura) · <1 = chiusura posizione |
| **DTE** | Giorni alla scadenza | <7 = speculativo · 60–210 = swing istituzionale |
| **IV** | Implied Volatility % | Alta = opzione cara · Bassa = economica vs storia |
| **delta** | Sensitività a $1 di movimento | 0.10–0.30 = OTM (leva alta) · 0.40–0.60 = ATM · >0.70 = ITM |
| **gamma** | Variazione delta per $1 | Alto gamma + DTE breve = posizione esplosiva vicino scadenza |
| **theta** | Decadimento temporale giornaliero | Negativo per chi compra. Con DTE 60+ è gestibile |
| **vega** | Sensitività alla volatilità | Alto vega = beneficia da spike di volatilità (es. eventi macro) |
| **ITM** | Strike favorevole vs prezzo | ✅ = In The Money |
| **ATM** | Strike ≤1% dal prezzo | 🎯 = At The Money |
| **OTM** | Strike sfavorevole vs prezzo | ⬆️ = Out of The Money |

---

## 🔍 Filtri Principali

| Filtro | Cosa fa |
|---|---|
| **Volume minimo** | Soglia minima di contratti scambiati oggi |
| **VOI minimo** | >1.0 = nuova apertura (soldi freschi) |
| **DTE min/max** | Finestra temporale delle scadenze |
| **Distanza strike % min/max** | Seleziona la zona di moneyness. Es: 5–10% = OTM moderato |
| **Flow $ minimo** | >$500K = smart money. >$1M = whale |
| **Ask Hit % minimo** | Solo contratti con buyer aggressivi |
| **Spread bid/ask max** | Esclude contratti illiquidi |
| **Delta min/max** | Seleziona il range di leva desiderato |

---

## 🐋 Come rilevare l'Accumulo Balena

La colonna **🐋 DAYS** conta quante volte lo stesso contratto (ticker + strike + scadenza) è comparso nelle scansioni precedenti:

- **1 giorno** → segnale singolo, potrebbe essere rumore
- **2 giorni** 🟢 → attenzione, possibile accumulo in corso
- **≥3 giorni** 🔵 → balena che sta costruendo una posizione grossa

**Routine consigliata:** Scansiona SPY SWING ogni giorno nelle prime 2 ore di mercato (15:30–17:30 ora italiana). Se vedi lo stesso strike/scadenza per 3+ giorni con Ask Hit ≥55% e SWEEP 🌊 → segnale ad alta convinzione.

---

## 📐 Configurazione SPY SWING consigliata

| Parametro | Valore |
|---|---|
| Modalità | SPY SWING |
| Ticker | SPY |
| Tipo opzione | BOTH (vedi accumulo, operi solo CALL) |
| DTE | 60 – 210 giorni |
| VOI min | 1.2 |
| Flow $ min | $500.000 |
| Distanza strike | 5% – 10% (OTM moderato) |
| Ask Hit % min | 55% |
| Delta | 0.10 – 0.70 |

---

## ⚠️ Note Operative

- Questo tool è uno **screener di primo livello**. L'analisi finale va completata su IBKR (grafico, contesto macro, greche).
- Il flusso di opzioni anticipa spesso il movimento del sottostante di 1–5 giorni, ma non è infallibile.
- La combinazione più forte: **VOI alto + ASK_HIT ≥55% + SWEEP 🌊 + 🐋 DAYS ≥2**
- Paper trading consigliato per le prime settimane: 1–2 operazioni al giorno con note su entry, razionale e risultato.
- Nessun ordine viene eseguito automaticamente. Il controllo finale è sempre tuo.
""")

# =========================
# STORICO — VISUALIZZAZIONE
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
                save_history([])
                st.success("Storico cancellato.")
                st.rerun()

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
            for _, row in top.iterrows():
                greeks_line = ""
                if row.get("delta") is not None:
                    greeks_line = f"Δ {row['delta']}  Γ {row['gamma']}  Θ {row['theta']}  V {row['vega']}\n"
                ask_hit_val = row.get("ASK_HIT")
                sweep_val   = row.get("SWEEP", "")
                whale_days  = row.get("🐋 DAYS", 0)
                hit_line, whale_line = "", ""
                if ask_hit_val is not None:
                    hit_emoji = "🟢" if ask_hit_val>=70 else ("🔴" if ask_hit_val<=30 else "🟡")
                    hit_line  = f"Ask Hit: {hit_emoji} <b>{ask_hit_val:.0f}%</b>  {sweep_val}\n"
                if whale_days >= 2:
                    whale_line = f"🐋 Accumulo: <b>{whale_days} giorni</b>\n"
                telegram_text += (
                    f"{row['SIG']}  {row['BIAS']}\n"
                    f"<b>{row['OPZIONE']}</b>\n"
                    f"Underlying: ${row['UNDER']}  |  Mid: ${row['MID']}\n"
                    f"Flow: <b>{row['FLOW $']}</b>  |  VOI: {row['VOI']}\n"
                    f"{hit_line}{whale_line}{greeks_line}\n"
                )

    if not final_df.empty:
        st.success(f"✅ Trovate {len(final_df)} opportunità totali")

        display_cols = [
            "SIG","FLOW $","CLUSTER","BIAS","SWEEP","🐋 DAYS","OPZIONE",
            "UNDER","MID","bid","ask","SPREAD","volume","OI","VOI","DTE","IV",
            "ASK_HIT","delta","gamma","theta","vega","ITM","ATM","OTM",
        ]
        display_cols = [c for c in display_cols if c in final_df.columns]

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
            "bid":     lambda x: f"${x:.2f}"        if pd.notna(x) else "—",
            "ask":     lambda x: f"${x:.2f}"        if pd.notna(x) else "—",
            "SPREAD":  lambda x: f"${x:.2f}"        if pd.notna(x) else "—",
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
            .map(hl_sig,   subset=["SIG"])
            .map(hl_ask,   subset=["ASK_HIT"]  if "ASK_HIT"  in final_df.columns else [])
            .map(hl_sweep, subset=["SWEEP"]     if "SWEEP"    in final_df.columns else [])
            .map(hl_whale, subset=["🐋 DAYS"]   if "🐋 DAYS"  in final_df.columns else [])
            .format(fmt, na_rep="—")
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

        if send_telegram and telegram_text:
            ok = send_telegram_message(telegram_text)
            st.success("📲 Alert Telegram inviato!") if ok else st.error("❌ Errore invio Telegram")
    else:
        st.warning("⚠️ Nessuna opportunità trovata. Prova ad allargare i filtri.")

# =========================
# FOOTER
# =========================
st.divider()
st.caption(
    "⚠️ Questo tool è uno screener di primo livello. "
    "L'analisi finale (grafico, contesto macro, greche) va completata su IBKR. "
    "Nessun ordine viene eseguito automaticamente. — v4.1"
)
