import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from collections import defaultdict

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
st.caption("Powered by Polygon.io — Greeks | Ask Hit | Sweep Detection")

# =========================
# MODALITÀ
# =========================
mode = st.radio(
    "Modalità Trading",
    ["SMALL CAP", "MID CAP", "BIG CAP", "SNIPER", "HOT ONLY"],
    horizontal=True,
    key="mode_radio"
)

# =========================
# PRESET
# =========================
PRESETS = {
    "SMALL CAP": {
        "volume_min": 100,
        "voi_min": 1.5,
        "dte_max": 60,
        "dte_min": 2,
        "strike_distance": 20,
        "spread_max": 20.0,
        "delta_min": 0.05,
        "delta_max": 0.95,
        "ask_hit_min": 0.0,
        "desc": "Small Cap (<$2B) — bassa liquidità, filtri adattati"
    },
    "MID CAP": {
        "volume_min": 200,
        "voi_min": 1.2,
        "dte_max": 90,
        "dte_min": 2,
        "strike_distance": 15,
        "spread_max": 20.0,
        "delta_min": 0.10,
        "delta_max": 0.90,
        "ask_hit_min": 0.0,
        "desc": "Mid Cap ($2B-$10B) — bilanciato tra liquidità e segnale"
    },
    "BIG CAP": {
        "volume_min": 500,
        "voi_min": 1.0,
        "dte_max": 120,
        "dte_min": 1,
        "strike_distance": 12,
        "spread_max": 20.0,
        "delta_min": 0.10,
        "delta_max": 0.90,
        "ask_hit_min": 0.0,
        "desc": "Big Cap (>$10B) — alta liquidità, filtri standard"
    },
    "SNIPER": {
        "volume_min": 1000,
        "voi_min": 2.0,
        "dte_max": 14,
        "dte_min": 1,
        "strike_distance": 5,
        "spread_max": 20.0,
        "delta_min": 0.20,
        "delta_max": 0.80,
        "ask_hit_min": 55.0,
        "desc": "SNIPER — strike vicino, scadenza breve, alta pressione"
    },
    "HOT ONLY": {
        "volume_min": 2000,
        "voi_min": 3.0,
        "dte_max": 7,
        "dte_min": 1,
        "strike_distance": 3,
        "spread_max": 20.0,
        "delta_min": 0.25,
        "delta_max": 0.75,
        "ask_hit_min": 60.0,
        "desc": "HOT ONLY — solo flussi anomali estremi, scadenza imminente"
    },
}

preset = PRESETS[mode]
st.caption(f"ℹ️ {preset['desc']}")

# Reset sliders al cambio modalità
APP_VERSION = "4.0"
if ("last_mode" not in st.session_state or
    st.session_state.get("last_mode") != mode or
    st.session_state.get("app_version") != APP_VERSION):
    st.session_state["app_version"]   = APP_VERSION
    st.session_state["last_mode"]     = mode
    st.session_state["volume_min"]    = preset["volume_min"]
    st.session_state["voi_min"]       = float(preset["voi_min"])
    st.session_state["dte_max"]       = preset["dte_max"]
    st.session_state["dte_min"]       = preset["dte_min"]
    st.session_state["strike_dist"]   = preset["strike_distance"]
    st.session_state["spread_max"]    = float(preset["spread_max"])
    st.session_state["delta_min"]     = float(preset["delta_min"])
    st.session_state["delta_max"]     = float(preset["delta_max"])
    st.session_state["ask_hit_min"]   = float(preset["ask_hit_min"])

# =========================
# SLIDERS
# =========================
col_s1, col_s2 = st.columns(2)
with col_s1:
    volume_min      = st.slider("Volume minimo (contratti)",  0,     50000,  st.session_state["volume_min"],              key="volume_min")
    voi_min         = st.slider("VOI minimo (vol/OI)",        0.0,   20.0,   st.session_state["voi_min"],    step=0.1,    key="voi_min")
    dte_min         = st.slider("DTE minimo (giorni)",        0,     30,     st.session_state["dte_min"],                 key="dte_min")
    dte_max         = st.slider("DTE massimo (giorni)",       1,     365,    st.session_state["dte_max"],                 key="dte_max")
with col_s2:
    strike_distance = st.slider("Distanza strike %",          1,     50,     st.session_state["strike_dist"],             key="strike_dist")
    spread_max      = st.slider("Spread bid/ask max ($)",     0.01,  20.0,   st.session_state["spread_max"], step=0.01,   key="spread_max")
    delta_min       = st.slider("Delta minimo",               0.0,   1.0,    st.session_state["delta_min"],  step=0.01,   key="delta_min")
    delta_max       = st.slider("Delta massimo",              0.0,   1.0,    st.session_state["delta_max"],  step=0.01,   key="delta_max")

ask_hit_min = st.slider(
    "Ask Hit % minimo (0 = mostra tutto)",
    0.0, 100.0,
    st.session_state["ask_hit_min"],
    step=5.0,
    key="ask_hit_min",
    help="Filtra contratti dove la pressione d'acquisto supera questa soglia. ≥55% = buyer aggressivo."
)

col1, col2 = st.columns(2)
with col1:
    option_type = st.radio("Tipo opzione", ["CALL", "PUT", "BOTH"], horizontal=True)
with col2:
    send_telegram = st.checkbox("📲 Attiva Telegram Alerts", value=False)

tickers_input = st.text_input("Ticker (separati da virgola)", "SPY,QQQ,AAPL,TSLA")

# =========================
# HELPERS
# =========================

def format_k(x):
    if x >= 1_000_000:
        return f"{x / 1_000_000:.1f}M"
    elif x >= 1_000:
        return f"{x / 1_000:.1f}K"
    return str(int(x))


def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(url, data=payload, timeout=5)
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
            price = (
                data.get("day", {}).get("c") or
                data.get("day", {}).get("o") or
                data.get("prevDay", {}).get("c") or
                data.get("lastTrade", {}).get("p")
            )
            if price and float(price) > 0.5:
                return round(float(price), 2)
    except Exception:
        pass

    try:
        url2 = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
        r2 = requests.get(url2, params={"apiKey": POLYGON_API_KEY}, timeout=8)
        if r2.status_code == 200:
            res = r2.json().get("results", [])
            if res:
                return round(float(res[0]["c"]), 2)
    except Exception:
        pass

    try:
        url3 = f"https://api.polygon.io/v3/snapshot/options/{ticker}"
        r3 = requests.get(url3, params={"apiKey": POLYGON_API_KEY, "limit": 1}, timeout=8)
        if r3.status_code == 200:
            results = r3.json().get("results", [])
            if results:
                day = results[0].get("day", {})
                price3 = day.get("close") or day.get("previous_close")
                if price3 and float(price3) > 0.5:
                    return round(float(price3), 2)
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
    all_rows = []
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker}"

    params = {
        "apiKey":              POLYGON_API_KEY,
        "expiration_date.gte": exp_min,
        "expiration_date.lte": exp_max,
        "limit":               250,
    }

    page_count = 0
    while url and page_count < 20:
        try:
            r = requests.get(url, params=params, timeout=12)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            break

        all_rows.extend(data.get("results", []))

        next_url = data.get("next_url")
        if next_url:
            url    = next_url + f"&apiKey={POLYGON_API_KEY}"
            params = {}
        else:
            url = None
        page_count += 1

    return all_rows


# =========================
# ASK HIT REALE — Polygon trades
# Legge gli ultimi N trades del contratto e calcola la % eseguita at/near ask
# =========================

@st.cache_data(ttl=120, show_spinner=False)
def get_ask_hit_real(contract_ticker: str, bid: float, ask: float) -> tuple[float | None, bool]:
    """
    Restituisce (ask_hit_pct, is_sweep).
    ask_hit_pct: % trades eseguiti at/near ask (buyer aggressivo)
    is_sweep: True se il contratto ha trades su ≥2 exchange diversi negli ultimi trades
    """
    if not contract_ticker:
        return None, False

    try:
        url = f"https://api.polygon.io/v2/trades/{contract_ticker}"
        params = {
            "apiKey": POLYGON_API_KEY,
            "limit":  50,        # ultimi 50 trades
            "order": "desc",
            "sort":  "timestamp",
        }
        r = requests.get(url, params=params, timeout=8)
        if r.status_code != 200:
            return None, False

        trades = r.json().get("results", [])
        if not trades:
            return None, False

        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else None

        ask_hits   = 0
        total_seen = 0
        exchanges  = set()

        for t in trades:
            price      = t.get("price", 0)
            exchange   = t.get("exchange")
            conditions = t.get("conditions", [])

            if exchange:
                exchanges.add(exchange)

            # Salta trades con condizioni speciali (opening, cancelled, etc.)
            skip_conditions = {12, 13, 14, 15, 16, 17, 18, 41}
            if any(c in skip_conditions for c in (conditions or [])):
                continue

            if price <= 0:
                continue

            total_seen += 1

            # Classifica il trade
            if mid is not None:
                if price >= ask * 0.995:       # pagato ask o sopra → buyer aggressivo
                    ask_hits += 1
                elif price <= bid * 1.005:     # eseguito a bid o sotto → seller aggressivo
                    pass
                # tra bid e ask = mid, non conta
            else:
                # Senza bid/ask usa aggressor_side se disponibile
                side = t.get("aggressor_side", "")
                if side == "buyer":
                    ask_hits += 1

        if total_seen == 0:
            return None, False

        ask_hit_pct = round((ask_hits / total_seen) * 100, 1)
        is_sweep    = len(exchanges) >= 2   # trade su ≥2 exchange = sweep istituzionale

        return ask_hit_pct, is_sweep

    except Exception:
        return None, False


# =========================
# PARSE + FILTRI
# =========================

def parse_and_filter(raw: list[dict], underlying: float, ticker: str) -> pd.DataFrame:
    rows = []
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

        if not strike or not exp_str:
            continue

        try:
            exp_dt = pd.to_datetime(exp_str)
        except Exception:
            continue

        volume = day.get("volume") or 0
        oi     = item.get("open_interest") or 0
        bid    = quotes.get("bid") or 0
        ask    = quotes.get("ask") or 0

        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2
        elif ask > 0:
            mid = ask
        elif bid > 0:
            mid = bid
        else:
            mid = day.get("close") or day.get("vwap") or 0

        iv    = item.get("implied_volatility") or 0
        delta = greeks.get("delta")
        gamma = greeks.get("gamma")
        theta = greeks.get("theta")
        vega  = greeks.get("vega")

        spread = (ask - bid) if (ask > 0 and bid > 0) else None

        rows.append({
            "ticker_sym":  ticker_sym,
            "type":        contract_type,
            "strike":      strike,
            "expiration":  exp_dt,
            "volume":      int(volume),
            "OI":          int(oi),
            "MID":         round(mid, 2),
            "bid":         round(bid, 2),
            "ask":         round(ask, 2),
            "SPREAD":      round(spread, 2) if spread is not None else None,
            "IV":          round(iv * 100, 1) if iv else None,
            "delta":       round(abs(delta), 3) if delta is not None else None,
            "gamma":       round(gamma, 4)      if gamma is not None else None,
            "theta":       round(theta, 3)      if theta is not None else None,
            "vega":        round(vega, 3)       if vega is not None else None,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # METRICHE
    df["VOI"]            = (df["volume"] / df["OI"].replace(0, 1)).round(2)
    df["DTE"]            = (df["expiration"] - today).dt.days
    df["DIST_STRIKE"]    = ((df["strike"] - underlying).abs() / underlying * 100).round(1)
    df["UNDER"]          = underlying
    df["FLOW_POWER_NUM"] = (df["volume"] * df["MID"]).round(0)

    # FILTRI BASE
    df = df[df["volume"]        >= volume_min]
    df = df[df["VOI"]           >= voi_min]
    df = df[df["DTE"]           >= dte_min]
    df = df[df["DTE"]           <= dte_max]
    df = df[df["DIST_STRIKE"]   <= strike_distance]

    if spread_max < 20.0:
        df = df[(df["SPREAD"].isna()) | (df["SPREAD"] <= spread_max)]

    df = df[
        df["delta"].isna() |
        ((df["delta"] >= delta_min) & (df["delta"] <= delta_max))
    ]

    # PUT/CALL RATIO — prima del filtro tipo
    calls_all = df[df["type"] == "CALL"]["volume"].sum()
    puts_all  = df[df["type"] == "PUT"]["volume"].sum()
    df.attrs["pc_ratio"]  = round(puts_all / calls_all, 2) if calls_all > 0 else 0
    df.attrs["calls_vol"] = int(calls_all)
    df.attrs["puts_vol"]  = int(puts_all)

    if option_type != "BOTH":
        df = df[df["type"] == option_type]

    if df.empty:
        return df

    # LABEL OPZIONE
    df["OPZIONE"] = (
        ticker + " " +
        df["expiration"].dt.strftime("%b").str.upper() + " " +
        df["expiration"].dt.strftime("%d") + " '" +
        df["expiration"].dt.strftime("%y") + " " +
        df["strike"].apply(lambda x: str(int(x)) if x == int(x) else str(x)) + " " +
        df["type"].str[0]
    )

    df["CLUSTER"] = df.groupby("strike")["volume"].transform("sum").apply(format_k)
    df["FLOW $"]  = df["FLOW_POWER_NUM"].apply(format_k)

    def signal(row):
        try:
            voi = float(row["VOI"])
        except:
            voi = 0
        if voi >= 5.0: return "🟢 GO"
        if voi >= 2.0: return "🟡 HOLD"
        return "🔴 STOP"

    df["SIG"]  = df.apply(signal, axis=1)
    df["BIAS"] = df["type"].apply(lambda x: "📈 L" if x == "CALL" else "📉 S")

    df["ITM"] = df.apply(
        lambda r: "✅" if (r["type"] == "CALL" and r["strike"] < underlying)
                       or (r["type"] == "PUT"  and r["strike"] > underlying) else "", axis=1)
    df["ATM"] = df["DIST_STRIKE"].apply(lambda x: "🎯" if x <= 1.0 else "")
    df["OTM"] = df.apply(
        lambda r: "⬆️" if (r["type"] == "CALL" and r["strike"] > underlying)
                       or (r["type"] == "PUT"  and r["strike"] < underlying) else "", axis=1)

    return df.sort_values("FLOW_POWER_NUM", ascending=False)


# =========================
# ARRICCHIMENTO ASK HIT + SWEEP (solo top N per non fare troppe chiamate API)
# =========================

def enrich_with_flow_data(df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    """
    Chiama Polygon trades per i top_n contratti per volume.
    Aggiunge colonne ASK_HIT (%) e SWEEP (🌊 o "").
    """
    df = df.copy()
    df["ASK_HIT"] = None
    df["SWEEP"]   = ""

    # Lavoriamo solo sulle prime top_n righe (già ordinate per FLOW_POWER_NUM)
    idx_list = df.head(top_n).index.tolist()

    progress = st.progress(0, text="📡 Analisi flusso in corso...")
    total = len(idx_list)

    for i, idx in enumerate(idx_list):
        row           = df.loc[idx]
        contract_sym  = row.get("ticker_sym", "")
        bid           = row.get("bid", 0)
        ask           = row.get("ask", 0)

        if contract_sym:
            ask_hit_pct, is_sweep = get_ask_hit_real(contract_sym, bid, ask)
            df.at[idx, "ASK_HIT"] = ask_hit_pct
            df.at[idx, "SWEEP"]   = "🌊" if is_sweep else ""

        progress.progress((i + 1) / total, text=f"📡 Flow analysis: {i+1}/{total}")

    progress.empty()
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

    if df.empty:
        return df

    # Mostra P/C ratio
    pc = df.attrs.get("pc_ratio", 0)
    cv = df.attrs.get("calls_vol", 0)
    pv = df.attrs.get("puts_vol", 0)
    bias = "📈 BULLISH" if pc < 0.8 else ("📉 BEARISH" if pc > 1.2 else "⚖️ NEUTRO")
    st.caption(f"Put/Call Ratio: **{pc}** | CALL vol: {cv:,} | PUT vol: {pv:,} | Bias: {bias}")

    # Arricchisci con Ask Hit reale e Sweep
    df = enrich_with_flow_data(df, top_n=15)

    # Filtro Ask Hit minimo (solo sulle righe che hanno il dato)
    if ask_hit_min > 0:
        df = df[df["ASK_HIT"].isna() | (df["ASK_HIT"] >= ask_hit_min)]

    return df


# =========================
# LEGENDA
# =========================
with st.expander("📖 Legenda colonne nuove"):
    st.markdown("""
| Colonna | Significato |
|---|---|
| **ASK_HIT %** | % dei trades eseguiti at/near ask. ≥70% 🟢 buyer aggressivo, ≤30% 🔴 seller aggressivo, 30-70% 🟡 neutro |
| **SWEEP** | 🌊 = contratto tradato su ≥2 exchange simultaneamente → segnale di intenzionalità istituzionale |

**Come leggere ASK_HIT:**
- **≥70%** → i buyer stanno pagando il prezzo ask → **pressione rialzista reale**
- **≤30%** → i seller abbassano al bid → **pressione ribassista o liquidazione**
- **30–70%** → flusso misto / neutro

**Come leggere SWEEP:**
- Un sweep su più exchange significa che qualcuno ha comprato/venduto su tutti i market maker contemporaneamente per riempire un ordine grande velocemente → **segnale istituzionale forte**
""")

# =========================
# SCAN BUTTON
# =========================
if st.button("🚀 Scansiona mercato", type="primary", use_container_width=True):

    tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]

    if not tickers:
        st.error("Inserisci almeno un ticker.")
        st.stop()

    final_df      = pd.DataFrame()
    telegram_text = ""

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
                    greeks_line = (
                        f"Δ {row['delta']}  Γ {row['gamma']}  "
                        f"Θ {row['theta']}  V {row['vega']}\n"
                    )
                ask_hit_val = row.get("ASK_HIT")
                sweep_val   = row.get("SWEEP", "")
                flow_line   = f"Flow: <b>{row['FLOW $']}</b>  |  VOI: {row['VOI']}  |  Vol: {row['volume']}\n"
                hit_line    = ""
                if ask_hit_val is not None:
                    hit_emoji = "🟢" if ask_hit_val >= 70 else ("🔴" if ask_hit_val <= 30 else "🟡")
                    hit_line  = f"Ask Hit: {hit_emoji} <b>{ask_hit_val:.0f}%</b>  {sweep_val}\n"

                telegram_text += (
                    f"{row['SIG']}  {row['BIAS']}\n"
                    f"<b>{row['OPZIONE']}</b>\n"
                    f"Underlying: ${row['UNDER']}  |  Mid: ${row['MID']}\n"
                    f"{flow_line}"
                    f"{hit_line}"
                    f"{greeks_line}\n"
                )

    # DISPLAY
    if not final_df.empty:
        st.success(f"✅ Trovate {len(final_df)} opportunità totali")

        display_cols = [
            "SIG", "FLOW $", "CLUSTER", "BIAS", "SWEEP", "OPZIONE",
            "UNDER", "MID", "bid", "ask", "SPREAD",
            "volume", "OI", "VOI", "DTE", "IV",
            "ASK_HIT",
            "delta", "gamma", "theta", "vega",
            "ITM", "ATM", "OTM",
        ]
        display_cols = [c for c in display_cols if c in final_df.columns]

        def highlight_sig(val):
            if "GO"   in str(val): return "background-color:#1a3a1a; color:#00ff88"
            if "HOLD" in str(val): return "background-color:#3a3a0a; color:#ffdd00"
            if "STOP" in str(val): return "background-color:#3a0a0a; color:#ff4444"
            return ""

        def highlight_ask_hit(val):
            try:
                v = float(val)
                if v >= 70: return "background-color:#1a3a1a; color:#00ff88"
                if v <= 30: return "background-color:#3a0a0a; color:#ff4444"
                return "background-color:#3a3a0a; color:#ffdd00"
            except:
                return ""

        def highlight_sweep(val):
            if val == "🌊":
                return "background-color:#1a1a3a; color:#88aaff"
            return ""

        styled = (
            final_df[display_cols]
            .reset_index(drop=True)
            .style
            .map(highlight_sig,      subset=["SIG"])
            .map(highlight_ask_hit,  subset=["ASK_HIT"] if "ASK_HIT" in final_df.columns else [])
            .map(highlight_sweep,    subset=["SWEEP"]   if "SWEEP"   in final_df.columns else [])
            .format({
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
            }, na_rep="—")
        )

        st.dataframe(styled, use_container_width=True, hide_index=True)

        if send_telegram and telegram_text:
            ok = send_telegram_message(telegram_text)
            if ok:
                st.success("📲 Alert Telegram inviato!")
            else:
                st.error("❌ Errore invio Telegram")
    else:
        st.warning("⚠️ Nessuna opportunità trovata. Prova ad allargare i filtri.")

# =========================
# FOOTER
# =========================
st.divider()
st.caption(
    "⚠️ Questo tool è uno screener di primo livello. "
    "L'analisi finale (grafico, contesto macro, greche) va completata su IBKR. "
    "Nessun ordine viene eseguito automaticamente."
)
