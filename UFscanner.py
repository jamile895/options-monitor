import streamlit as st
import pandas as pd
import requests
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
st.caption("Powered by Polygon.io — Greeks included")

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
        "desc": "HOT ONLY — solo flussi anomali estremi, scadenza imminente"
    },
}

preset = PRESETS[mode]
st.caption(f"ℹ️ {preset['desc']}")

# Reset sliders al cambio modalità
APP_VERSION = "3.5"
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

# =========================
# SLIDERS
# =========================
col_s1, col_s2 = st.columns(2)
with col_s1:
    volume_min      = st.slider("Volume minimo (contratti)",  0,     50000,  st.session_state["volume_min"],             key="volume_min")
    voi_min         = st.slider("VOI minimo (vol/OI)",        0.0,   20.0,   st.session_state["voi_min"],   step=0.1,    key="voi_min")
    dte_min         = st.slider("DTE minimo (giorni)",        0,     30,     st.session_state["dte_min"],                key="dte_min")
    dte_max         = st.slider("DTE massimo (giorni)",       1,     365,    st.session_state["dte_max"],                key="dte_max")
with col_s2:
    strike_distance = st.slider("Distanza strike %",          1,     50,     st.session_state["strike_dist"],            key="strike_dist")
    spread_max      = st.slider("Spread bid/ask max ($)",     0.01,  20.0,   st.session_state["spread_max"], step=0.01,  key="spread_max")
    delta_min       = st.slider("Delta minimo",               0.0,   1.0,    st.session_state["delta_min"],  step=0.01,  key="delta_min")
    delta_max       = st.slider("Delta massimo",              0.0,   1.0,    st.session_state["delta_max"],  step=0.01,  key="delta_max")

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
# Usa endpoint stocks snapshot per prezzo affidabile
# =========================

def get_stock_price(ticker: str) -> float | None:
    # Metodo 1: snapshot stocks (più affidabile)
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

    # Metodo 2: aggregati prev close
    try:
        url2 = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
        r2 = requests.get(url2, params={"apiKey": POLYGON_API_KEY}, timeout=8)
        if r2.status_code == 200:
            res = r2.json().get("results", [])
            if res:
                return round(float(res[0]["c"]), 2)
    except Exception:
        pass

    # Metodo 3: snapshot opzioni — close del primo contratto
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

def get_options_chain(ticker: str, dte_min: int, dte_max: int) -> list[dict]:
    today   = datetime.today().date()
    exp_min = (today + timedelta(days=dte_min)).isoformat()
    exp_max = (today + timedelta(days=dte_max)).isoformat()
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

        # Prezzo opzione: midpoint bid/ask è il più affidabile intraday
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

        # Spread in dollari
        spread = (ask - bid) if (ask > 0 and bid > 0) else None

        # Ask Hitting % stimato da vwap e open/close
        vwap      = day.get("vwap") or 0
        day_open  = day.get("open") or 0
        day_close = day.get("close") or 0

        if vwap > 0 and mid > 0:
            # Se MID > vwap = pressione acquisto (hitting ask)
            ask_hit = round(min(100, max(0, ((mid - vwap) / vwap * 100 + 50))), 1)
        elif day_open > 0 and day_close > 0:
            # Fallback: confronto open/close
            ask_hit = round(min(100, max(0, (day_close / day_open * 50))), 1)
        else:
            ask_hit = None

        rows.append({
            "ticker_sym": ticker_sym,
            "type":       contract_type,
            "strike":     strike,
            "expiration": exp_dt,
            "volume":     int(volume),
            "OI":         int(oi),
            "MID":        round(mid, 2),
            "bid":        round(bid, 2),
            "ask":        round(ask, 2),
            "SPREAD":     round(spread, 2) if spread is not None else None,
            "IV":         round(iv * 100, 1) if iv else None,
            "delta":      round(abs(delta), 3) if delta is not None else None,
            "gamma":      round(gamma, 4) if gamma is not None else None,
            "theta":      round(theta, 3) if theta is not None else None,
            "vega":       round(vega, 3) if vega is not None else None,
            "ASK_HIT":    ask_hit,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # METRICHE
    df["VOI"]            = (df["volume"] / df["OI"].replace(0, 1)).round(2)
    df["ASK_HIT"]        = df["ASK_HIT"].round(0) if "ASK_HIT" in df.columns else df.get("ASK_HIT")
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

    # FILTRO SPREAD — solo se dato disponibile
    if spread_max < 20.0:
        df = df[(df["SPREAD"].isna()) | (df["SPREAD"] <= spread_max)]

    # FILTRO DELTA — solo se greche disponibili
    df = df[
        df["delta"].isna() |
        ((df["delta"] >= delta_min) & (df["delta"] <= delta_max))
    ]

    # PUT/CALL RATIO — calcolato PRIMA del filtro tipo opzione
    calls_all = df[df["type"] == "CALL"]["volume"].sum()
    puts_all  = df[df["type"] == "PUT"]["volume"].sum()
    df.attrs["pc_ratio"]   = round(puts_all / calls_all, 2) if calls_all > 0 else 0
    df.attrs["calls_vol"]  = int(calls_all)
    df.attrs["puts_vol"]   = int(puts_all)

    # FILTRO TIPO OPZIONE
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

    # CLUSTER — volume totale per strike
    df["CLUSTER"] = df.groupby("strike")["volume"].transform("sum").apply(format_k)

    # FLOW POWER formattato
    df["FLOW $"] = df["FLOW_POWER_NUM"].apply(format_k)

    # SIGNAL — basato su VOI (affidabile) + FLOW come conferma
    def signal(row):
        voi  = row["VOI"]
        flow = row["FLOW_POWER_NUM"]
        # GO: VOI alto E flow significativo
        if voi >= 5.0 and flow >= 50_000:  return "🟢 GO"
        if voi >= 5.0:                      return "🟢 GO"
        # HOLD: VOI medio o flow medio
        if voi >= 2.0 and flow >= 20_000:  return "🟡 HOLD"
        if voi >= 2.0:                      return "🟡 HOLD"
        if flow >= 50_000:                  return "🟡 HOLD"
        return "🔴 STOP"

    df["SIG"] = df.apply(signal, axis=1)
    df["BIAS"] = df["type"].apply(lambda x: "📈 L" if x == "CALL" else "📉 S")

    # ITM / ATM / OTM
    df["ITM"] = df.apply(
        lambda r: "✅" if (r["type"] == "CALL" and r["strike"] < underlying)
                       or (r["type"] == "PUT"  and r["strike"] > underlying) else "", axis=1)
    df["ATM"] = df["DIST_STRIKE"].apply(lambda x: "🎯" if x <= 1.0 else "")
    df["OTM"] = df.apply(
        lambda r: "⬆️" if (r["type"] == "CALL" and r["strike"] > underlying)
                       or (r["type"] == "PUT"  and r["strike"] < underlying) else "", axis=1)

    return df.sort_values("FLOW_POWER_NUM", ascending=False)


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

    if not df.empty:
        pc = df.attrs.get("pc_ratio", 0)
        cv = df.attrs.get("calls_vol", 0)
        pv = df.attrs.get("puts_vol", 0)
        bias = "📈 BULLISH" if pc < 0.8 else ("📉 BEARISH" if pc > 1.2 else "⚖️ NEUTRO")
        st.caption(f"Put/Call Ratio: **{pc}** | CALL vol: {cv:,} | PUT vol: {pv:,} | Bias: {bias}")

    return df


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
                telegram_text += (
                    f"{row['SIG']}  {row['BIAS']}\n"
                    f"<b>{row['OPZIONE']}</b>\n"
                    f"Underlying: ${row['UNDER']}  |  Mid: ${row['MID']}\n"
                    f"Flow: <b>{row['FLOW $']}</b>  |  VOI: {row['VOI']}  |  Vol: {row['volume']}\n"
                    f"{greeks_line}\n"
                )

    # DISPLAY
    if not final_df.empty:
        st.success(f"✅ Trovate {len(final_df)} opportunità totali")

        display_cols = [
            "SIG", "FLOW $", "CLUSTER", "BIAS", "OPZIONE",
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
                if v >= 70: return "background-color:#1a3a1a; color:#00ff88"  # verde = acquisto
                if v <= 30: return "background-color:#3a0a0a; color:#ff4444"  # rosso = vendita
                return "background-color:#3a3a0a; color:#ffdd00"              # giallo = neutro
            except:
                return ""

        styled = (
            final_df[display_cols]
            .reset_index(drop=True)
            .style
            .map(highlight_sig, subset=["SIG"])
            .map(highlight_ask_hit, subset=["ASK_HIT"] if "ASK_HIT" in final_df.columns else [])
            .format({
                "UNDER":   lambda x: f"${x:.2f}"       if pd.notna(x) else "—",
                "MID":     lambda x: f"${x:.2f}"       if pd.notna(x) else "—",
                "bid":     lambda x: f"${x:.2f}"       if pd.notna(x) else "—",
                "ask":     lambda x: f"${x:.2f}"       if pd.notna(x) else "—",
                "SPREAD":  lambda x: f"${x:.2f}"       if pd.notna(x) else "—",
                "VOI":     lambda x: f"{float(x):.2f}" if pd.notna(x) else "—",
                "ASK_HIT": lambda x: f"{float(x):.0f}%" if pd.notna(x) else "—",
                "IV":      lambda x: f"{x:.1f}%"       if pd.notna(x) else "—",
                "delta":   lambda x: f"{x:.3f}"        if pd.notna(x) else "—",
                "gamma":   lambda x: f"{x:.4f}"        if pd.notna(x) else "—",
                "theta":   lambda x: f"{x:.3f}"        if pd.notna(x) else "—",
                "vega":    lambda x: f"{x:.3f}"        if pd.notna(x) else "—",
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
