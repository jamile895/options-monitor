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
    ["MANUALE", "BASE", "AGGRESSIVO", "SNIPER", "HOT ONLY"],
    horizontal=True
)

# =========================
# PRESET
# =========================
PRESETS = {
    "MANUALE":    {"volume_min": 1000, "voi_min": 1.5, "dte_max": 30, "strike_distance": 10, "spread_max": 0.5},
    "BASE":       {"volume_min": 500,  "voi_min": 1.2, "dte_max": 30, "strike_distance": 15, "spread_max": 0.8},
    "AGGRESSIVO": {"volume_min": 300,  "voi_min": 1.0, "dte_max": 45, "strike_distance": 20, "spread_max": 1.0},
    "SNIPER":     {"volume_min": 2000, "voi_min": 2.0, "dte_max": 10, "strike_distance": 5,  "spread_max": 0.3},
    "HOT ONLY":   {"volume_min": 5000, "voi_min": 3.0, "dte_max": 7,  "strike_distance": 3,  "spread_max": 0.2},
}

preset = PRESETS[mode]

col_s1, col_s2 = st.columns(2)
with col_s1:
    volume_min      = st.slider("Volume minimo",      0,     10000, preset["volume_min"],           key=f"vol_{mode}")
    voi_min         = st.slider("VOI minimo",          0.0,   10.0,  float(preset["voi_min"]),  step=0.1, key=f"voi_{mode}")
    dte_max         = st.slider("DTE max",             1,     60,    preset["dte_max"],               key=f"dte_{mode}")
with col_s2:
    strike_distance = st.slider("Distanza strike %",  1,     20,    preset["strike_distance"],       key=f"str_{mode}")
    spread_max      = st.slider("Spread bid/ask max", 0.05,  2.0,   float(preset["spread_max"]), step=0.05, key=f"spd_{mode}")

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
# POLYGON.IO — PREZZO UNDERLYING
# =========================

def get_underlying_price_polygon(ticker: str) -> float | None:
    url = f"https://api.polygon.io/v2/last/trade/{ticker}"
    params = {"apiKey": POLYGON_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=8)
            if r.status_code == 200:
            data = r.json()
            res = data.get("results", {})
            price = res.get("p") or res.get("c") or res.get("vw")
            if price:
                return round(float(price), 2)
    except Exception:
        pass
    # fallback: previous close
    try:
        url2 = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
        r2 = requests.get(url2, params=params, timeout=8)
        if r2.status_code == 200:
            return round(r2.json()["results"][0]["c"], 2)
    except Exception:
        pass
    return None


# =========================
# POLYGON.IO — CATENA OPZIONI
# =========================

def get_options_chain_polygon(ticker: str, dte_max: int) -> list[dict]:
    """
    Usa l'endpoint /v3/snapshot/options/{ticker} di Polygon.io
    che restituisce greche, volume, OI, bid/ask in una sola chiamata.
    """
    today      = datetime.today().date()
    exp_max    = (today + timedelta(days=dte_max)).isoformat()
    all_rows   = []
    url        = f"https://api.polygon.io/v3/snapshot/options/{ticker}"

    params = {
        "apiKey":           POLYGON_API_KEY,
        "expiration_date.lte": exp_max,
        "limit":            250,
    }

    page_count = 0
    while url and page_count < 20:          # max 20 pagine = 5000 contratti
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            break

        results = data.get("results", [])
        all_rows.extend(results)

        # paginazione cursor
        next_url = data.get("next_url")
        if next_url:
            url    = next_url + f"&apiKey={POLYGON_API_KEY}"
            params = {}          # i params sono già nell'URL
        else:
            url = None
        page_count += 1

    return all_rows


# =========================
# PARSE + FILTRI
# =========================

def parse_polygon_options(raw_results: list[dict], underlying: float, ticker: str) -> pd.DataFrame:
    rows = []
    today = pd.Timestamp.today().normalize()

    for item in raw_results:
        details  = item.get("details", {})
        greeks   = item.get("greeks", {})
        day      = item.get("day", {})
        quotes   = item.get("last_quote", {})

        contract_type = details.get("contract_type", "").upper()   # call/put
        strike        = details.get("strike_price")
        exp_str       = details.get("expiration_date", "")
        ticker_sym    = details.get("ticker", "")

        try:
            exp_dt = pd.to_datetime(exp_str)
        except Exception:
            continue

        volume = day.get("volume") or 0
        oi     = item.get("open_interest") or 0
        last   = day.get("close") or day.get("last") or 0
        bid    = quotes.get("bid") or 0
        ask    = quotes.get("ask") or 0
        iv     = item.get("implied_volatility") or 0

        # greche
        delta  = greeks.get("delta")
        gamma  = greeks.get("gamma")
        theta  = greeks.get("theta")
        vega   = greeks.get("vega")

        rows.append({
            "ticker_sym":  ticker_sym,
            "type":        contract_type,
            "strike":      strike,
            "expiration":  exp_dt,
            "volume":      volume,
            "OI":          oi,
            "LAST":        last,
            "bid":         bid,
            "ask":         ask,
            "IV":          round(iv * 100, 1) if iv else None,     # in %
            "delta":       round(delta, 3) if delta else None,
            "gamma":       round(gamma, 4) if gamma else None,
            "theta":       round(theta, 3) if theta else None,
            "vega":        round(vega, 3) if vega else None,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # --- METRICHE ---
    df["FLOW_POWER_NUM"] = df["volume"] * df["LAST"]
    df["VOI"]            = df["volume"] / df["OI"].replace(0, 1)
    df["DTE"]            = (df["expiration"] - today).dt.days
    df["DIST_STRIKE"]    = (df["strike"] - underlying).abs() / underlying * 100
    df["SPREAD"]         = df["ask"] - df["bid"]
    df["UNDER"]          = underlying

    # --- FILTRI ---
    df = df[df["volume"]           >= volume_min]
    df = df[df["VOI"]              >= voi_min]
    df = df[df["DTE"]              <= dte_max]
    df = df[df["DTE"]              >= 0]
    df = df[df["DIST_STRIKE"]      <= strike_distance]
    df = df[df["FLOW_POWER_NUM"]   >= 100_000]
    df = df[df["SPREAD"]           <= spread_max]

    if option_type != "BOTH":
        df = df[df["type"] == option_type]

    if df.empty:
        return df

    # --- LABEL OPZIONE ---
    df["OPZIONE"] = (
        ticker + " " +
        df["expiration"].dt.strftime("%b").str.upper() + " " +
        df["expiration"].dt.strftime("%d") + " '" +
        df["expiration"].dt.strftime("%y") + " " +
        df["strike"].astype(int).astype(str) + " " +
        df["type"].str[0]                          # C / P
    )

    # --- CLUSTER ---
    df["CLUSTER"] = df.groupby("strike")["volume"].transform("sum").apply(format_k)

    # --- FLOW POWER ---
    df["FLOW $"] = df["FLOW_POWER_NUM"].apply(format_k)

    # --- SIGNAL ---
    def signal(x):
        if x > 1_000_000:
            return "🟢 GO"
        elif x > 300_000:
            return "🟡 HOLD"
        return "🔴 STOP"

    df["SIG"]  = df["FLOW_POWER_NUM"].apply(signal)
    df["BIAS"] = df["type"].apply(lambda x: "📈 L" if x == "CALL" else "📉 S")

    # --- ITM / ATM / OTM ---
    df["ITM"] = df.apply(
        lambda r: "✅" if (r["type"] == "CALL" and r["strike"] < underlying)
                       or (r["type"] == "PUT"  and r["strike"] > underlying) else "", axis=1)
    df["ATM"] = df["DIST_STRIKE"].apply(lambda x: "🎯" if x < 1 else "")
    df["OTM"] = df.apply(
        lambda r: "⬆️" if (r["type"] == "CALL" and r["strike"] > underlying)
                       or (r["type"] == "PUT"  and r["strike"] < underlying) else "", axis=1)

    # --- VOI fmt ---
    df["VOI"] = df["VOI"].round(2)

    # --- CHG ---
    df["CHG"] = ((df["LAST"] - df["bid"]) / df["bid"].replace(0, 1) * 100).round(1)

    return df


# =========================
# WRAPPER PRINCIPALE
# =========================

def get_options_data(ticker: str) -> pd.DataFrame:
    underlying = get_underlying_price_polygon(ticker)

    if underlying is None:
        st.warning(f"⚠️ Impossibile ottenere prezzo per {ticker}")
        return pd.DataFrame()

    with st.spinner(f"📡 Scaricando catena opzioni {ticker} da Polygon.io..."):
        raw = get_options_chain_polygon(ticker, dte_max)

    if not raw:
        st.warning(f"⚠️ Nessun dato opzioni ricevuto da Polygon per {ticker}")
        return pd.DataFrame()

    df = parse_polygon_options(raw, underlying, ticker)
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
        df = get_options_data(ticker)

        if df.empty:
            st.info(f"Nessuna opportunità trovata per {ticker} con i filtri correnti.")
            continue

        top = df.sort_values("FLOW_POWER_NUM", ascending=False).head(5)
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
                    f"Flow: <b>{row['FLOW $']}</b>  |  VOI: {row['VOI']}  |  Spread: {row['SPREAD']:.2f}\n"
                    f"{greeks_line}\n"
                )

    # --- DISPLAY ---
    if not final_df.empty:
        st.success(f"✅ Trovate {len(final_df)} opportunità totali")

        # colonne base
        display_cols = [
            "SIG", "FLOW $", "CLUSTER", "BIAS", "OPZIONE",
            "UNDER", "LAST", "bid", "ask", "SPREAD", "CHG",
            "volume", "OI", "VOI", "IV",
            "delta", "gamma", "theta", "vega",
            "ITM", "ATM", "OTM",
        ]
        display_cols = [c for c in display_cols if c in final_df.columns]

        # formattazione condizionale SIG
        def highlight_sig(val):
            if "GO"   in str(val): return "background-color:#1a3a1a; color:#00ff88"
            if "HOLD" in str(val): return "background-color:#3a3a0a; color:#ffdd00"
            if "STOP" in str(val): return "background-color:#3a0a0a; color:#ff4444"
            return ""

        styled = (
            final_df[display_cols]
            .reset_index(drop=True)
            .style
            .map(highlight_sig, subset=["SIG"])
            .format({
                "IV":    lambda x: f"{x:.1f}%" if pd.notna(x) else "—",
                "delta": lambda x: f"{x:.3f}"  if pd.notna(x) else "—",
                "gamma": lambda x: f"{x:.4f}"  if pd.notna(x) else "—",
                "theta": lambda x: f"{x:.3f}"  if pd.notna(x) else "—",
                "vega":  lambda x: f"{x:.3f}"  if pd.notna(x) else "—",
                "SPREAD":lambda x: f"{x:.2f}"  if pd.notna(x) else "—",
                "CHG":   lambda x: f"{x:+.1f}%"if pd.notna(x) else "—",
                "UNDER": lambda x: f"${x:.2f}" if pd.notna(x) else "—",
                "LAST":  lambda x: f"${x:.2f}" if pd.notna(x) else "—",
            }, na_rep="—")
        )

        st.dataframe(styled, use_container_width=True, hide_index=True)

        if send_telegram and telegram_text:
            ok = send_telegram_message(telegram_text)
            if ok:
                st.success("📲 Alert Telegram inviato!")
            else:
                st.error("❌ Errore invio Telegram — controlla token e chat_id nei Secrets")
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
