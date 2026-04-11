import streamlit as st
import pandas as pd
import yfinance as yf
import requests

# =========================
# TELEGRAM CONFIG
# =========================
TELEGRAM_TOKEN = "INSERISCI_TOKEN"
TELEGRAM_CHAT_ID = "INSERISCI_CHAT_ID"

# =========================
# CONFIG UI
# =========================
st.set_page_config(layout="wide", page_title="Options Flow Scanner PRO")
st.title("🔥 Options Flow Scanner PRO 🔥 by Ugo Fortezze")

# =========================
# MODALITÀ
# =========================
mode = st.radio(
    "Modalità Trading",
    ["MANUALE", "BASE", "AGGRESSIVO", "SNIPER", "HOT ONLY"],
    horizontal=True
)

# =========================
# PRESET — risolve il bug slider/session_state
# =========================
PRESETS = {
    "MANUALE":     {"volume_min": 1000, "voi_min": 1.5, "dte_max": 30, "strike_distance": 10},
    "BASE":        {"volume_min": 500,  "voi_min": 1.2, "dte_max": 30, "strike_distance": 15},
    "AGGRESSIVO":  {"volume_min": 300,  "voi_min": 1.0, "dte_max": 45, "strike_distance": 20},
    "SNIPER":      {"volume_min": 2000, "voi_min": 2.0, "dte_max": 10, "strike_distance": 5},
    "HOT ONLY":    {"volume_min": 5000, "voi_min": 3.0, "dte_max": 7,  "strike_distance": 3},
}

preset = PRESETS[mode]

# FIX: usa key univoca per ogni modalità → evita conflitto Streamlit slider/session_state
volume_min = st.slider("Volume minimo", 0, 10000, preset["volume_min"], key=f"vol_{mode}")
voi_min    = st.slider("VOI minimo", 0.0, 10.0, float(preset["voi_min"]), step=0.1, key=f"voi_{mode}")
dte_max    = st.slider("DTE max", 1, 60, preset["dte_max"], key=f"dte_{mode}")
strike_distance = st.slider("Distanza strike %", 1, 20, preset["strike_distance"], key=f"str_{mode}")

# =========================
# ALTRI PARAMETRI
# =========================
col1, col2 = st.columns(2)
with col1:
    option_type = st.radio("Tipo opzione", ["CALL", "PUT", "BOTH"], horizontal=True)
with col2:
    send_telegram = st.checkbox("📲 Attiva Telegram Alerts", value=False)

tickers_input = st.text_input("Ticker (separati da virgola)", "SPY,QQQ")

# =========================
# FUNZIONI
# =========================

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(url, data=payload, timeout=5)
        return r.ok
    except Exception as e:
        st.warning(f"Telegram error: {e}")
        return False


def get_underlying_price(ticker):
    try:
        data = yf.Ticker(ticker).history(period="1d")
        if data.empty:
            return None
        return round(data["Close"].iloc[-1], 2)
    except Exception:
        return None


def format_k(x):
    if x >= 1_000_000:
        return f"{x / 1_000_000:.1f}M"
    elif x >= 1_000:
        return f"{x / 1_000:.1f}K"
    return str(int(x))


def get_options_data(ticker: str) -> pd.DataFrame:
    tk = yf.Ticker(ticker)
    underlying = get_underlying_price(ticker)

    if underlying is None:
        st.warning(f"⚠️ Impossibile ottenere prezzo per {ticker}")
        return pd.DataFrame()

    rows = []
    expirations = tk.options

    if not expirations:
        st.warning(f"⚠️ Nessuna scadenza disponibile per {ticker}")
        return pd.DataFrame()

    progress = st.progress(0, text=f"Scaricando catene opzioni {ticker}...")

    for i, exp in enumerate(expirations):
        try:
            chain = tk.option_chain(exp)
            for df_chain, typ in [(chain.calls, "CALL"), (chain.puts, "PUT")]:
                df_chain = df_chain.copy()
                df_chain["type"] = typ
                df_chain["expiration"] = pd.to_datetime(exp)
                rows.append(df_chain)
        except Exception:
            continue
        progress.progress((i + 1) / len(expirations), text=f"Scadenza {exp}...")

    progress.empty()

    if not rows:
        return pd.DataFrame()

    df = pd.concat(rows, ignore_index=True)

    # --- METRICHE ---
    df["FLOW_POWER_NUM"] = df["volume"] * df["lastPrice"]
    df["openInterest"] = df["openInterest"].fillna(0)
    df["VOI"] = df["volume"] / df["openInterest"].replace(0, 1)
    df["DTE"] = (df["expiration"] - pd.Timestamp.today()).dt.days
    df["DIST_STRIKE"] = (df["strike"] - underlying).abs() / underlying * 100
    df["underlying"] = underlying

    # --- FILTRI ---
    df = df[df["volume"] >= volume_min]
    df = df[df["VOI"] >= voi_min]
    df = df[df["DTE"] <= dte_max]
    df = df[df["DIST_STRIKE"] <= strike_distance]
    df = df[df["FLOW_POWER_NUM"] >= 100_000]

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
        df["strike"].astype(int).astype(str)
    )

    # --- CLUSTER ---
    df["CLUSTER"] = df.groupby("strike")["volume"].transform("sum").apply(format_k)

    # --- FLOW POWER ---
    df["FLOW_POWER"] = df["FLOW_POWER_NUM"].apply(format_k)

    def signal(x):
        if x > 1_000_000:
            return "🟢 GO"
        elif x > 300_000:
            return "🟡 HOLD"
        return "🔴 STOP"

    df["FLOW STRENGTH"] = df["FLOW_POWER_NUM"].apply(signal)
    df["BIAS"] = df["type"].apply(lambda x: "📈 LONG" if x == "CALL" else "📉 SHORT")

    # --- ITM / ATM / OTM ---
    df["ITM"] = df.apply(
        lambda r: "✅" if (r["type"] == "CALL" and r["strike"] < underlying)
                       or (r["type"] == "PUT"  and r["strike"] > underlying) else "", axis=1
    )
    df["ATM"] = df["DIST_STRIKE"].apply(lambda x: "🎯" if x < 1 else "")
    df["OTM"] = df.apply(
        lambda r: "⬆️" if (r["type"] == "CALL" and r["strike"] > underlying)
                       or (r["type"] == "PUT"  and r["strike"] < underlying) else "", axis=1
    )

    return df


# =========================
# SCAN
# =========================
if st.button("🚀 Scansiona mercato", type="primary", use_container_width=True):

    tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]

    if not tickers:
        st.error("Inserisci almeno un ticker.")
        st.stop()

    final_df = pd.DataFrame()
    telegram_text = ""

    for ticker in tickers:
        st.markdown(f"### 🔍 {ticker}")
        with st.spinner(f"Analisi {ticker}..."):
            df = get_options_data(ticker)

        if df.empty:
            st.info(f"Nessuna opportunità trovata per {ticker} con i filtri correnti.")
            continue

        top = df.sort_values("FLOW_POWER_NUM", ascending=False).head(5)
        final_df = pd.concat([final_df, top], ignore_index=True)

        if send_telegram:
            telegram_text += f"🔥 <b>TOP FLOW — {ticker}</b> [{mode}]\n\n"
            for _, row in top.iterrows():
                telegram_text += (
                    f"{row['FLOW STRENGTH']}  {row['BIAS']}\n"
                    f"<b>{row['OPZIONE']}</b>\n"
                    f"Flow: <b>{row['FLOW_POWER']}</b>  |  VOI: {row['VOI']:.1f}\n\n"
                )

    # --- DISPLAY ---
    if not final_df.empty:
        st.success(f"✅ Trovate {len(final_df)} opportunità totali")

        display_cols = [
            "FLOW STRENGTH", "FLOW_POWER", "CLUSTER", "BIAS", "OPZIONE",
            "underlying", "lastPrice", "bid", "ask", "change",
            "volume", "openInterest", "VOI", "impliedVolatility",
            "ITM", "ATM", "OTM",
        ]
        # Mostra solo colonne effettivamente presenti
        display_cols = [c for c in display_cols if c in final_df.columns]

        st.dataframe(
            final_df[display_cols]
            .sort_values("FLOW_POWER_NUM", ascending=False)
            .reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

        if send_telegram and telegram_text:
            ok = send_telegram_message(telegram_text)
            if ok:
                st.success("📲 Alert Telegram inviato!")
            else:
                st.error("❌ Errore invio Telegram — controlla token e chat_id")
    else:
        st.warning("⚠️ Nessuna opportunità trovata. Prova ad allargare i filtri.")
