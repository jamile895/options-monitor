import streamlit as st
import requests

st.title("🔧 Polygon.io — Test Diagnostico")

POLYGON_API_KEY = st.secrets["POLYGON_API_KEY"]

ticker = st.text_input("Ticker", "TSLA")

if st.button("TEST"):

    st.subheader("TEST 1 — Snapshot Opzioni")
    try:
        r = requests.get(
            f"https://api.polygon.io/v3/snapshot/options/{ticker}",
            params={"apiKey": POLYGON_API_KEY, "limit": 1},
            timeout=10
        )
        st.write(f"Status: {r.status_code}")
        data = r.json()
        results = data.get("results", [])
        st.write(f"Risultati: {len(results)}")
        if results:
            st.write("Primo risultato:")
            st.json(results[0])
    except Exception as e:
        st.error(f"Errore: {e}")

    st.divider()
    st.subheader("TEST 2 — Aggs Prev Close")
    try:
        r2 = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev",
            params={"apiKey": POLYGON_API_KEY},
            timeout=10
        )
        st.write(f"Status: {r2.status_code}")
        st.json(r2.json())
    except Exception as e:
        st.error(f"Errore: {e}")

    st.divider()
    st.subheader("TEST 3 — Last Trade")
    try:
        r3 = requests.get(
            f"https://api.polygon.io/v2/last/trade/{ticker}",
            params={"apiKey": POLYGON_API_KEY},
            timeout=10
        )
        st.write(f"Status: {r3.status_code}")
        st.json(r3.json())
    except Exception as e:
        st.error(f"Errore: {e}")
