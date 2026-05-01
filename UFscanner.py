import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# =========================
# SECRETS
# =========================
TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
POLYGON_API_KEY  = st.secrets["POLYGON_API_KEY"]
GOOGLE_SHEET_ID  = st.secrets["GOOGLE_SHEET_ID"]

# =========================
# GOOGLE SHEETS — CONNESSIONE
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_gsheet_client():
    try:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
        if isinstance(raw, str):
            service_account_info = json.loads(raw)
        else:
            service_account_info = dict(raw)
        creds  = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.warning(f"⚠️ Google Sheets non disponibile: {e}")
        return None

def get_sheet(sheet_name: str):
    client = get_gsheet_client()
    if client is None:
        return None
    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        st.warning(f"⚠️ Foglio '{sheet_name}' non trovato: {e}")
        return None

# =========================
# CONFIG UI
# =========================
st.set_page_config(layout="wide", page_title="Options Flow Scanner PRO")

# =========================
# STORICO SCANSIONI
# =========================
HISTORY_COLS           = ["date","ticker","strike","expiration","type","flow","voi","ask_hit","sweep","iv"]
WATCHLIST_COLS         = ["ticker","strike","expiration","type","note","added"]
WATCHLIST_HISTORY_COLS = ["date","ticker","strike","expiration","type","mid","voi","iv","volume","underlying"]

@st.cache_data(ttl=300)
def load_history() -> list:
    sheet = get_sheet("history")
    if sheet:
        try:
            rows = sheet.get_all_records()
            return rows
        except Exception:
            pass
    if os.path.exists("scan_history.json"):
        try:
            with open("scan_history.json", "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []

_history_buffer = []

def save_history_row(entry: dict):
    global _history_buffer
    _history_buffer.append(entry)

def flush_history_buffer():
    global _history_buffer
    if not _history_buffer:
        return
    import time
    sheet = get_sheet("history")
    if sheet:
        for attempt in range(3):
            try:
                existing = sheet.row_values(1)
                if not existing:
                    sheet.append_row(HISTORY_COLS)
                rows = [[str(e.get(c, "")) for c in HISTORY_COLS] for e in _history_buffer]
                sheet.append_rows(rows)
                _history_buffer = []
                load_history.clear()
                return True
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                break
    try:
        history = []
        if os.path.exists("scan_history.json"):
            with open("scan_history.json", "r") as f:
                history = json.load(f)
        history.extend(_history_buffer)
        history = history[-500:]
        with open("scan_history.json", "w") as f:
            json.dump(history, f)
        _history_buffer = []
    except Exception:
        pass
    return False

def add_to_history(ticker, strike, expiration, contract_type, flow_power, voi, ask_hit, sweep, iv=None):
    today_str = datetime.today().strftime("%Y-%m-%d")
    for e in _history_buffer:
        if (str(e.get("date","")) == today_str and
            str(e.get("ticker","")) == str(ticker) and
            str(e.get("strike","")) == str(strike) and
            str(e.get("expiration","")) == str(expiration) and
            str(e.get("type","")) == str(contract_type)):
            return
    entry = {
        "date":       today_str,
        "ticker":     str(ticker),
        "strike":     str(strike),
        "expiration": str(expiration),
        "type":       str(contract_type),
        "flow":       round(float(flow_power), 0),
        "voi":        round(float(voi), 2),
        "ask_hit":    round(float(ask_hit), 1) if ask_hit is not None else None,
        "sweep":      str(sweep),
        "iv":         round(float(iv), 2) if iv is not None else None,
    }
    save_history_row(entry)

def get_cluster_repeat(ticker, strike, expiration, contract_type, history=None) -> int:
    if history is None:
        history = load_history()
    days = set()
    for e in history:
        if (str(e.get("ticker","")) == str(ticker) and
            str(e.get("strike","")) == str(strike) and
            str(e.get("expiration","")) == str(expiration) and
            str(e.get("type","")) == str(contract_type)):
            days.add(str(e.get("date","")))
    return len(days)

def earnings_in_dte(ticker: str, dte_max_days: int) -> tuple[bool, str]:
    """
    v7.0 — Earnings manuali: legge da st.session_state["earnings_dates"]
    che è un dict {ticker: "YYYY-MM-DD"} popolato dall'utente nella sidebar.
    La stima automatica Polygon è stata rimossa perché inaffidabile.
    """
    earnings_map = st.session_state.get("earnings_dates", {})
    earnings_date = earnings_map.get(ticker.upper(), "")
    if not earnings_date: return False, ""
    try:
        today = datetime.today().date()
        ed = datetime.strptime(earnings_date, "%Y-%m-%d").date()
        days_to_earnings = (ed - today).days
        if 0 <= days_to_earnings <= dte_max_days:
            return True, earnings_date
    except Exception: pass
    return False, ""

def compute_score(voi, ask_hit, sweep, whale_days, flow_num, has_earn, iv=None):
    score = 0
    try:
        v = float(voi)
        if   v >= 10: score += 30
        elif v >= 5:  score += 22
        elif v >= 3:  score += 15
        elif v >= 2:  score += 10
        elif v >= 1:  score += 5
    except: pass
    try:
        f = float(flow_num)
        if   f >= 1_000_000: score += 20
        elif f >= 500_000:   score += 15
        elif f >= 200_000:   score += 10
        elif f >= 100_000:   score += 5
        elif f >= 50_000:    score += 2
    except: pass
    try:
        if ask_hit is not None:
            ah = float(ask_hit)
            if   ah >= 80: score += 20
            elif ah >= 70: score += 15
            elif ah >= 60: score += 10
            elif ah >= 50: score += 5
            elif ah <= 30: score -= 5
    except: pass
    try:
        d = int(whale_days)
        if   d >= 5: score += 15
        elif d >= 3: score += 10
        elif d >= 2: score += 6
        elif d >= 1: score += 2
    except: pass
    if sweep == "🌊": score += 10
    if has_earn: score -= 15
    return max(0, min(100, score))

def score_label(score: int) -> str:
    if score >= 75: return f"🔥 {score}"
    if score >= 50: return f"⚡ {score}"
    if score >= 30: return f"👀 {score}"
    return f"💤 {score}"

def get_voi_baseline(ticker, strike, expiration, contract_type, history=None) -> dict:
    if history is None:
        history = load_history()
    voi_values = []
    for e in history:
        if (str(e.get("ticker",""))==str(ticker) and str(e.get("strike",""))==str(strike) and
            str(e.get("expiration",""))==str(expiration) and str(e.get("type",""))==str(contract_type)):
            try:
                v = float(e.get("voi",0))
                if v > 0: voi_values.append(v)
            except: pass
    if len(voi_values) < 2:
        return {"mean": None, "count": len(voi_values), "anomaly_pct": None}
    mean_voi = sum(voi_values) / len(voi_values)
    return {"mean": round(mean_voi,2), "count": len(voi_values), "anomaly_pct": None}

def voi_anomaly_label(current_voi: float, baseline: dict) -> str:
    if baseline["mean"] is None or baseline["count"] < 2: return ""
    mean = baseline["mean"]
    if mean <= 0: return ""
    pct = round(((current_voi - mean) / mean) * 100, 0)
    count = baseline["count"]
    if pct >= 100: return f"🚀 +{int(pct)}% vs {mean:.1f} ({count}d)"
    elif pct >= 50: return f"📈 +{int(pct)}% vs {mean:.1f} ({count}d)"
    elif pct >= 0:  return f"➡️ +{int(pct)}% vs {mean:.1f} ({count}d)"
    else:           return f"📉 {int(pct)}% vs {mean:.1f} ({count}d)"

def compute_strike_sentiment(df_full: pd.DataFrame) -> pd.DataFrame:
    if df_full.empty: return pd.DataFrame()
    rows = []
    for strike in sorted(df_full["strike"].unique()):
        calls = df_full[(df_full["strike"]==strike) & (df_full["type"]=="CALL")]
        puts  = df_full[(df_full["strike"]==strike) & (df_full["type"]=="PUT")]
        call_vol  = int(calls["volume"].sum()) if not calls.empty else 0
        put_vol   = int(puts["volume"].sum())  if not puts.empty else 0
        call_flow = round(calls["FLOW_POWER_NUM"].sum(),0) if not calls.empty else 0
        put_flow  = round(puts["FLOW_POWER_NUM"].sum(),0)  if not puts.empty else 0
        if call_vol==0 and put_vol==0: continue
        if call_vol>0 and put_vol>0:   ratio = round(call_vol/put_vol,2)
        elif call_vol>0:               ratio = 99.0
        else:                          ratio = 0.01
        if   ratio >= 2.0: sentiment,color = "📈 BULLISH","bull"
        elif ratio <= 0.5: sentiment,color = "📉 BEARISH","bear"
        else:              sentiment,color = "⚖️ NEUTRO","neut"
        rows.append({"Strike":strike,"CALL vol":call_vol,"PUT vol":put_vol,
                     "C/P Ratio":ratio,"Sentiment":sentiment,
                     "Flow CALL":format_k(call_flow) if call_flow>0 else "—",
                     "Flow PUT":format_k(put_flow) if put_flow>0 else "—","_color":color})
    return pd.DataFrame(rows) if rows else pd.DataFrame()

# =========================
# WATCHLIST
# =========================

@st.cache_data(ttl=300)
def load_watchlist() -> list:
    sheet = get_sheet("watchlist")
    if sheet:
        try:
            rows = sheet.get_all_records()
            return rows
        except Exception:
            pass
    if os.path.exists("watchlist.json"):
        try:
            with open("watchlist.json", "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_watchlist(wl: list):
    sheet = get_sheet("watchlist")
    if sheet:
        try:
            sheet.clear()
            sheet.append_row(WATCHLIST_COLS)
            for e in wl:
                row = [str(e.get(c, "")) for c in WATCHLIST_COLS]
                sheet.append_row(row)
            load_watchlist.clear()
            return True
        except Exception:
            pass
    try:
        with open("watchlist.json", "w") as f:
            json.dump(wl, f)
    except Exception:
        pass
    return False

def add_to_watchlist(ticker: str, strike: float, expiration: str, contract_type: str, note: str = ""):
    wl = load_watchlist()
    key = (str(ticker), str(strike), str(expiration), str(contract_type))
    existing = {(str(e.get("ticker","")), str(e.get("strike","")),
                 str(e.get("expiration","")), str(e.get("type",""))) for e in wl}
    if key in existing:
        return False
    entry = {
        "ticker": str(ticker), "strike": str(strike),
        "expiration": str(expiration), "type": str(contract_type),
        "note": str(note), "added": datetime.today().strftime("%Y-%m-%d"),
    }
    sheet = get_sheet("watchlist")
    if sheet:
        try:
            existing_header = sheet.row_values(1)
            if not existing_header:
                sheet.append_row(WATCHLIST_COLS)
            sheet.append_row([str(entry.get(c,"")) for c in WATCHLIST_COLS])
            load_watchlist.clear()
            return True
        except Exception:
            pass
    try:
        wl.append(entry)
        with open("watchlist.json", "w") as f:
            json.dump(wl, f)
        return True
    except Exception:
        return False

def save_watchlist_snapshot(ticker, strike, expiration, contract_type, mid, voi, iv, volume, underlying):
    today_str = datetime.today().strftime("%Y-%m-%d")
    sheet = get_sheet("wl_history")
    if sheet:
        import time
        for attempt in range(3):
            try:
                all_vals = sheet.get_all_values()
                if not all_vals or all_vals[0] != WATCHLIST_HISTORY_COLS:
                    sheet.clear()
                    sheet.append_row(WATCHLIST_HISTORY_COLS)
                existing = sheet.get_all_records()
                for e in existing:
                    if (str(e.get("date",""))==today_str and str(e.get("ticker",""))==str(ticker) and
                        str(e.get("strike",""))==str(strike) and str(e.get("expiration",""))==str(expiration) and
                        str(e.get("type",""))==str(contract_type)):
                        return
                row = [today_str,str(ticker),str(strike),str(expiration),str(contract_type),
                       str(round(float(mid),2)) if mid else "",str(round(float(voi),2)) if voi else "",
                       str(round(float(iv),1)) if iv else "",str(int(volume)) if volume else "",
                       str(round(float(underlying),2)) if underlying else ""]
                sheet.append_row(row)
                return True
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    time.sleep(2**attempt); continue
                break
    try:
        hist = []
        if os.path.exists("wl_history.json"):
            with open("wl_history.json","r") as f: hist = json.load(f)
        hist.append({"date":today_str,"ticker":str(ticker),"strike":str(strike),
                     "expiration":str(expiration),"type":str(contract_type),
                     "mid":mid,"voi":voi,"iv":iv,"volume":volume,"underlying":underlying})
        with open("wl_history.json","w") as f: json.dump(hist,f)
    except Exception: pass

def load_watchlist_history(ticker=None, strike=None, expiration=None, contract_type=None) -> list:
    sheet = get_sheet("wl_history")
    rows = []
    if sheet:
        try: rows = sheet.get_all_records()
        except Exception: pass
    if not rows and os.path.exists("wl_history.json"):
        try:
            with open("wl_history.json","r") as f: rows = json.load(f)
        except Exception: pass
    if ticker:
        rows = [r for r in rows if str(r.get("ticker",""))==str(ticker) and
                str(r.get("strike",""))==str(strike) and str(r.get("expiration",""))==str(expiration) and
                str(r.get("type",""))==str(contract_type)]
    return rows

# =========================
# HELPERS
# =========================

def format_k(x):
    if x >= 1_000_000: return f"{x/1_000_000:.1f}M"
    elif x >= 1_000:   return f"{x/1_000:.1f}K"
    return str(int(x))

def send_telegram_message(text: str) -> bool:
    if len(text) > 4000:
        text = text[:4000] + "\n\n... [troncato]"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        import re
        plain = re.sub(r"<[^>]+>", "", text)
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": plain}, timeout=5)
        return r.ok
    except Exception as e:
        st.warning(f"Telegram error: {e}")
        return False

def width_label(val, min_val, max_val, invert=False):
    if max_val == min_val: return ""
    pct = (val - min_val) / (max_val - min_val)
    if invert: pct = 1 - pct
    if pct >= 0.7:   return "🟢 WIDE"
    elif pct >= 0.4: return "🟡 MED"
    else:            return "🔴 NARROW"

def dte_label(dmin, dmax):
    span = dmax - dmin
    if span >= 150: return "🟢 WIDE"
    elif span >= 60: return "🟡 MED"
    else:            return "🔴 NARROW"

# =========================
# SEC EDGAR — INSIDER TRADING (Form 4)
# =========================

TICKER_TO_CIK = {
    "AAPL": "0000320193", "MSFT": "0000789019", "GOOGL": "0001652044",
    "GOOG": "0001652044", "AMZN": "0001018724", "META":  "0001326801",
    "NVDA": "0001045810", "TSLA": "0001318605", "SPY":   "0000884394",
    "QQQ":  "0001067839", "NFLX": "0001065280", "AMD":   "0000002488",
    "INTC": "0000050863", "CRM":  "0001108524", "ORCL":  "0001341439",
    "UBER": "0001543151", "LYFT": "0001759509", "BABA":  "0001577552",
    "JPM":  "0000019617", "BAC":  "0000070858", "WMT":   "0000104169",
    "DIS":  "0001001039", "V":    "0001403161", "MA":    "0001141391",
    "CMG":  "0001058090", "SMCI": "0001375365", "PLTR":  "0001321655",
    "RKLB": "0001819994",
}

@st.cache_data(ttl=3600, show_spinner=False)
def get_cik_for_ticker(ticker: str) -> str | None:
    if ticker.upper() in TICKER_TO_CIK:
        return TICKER_TO_CIK[ticker.upper()]
    try:
        url2 = f"https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK={ticker}&type=4&dateb=&owner=include&count=1&search_text=&action=getcompany&output=atom"
        r2 = requests.get(url2, timeout=8,
                          headers={"User-Agent": "Options Flow Scanner Pro info@optionsflowpro.com"})
        if r2.status_code == 200:
            import re
            m = re.search(r'CIK=(\d+)', r2.text)
            if m:
                return m.group(1).zfill(10)
    except Exception:
        pass
    return None

@st.cache_data(ttl=3600, show_spinner=False)
def get_insider_transactions(ticker: str, days_back: int = 90) -> list[dict]:
    cik = get_cik_for_ticker(ticker)
    if not cik:
        return []

    cik_clean = str(cik).lstrip("0")
    cutoff = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    SEC_HEADERS = {"User-Agent": "Options Flow Scanner Pro info@optionsflowpro.com"}

    try:
        url = f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json"
        r = requests.get(url, timeout=10, headers=SEC_HEADERS)
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception:
        return []

    recent       = data.get("filings", {}).get("recent", {})
    forms        = recent.get("form", [])
    dates        = recent.get("filingDate", [])
    accessions   = recent.get("accessionNumber", [])
    descriptions = recent.get("primaryDocument", [])

    form4_filings = []
    for i, f in enumerate(forms):
        if f in ("4", "4/A") and dates[i] >= cutoff:
            form4_filings.append({
                "date":      dates[i],
                "accession": accessions[i].replace("-", ""),
                "doc":       descriptions[i] if i < len(descriptions) else "",
            })

    if not form4_filings:
        return []

    transactions = []
    for filing in form4_filings[:20]:
        try:
            acc     = filing["accession"]
            # Strategia: cerca il file XML puro tramite filing index
            acc_dashed = f"{acc[:10]}-{acc[10:12]}-{acc[12:]}"
            idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc}/{acc_dashed}-index.json"
            xml_doc = None
            try:
                ri = requests.get(idx_url, timeout=8, headers=SEC_HEADERS)
                if ri.status_code == 200:
                    idx_data = ri.json()
                    for item in idx_data.get("documents", []):
                        fname = item.get("filename","")
                        ftype = item.get("type","")
                        if ftype == "4" and fname.endswith(".xml"):
                            xml_doc = fname
                            break
                        if fname.endswith(".xml") and "form4" in fname.lower():
                            xml_doc = fname
            except Exception:
                pass
            # Fallback: prova form4.xml diretto
            if not xml_doc:
                xml_doc = "form4.xml"
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc}/{xml_doc}"
            rx = requests.get(xml_url, timeout=8, headers=SEC_HEADERS)
            if rx.status_code != 200:
                continue
            # Verifica che sia XML e non HTML
            if rx.text.strip().startswith("<!DOCTYPE html") or "<html" in rx.text[:200].lower():
                continue

            import xml.etree.ElementTree as ET
            root = ET.fromstring(rx.text)

            rp   = root.find(".//reportingOwner")
            name = ""
            role = ""
            if rp is not None:
                name_el = rp.find(".//rptOwnerName")
                if name_el is not None: name = name_el.text or ""
                role_el = rp.find(".//officerTitle")
                if role_el is not None: role = role_el.text or ""
                if not role:
                    is_dir   = rp.find(".//isDirector")
                    is_off   = rp.find(".//isOfficer")
                    is_10pct = rp.find(".//isTenPercentOwner")
                    if is_dir   is not None and is_dir.text   == "1": role = "Director"
                    elif is_off is not None and is_off.text   == "1": role = "Officer"
                    elif is_10pct is not None and is_10pct.text == "1": role = "10% Owner"

            for txn in root.findall(".//nonDerivativeTransaction"):
                try:
                    date_el   = txn.find(".//transactionDate/value")
                    code_el   = txn.find(".//transactionCode")
                    shares_el = txn.find(".//transactionShares/value")
                    price_el  = txn.find(".//transactionPricePerShare/value")
                    owned_el  = txn.find(".//sharesOwnedFollowingTransaction/value")

                    txn_date  = date_el.text  if date_el  is not None else filing["date"]
                    txn_code  = code_el.text  if code_el  is not None else "?"
                    shares    = float(shares_el.text) if shares_el is not None and shares_el.text else 0
                    price     = float(price_el.text)  if price_el  is not None and price_el.text  else 0
                    owned     = float(owned_el.text)  if owned_el  is not None and owned_el.text  else 0
                    value     = round(shares * price, 0)

                    TYPE_MAP = {
                        "P": "🟢 Acquisto",   "S": "🔴 Vendita",
                        "A": "🎁 Award",      "F": "🧾 Tax withhold",
                        "M": "⚙️ Exercise",   "G": "🎁 Gift",
                        "D": "📤 Disposition","I": "📥 Indirect",
                        "J": "⚖️ Other",
                    }
                    txn_type = TYPE_MAP.get(txn_code, f"❓ {txn_code}")

                    if shares > 0:
                        transactions.append({
                            "Data":     txn_date,
                            "Insider":  name.title(),
                            "Ruolo":    role,
                            "Tipo":     txn_type,
                            "Azioni":   int(shares),
                            "Prezzo":   round(price, 2),
                            "Valore $": int(value),
                            "Owned":    int(owned),
                            "_code":    txn_code,
                        })
                except Exception:
                    continue
        except Exception:
            continue

    transactions.sort(key=lambda x: x["Data"], reverse=True)
    return transactions


def render_insider_section():
    st.subheader("🕵️ Insider Trading — Form 4 SEC")
    st.caption("Fonte: SEC EDGAR (dati pubblici) · Aggiornamento: entro 2 gg lavorativi dalla transazione")

    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        ins_ticker = st.text_input("Ticker", "GOOGL", key="ins_ticker").upper().strip()
    with col_b:
        ins_days = st.selectbox("Periodo", [30, 60, 90, 180], index=1, key="ins_days")
    with col_c:
        ins_type = st.radio("Tipo", ["Tutti", "Solo acquisti", "Solo vendite"],
                            key="ins_type", horizontal=False)

    debug_mode = st.checkbox("🔧 Mostra diagnostica", value=False, key="ins_debug")

    if st.button("🔍 Carica Insider Data", key="ins_search", type="primary"):

        # ── DIAGNOSTICA OPZIONALE ──
        if debug_mode:
            st.markdown("---")
            st.markdown("**🔧 Diagnostica SEC EDGAR:**")
            cik_dbg = get_cik_for_ticker(ins_ticker)
            st.write(f"1️⃣ CIK risolto: `{cik_dbg}`")
            if not cik_dbg:
                st.error("❌ CIK non trovato — ticker non in dizionario e lookup fallito")
            else:
                import requests as _req
                SEC_H = {"User-Agent": "Options Flow Scanner Pro info@optionsflowpro.com"}
                sub_url = f"https://data.sec.gov/submissions/CIK{str(cik_dbg).zfill(10)}.json"
                st.write(f"2️⃣ URL: `{sub_url}`")
                try:
                    rs = _req.get(sub_url, timeout=10, headers=SEC_H)
                    st.write(f"   Status submissions: `{rs.status_code}`")
                    if rs.status_code == 200:
                        recent = rs.json().get("filings", {}).get("recent", {})
                        forms  = recent.get("form", [])
                        dates  = recent.get("filingDate", [])
                        accs   = recent.get("accessionNumber", [])
                        docs   = recent.get("primaryDocument", [])
                        cutoff_dbg = (datetime.today() - timedelta(days=ins_days)).strftime("%Y-%m-%d")
                        form4_dbg  = [(forms[i], dates[i], accs[i], docs[i])
                                      for i in range(len(forms))
                                      if forms[i] in ("4","4/A") and dates[i] >= cutoff_dbg]
                        st.write(f"   Form 4 nel periodo: `{len(form4_dbg)}`")
                        if form4_dbg:
                            f0 = form4_dbg[0]
                            acc0  = f0[2].replace("-","")
                            cik_c = str(cik_dbg).lstrip("0")
                            # Leggi l'index del filing per trovare il vero XML
                            idx_url = f"https://data.sec.gov/submissions/CIK{str(cik_dbg).zfill(10)}.json"
                            idx2_url = f"https://www.sec.gov/Archives/edgar/data/{cik_c}/{acc0}/{acc0[:10]}-{acc0[10:12]}-{acc0[12:]}-index.htm"
                            st.write(f"3️⃣ Index URL: `{idx2_url}`")
                            ri = _req.get(idx2_url, timeout=8, headers=SEC_H)
                            st.write(f"   Status index: `{ri.status_code}`")
                            if ri.status_code == 200:
                                import re as _re
                                # Trova tutti i file .xml nell'index
                                xml_files = _re.findall(r'href="([^"]+\.xml)"', ri.text, _re.IGNORECASE)
                                st.write(f"   File XML trovati nell'index: `{xml_files}`")
                            # Prova direttamente form4.xml senza sottocartella
                            xml_url2 = f"https://www.sec.gov/Archives/edgar/data/{cik_c}/{acc0}/form4.xml"
                            st.write(f"   Provo: `{xml_url2}`")
                            rx2 = _req.get(xml_url2, timeout=8, headers=SEC_H)
                            st.write(f"   Status: `{rx2.status_code}`")
                            if rx2.status_code == 200:
                                st.code(rx2.text[:400], language="xml")
                    else:
                        st.error(f"❌ Submissions status {rs.status_code}: {rs.text[:200]}")
                except Exception as ex_dbg:
                    st.error(f"❌ Eccezione: {ex_dbg}")
            st.markdown("---")

        with st.spinner(f"📡 Scaricando Form 4 per {ins_ticker}..."):
            txns = get_insider_transactions(ins_ticker, days_back=ins_days)

        if not txns:
            st.warning(f"⚠️ Nessun Form 4 trovato per **{ins_ticker}** negli ultimi {ins_days} giorni.")
            st.info("💡 Attiva la checkbox 🔧 Mostra diagnostica per vedere dove si blocca.")
            return

        df_ins = pd.DataFrame(txns)

        if ins_type == "Solo acquisti":
            df_ins = df_ins[df_ins["_code"] == "P"]
        elif ins_type == "Solo vendite":
            df_ins = df_ins[df_ins["_code"] == "S"]

        df_ins = df_ins.drop(columns=["_code"])

        if df_ins.empty:
            st.warning("Nessuna transazione del tipo selezionato nel periodo.")
            return

        buys     = df_ins[df_ins["Tipo"].str.contains("Acquisto")]
        sells    = df_ins[df_ins["Tipo"].str.contains("Vendita")]
        buy_val  = buys["Valore $"].sum()
        sell_val = sells["Valore $"].sum()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("📋 Transazioni", len(df_ins))
        k2.metric("🟢 Acquisti", f"{len(buys)} — ${buy_val/1_000:.0f}K" if buy_val < 1_000_000
                  else f"{len(buys)} — ${buy_val/1_000_000:.1f}M")
        k3.metric("🔴 Vendite", f"{len(sells)} — ${sell_val/1_000:.0f}K" if sell_val < 1_000_000
                  else f"{len(sells)} — ${sell_val/1_000_000:.1f}M")
        net = buy_val - sell_val
        net_str = (f"+${net/1_000_000:.1f}M"       if net >= 1_000_000
                   else f"+${net/1_000:.0f}K"       if net >= 0
                   else f"-${abs(net)/1_000_000:.1f}M" if abs(net) >= 1_000_000
                   else f"-${abs(net)/1_000:.0f}K")
        k4.metric("⚖️ Net Flow", net_str,
                  delta="BULLISH 🟢" if net > 0 else "BEARISH 🔴")

        st.divider()

        def color_tipo(val):
            if "Acquisto" in str(val):
                return "background-color:#1a3a1a; color:#00ff88; font-weight:bold"
            if "Vendita" in str(val):
                return "background-color:#3a0a0a; color:#ff4444; font-weight:bold"
            return "color:#aaaaaa"

        styled = (
            df_ins.style
            .map(color_tipo, subset=["Tipo"])
            .format({
                "Azioni":   "{:,}",
                "Prezzo":   "${:.2f}",
                "Valore $": "${:,}",
                "Owned":    "{:,}",
            })
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

        big_buys = buys[buys["Valore $"] >= 500_000]
        if not big_buys.empty:
            st.warning(
                f"🚨 **INSIDER BUY ALERT** — {len(big_buys)} acquisto/i superiori a $500K rilevati su {ins_ticker}!"
            )
            for _, row in big_buys.iterrows():
                st.markdown(
                    f"📌 **{row['Insider']}** ({row['Ruolo']}) — "
                    f"acquistato **{row['Azioni']:,} azioni** @ ${row['Prezzo']:.2f} "
                    f"= **${row['Valore $']:,}** il {row['Data']}"
                )

        with st.expander("ℹ️ Come leggere i dati insider", expanded=False):
            st.markdown("""
**Codici transazione:**
- 🟢 **Acquisto (P)** — acquisto diretto sul mercato aperto → segnale bullish forte
- 🔴 **Vendita (S)** — vendita sul mercato aperto → attenzione (ma può essere diversificazione)
- 🎁 **Award (A)** — azioni assegnate come compensazione → non direzionale
- ⚙️ **Exercise (M)** — esercizio stock option → spesso seguito da vendita
- 🧾 **Tax withhold (F)** — trattenuta fiscale → non direzionale

**Regola pratica:**
Un CEO/CFO che **compra sul mercato aperto** con denaro proprio è il segnale più forte.
Le vendite vanno sempre contestualizzate: molte sono pianificate con piani 10b5-1.

**Fonte:** SEC EDGAR Form 4 — deposito obbligatorio entro 2 giorni lavorativi dalla transazione.
""")

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

@st.cache_data(ttl=3600, show_spinner=False)
def get_short_interest(ticker: str) -> dict:
    result = {"short_pct": None, "days_to_cover": None, "short_shares": None}
    try:
        url = f"https://api.polygon.io/v2/reference/financials/{ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY, "limit": 1}, timeout=8)
        if r.status_code == 200:
            data = r.json().get("results", [])
            if data:
                fi = data[0]
                short_shares = fi.get("short_interest")
                float_shares = fi.get("float_shares") or fi.get("shares_outstanding")
                avg_volume   = fi.get("average_daily_volume")
                if short_shares and float_shares and float_shares > 0:
                    result["short_pct"]    = round((short_shares / float_shares) * 100, 1)
                    result["short_shares"] = int(short_shares)
                if short_shares and avg_volume and avg_volume > 0:
                    result["days_to_cover"] = round(short_shares / avg_volume, 1)
                return result
    except Exception:
        pass
    try:
        url2 = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        r2 = requests.get(url2, params={"apiKey": POLYGON_API_KEY}, timeout=8)
        if r2.status_code == 200:
            data2 = r2.json().get("ticker", {})
            si = data2.get("shortInterest") or data2.get("short_interest")
            if si:
                result["short_shares"] = int(si)
    except Exception:
        pass
    return result

@st.cache_data(ttl=300, show_spinner=False)
def get_dark_pool_pct(ticker: str) -> float | None:
    try:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        r = requests.get(url, params={"apiKey": POLYGON_API_KEY}, timeout=8)
        if r.status_code == 200:
            data = r.json().get("ticker", {})
            day       = data.get("day", {})
            total_vol = day.get("v") or 0
            otc_vol   = day.get("otcVolume") or 0
            dark_vol  = day.get("darkVolume") or otc_vol or 0
            if total_vol > 0 and dark_vol > 0:
                return round((dark_vol / total_vol) * 100, 1)
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
        contract_type = details.get("contract_type", "").upper()
        strike        = details.get("strike_price")
        exp_str       = details.get("expiration_date", "")
        ticker_sym    = details.get("ticker", "")
        if not strike or not exp_str: continue
        try:    exp_dt = pd.to_datetime(exp_str)
        except: continue
        volume    = day.get("volume") or 0
        oi        = item.get("open_interest") or 0
        day_close = day.get("close") or 0
        day_vwap  = day.get("vwap")  or 0
        mid = day_close if day_close > 0 else (day_vwap if day_vwap > 0 else 0)
        if mid > 0:
            half_spread = max(0.01, round(mid * 0.015, 2))
            bid    = round(mid - half_spread, 2)
            ask    = round(mid + half_spread, 2)
            spread = round(ask - bid, 2)
        else:
            bid = ask = spread = None
        iv    = item.get("implied_volatility") or 0
        delta = greeks.get("delta")
        gamma = greeks.get("gamma")
        theta = greeks.get("theta")
        vega  = greeks.get("vega")
        rows.append({
            "ticker_sym": ticker_sym, "type": contract_type,
            "strike": strike, "expiration": exp_dt, "exp_str": exp_str,
            "volume": int(volume), "OI": int(oi),
            "MID": round(mid, 2), "bid": bid, "ask": ask, "SPREAD": spread,
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

    def calc_gex(row):
        if row["gamma"] is None or row["gamma"] == 0: return 0
        sign = 1 if row["type"] == "CALL" else -1
        return sign * row["gamma"] * row["OI"] * 100 * (underlying ** 2) / 1_000_000
    df["GEX_M"] = df.apply(calc_gex, axis=1).round(2)

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
    def check_earnings(row):
        has_earn, earn_date = earnings_in_dte(ticker, int(row["DTE"]))
        if has_earn: return f"⚠️ {earn_date}"
        return ""
    df["EARN"] = df.apply(check_earnings, axis=1)
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
    df["ASK_HIT"]  = None
    df["SWEEP"]    = ""
    df["🐋 DAYS"]  = 0
    df["SCORE"]    = ""
    df["VOI_ANOM"] = ""
    cached_history = load_history()
    idx_list = df.head(top_n).index.tolist()
    progress = st.progress(0, text="📡 Analisi flusso in corso...")
    for i, idx in enumerate(idx_list):
        row = df.loc[idx]
        ask_hit_pct, is_sweep = get_ask_hit_real(
            row.get("ticker_sym",""), row.get("bid",0), row.get("ask",0)
        )
        sweep_str = "🌊" if is_sweep else ""
        df.at[idx, "ASK_HIT"] = ask_hit_pct
        df.at[idx, "SWEEP"]   = sweep_str
        add_to_history(ticker, row.get("strike"), row.get("exp_str",""),
                       row.get("type",""), row.get("FLOW_POWER_NUM",0),
                       row.get("VOI",0), ask_hit_pct, sweep_str)
        whale_days = get_cluster_repeat(
            ticker, row.get("strike"), row.get("exp_str",""), row.get("type",""),
            history=cached_history
        )
        df.at[idx, "🐋 DAYS"] = whale_days
        baseline = get_voi_baseline(
            ticker, row.get("strike"), row.get("exp_str",""), row.get("type",""),
            history=cached_history
        )
        try:    current_voi = float(row.get("VOI",0))
        except: current_voi = 0
        df.at[idx, "VOI_ANOM"] = voi_anomaly_label(current_voi, baseline)
        has_earn_flag = bool(row.get("EARN",""))
        score = compute_score(
            voi=current_voi, ask_hit=ask_hit_pct, sweep=sweep_str,
            whale_days=whale_days, flow_num=row.get("FLOW_POWER_NUM",0),
            has_earn=has_earn_flag, iv=row.get("IV")
        )
        df.at[idx, "SCORE"] = score_label(score)
        progress.progress((i+1)/len(idx_list), text=f"📡 Flow analysis: {i+1}/{len(idx_list)}")
    progress.empty()
    flush_history_buffer()
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
    has_earn, earn_date = earnings_in_dte(ticker, dte_max)
    if has_earn:
        days_to = (datetime.strptime(earn_date, "%Y-%m-%d").date() - datetime.today().date()).days
        st.warning(f"⚠️ **EARNINGS ALERT** — {ticker} ha earnings il **{earn_date}** ({days_to} giorni).")
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
    dp_pct = get_dark_pool_pct(ticker)
    dp_str = ""
    if dp_pct is not None:
        if dp_pct >= 50:   dp_str = f"  |  🔵🔵 Dark Pool: **{dp_pct}%** (accumulo forte)"
        elif dp_pct >= 30: dp_str = f"  |  🔵 Dark Pool: **{dp_pct}%** (accumulo istituzionale)"
        else:              dp_str = f"  |  Dark Pool: {dp_pct}%"
    st.caption(f"Put/Call Ratio: **{pc}** | CALL vol: {cv:,} | PUT vol: {pv:,} | Bias: {bias_label}{dp_str}")
    si_data = get_short_interest(ticker)
    si_parts = []
    if si_data.get("short_pct") is not None:
        sp = si_data["short_pct"]
        if   sp >= 40: si_emoji = "🔴🔴"
        elif sp >= 20: si_emoji = "🔴"
        elif sp >= 10: si_emoji = "🟡"
        else:          si_emoji = ""
        si_parts.append(f"{si_emoji} Short Interest: **{sp}%**")
    if si_data.get("days_to_cover") is not None:
        dtc = si_data["days_to_cover"]
        if   dtc >= 10: dtc_emoji = "🔴🔴"
        elif dtc >= 5:  dtc_emoji = "🔴"
        else:           dtc_emoji = ""
        si_parts.append(f"{dtc_emoji} Days to Cover: **{dtc}gg**")
    if si_parts:
        si_str = "  |  ".join(si_parts)
        squeeze = (si_data.get("short_pct") or 0) >= 20 and (si_data.get("days_to_cover") or 0) >= 5
        if squeeze:
            st.warning(f"🚀 **SQUEEZE FUEL** — {ticker}: {si_str} — Posizione short elevata con copertura lenta!")
        else:
            st.caption(f"📊 {si_str}")
    df_enriched = enrich_with_flow_data(df, ticker, top_n=15)
    if not df_enriched.empty:
        sentiment_df = compute_strike_sentiment(df)
        if not sentiment_df.empty:
            with st.expander(f"📊 Sentiment per Strike — {ticker}", expanded=False):
                def hl_sent(val):
                    if "BULLISH" in str(val): return "background-color:#1a3a1a; color:#00ff88"
                    if "BEARISH" in str(val): return "background-color:#3a0a0a; color:#ff4444"
                    return "background-color:#2a2a0a; color:#ffdd00"
                display_sent = sentiment_df.drop(columns=["_color"]).copy()
                st.dataframe(
                    display_sent.style.map(hl_sent, subset=["Sentiment"])
                    .format({"Strike": lambda x: str(int(x)) if x==int(x) else f"{x:.2f}",
                             "C/P Ratio": lambda x: f"{x:.2f}"}),
                    use_container_width=True, hide_index=True
                )
                bulls = sentiment_df[sentiment_df["_color"]=="bull"].nlargest(3,"C/P Ratio")
                bears = sentiment_df[sentiment_df["_color"]=="bear"].nsmallest(3,"C/P Ratio")
                col_b, col_s = st.columns(2)
                with col_b:
                    if not bulls.empty:
                        st.markdown("**🎯 Strike più BULLISH:**")
                        for _, r in bulls.iterrows():
                            st.markdown(f"Strike **{int(r['Strike']) if r['Strike']==int(r['Strike']) else r['Strike']}** — C/P {r['C/P Ratio']:.2f} | CALL {r['CALL vol']:,} vs PUT {r['PUT vol']:,}")
                with col_s:
                    if not bears.empty:
                        st.markdown("**🎯 Strike più BEARISH:**")
                        for _, r in bears.iterrows():
                            st.markdown(f"Strike **{int(r['Strike']) if r['Strike']==int(r['Strike']) else r['Strike']}** — C/P {r['C/P Ratio']:.2f} | CALL {r['CALL vol']:,} vs PUT {r['PUT vol']:,}")
    return df_enriched

# =========================
# SIDEBAR — TUTTI I FILTRI
# =========================
PRESETS = {
    "SMALL CAP": {
        "volume_min": 100, "voi_min": 1.5, "dte_max": 245, "dte_min": 45,
        "strike_dist_min": 0, "strike_dist_max": 20, "spread_max": 20.0,
        "delta_min": 0.05, "delta_max": 0.95, "ask_hit_min": 0.0, "flow_min": 0,
        "desc": "Small Cap (<$2B) — bassa liquidità, filtri adattati"
    },
    "MID CAP": {
        "volume_min": 200, "voi_min": 1.2, "dte_max": 245, "dte_min": 45,
        "strike_dist_min": 0, "strike_dist_max": 15, "spread_max": 20.0,
        "delta_min": 0.10, "delta_max": 0.90, "ask_hit_min": 0.0, "flow_min": 0,
        "desc": "Mid Cap ($2B-$10B) — bilanciato tra liquidità e segnale"
    },
    "BIG CAP": {
        "volume_min": 500, "voi_min": 1.0, "dte_max": 245, "dte_min": 45,
        "strike_dist_min": 0, "strike_dist_max": 12, "spread_max": 20.0,
        "delta_min": 0.10, "delta_max": 0.90, "ask_hit_min": 0.0, "flow_min": 0,
        "desc": "Big Cap (>$10B) — alta liquidità, filtri standard"
    },
    "SNIPER": {
        "volume_min": 1000, "voi_min": 2.0, "dte_max": 245, "dte_min": 45,
        "strike_dist_min": 0, "strike_dist_max": 5, "spread_max": 20.0,
        "delta_min": 0.20, "delta_max": 0.80, "ask_hit_min": 55.0, "flow_min": 0,
        "desc": "SNIPER — strike vicino, scadenza breve, alta pressione"
    },
    "HOT ONLY": {
        "volume_min": 2000, "voi_min": 3.0, "dte_max": 245, "dte_min": 45,
        "strike_dist_min": 0, "strike_dist_max": 3, "spread_max": 20.0,
        "delta_min": 0.25, "delta_max": 0.75, "ask_hit_min": 60.0, "flow_min": 0,
        "desc": "HOT ONLY — solo flussi anomali estremi, scadenza imminente"
    },
    "SPY SWING": {
        "volume_min": 100, "voi_min": 0.5, "dte_max": 245, "dte_min": 45,
        "strike_dist_min": 0, "strike_dist_max": 15, "spread_max": 20.0,
        "delta_min": 0.05, "delta_max": 0.80, "ask_hit_min": 0.0, "flow_min": 50000,
        "desc": "SPY SWING — DTE 45-245gg | Flow >$50K | Allarga i filtri, poi stringi manualmente"
    },
}

APP_VERSION = "7.0"

with st.sidebar:
    st.markdown("## 🔥 Options Flow Scanner")

    mode = st.radio(
        "**Modalità**",
        ["SMALL CAP", "MID CAP", "BIG CAP", "SNIPER", "HOT ONLY", "SPY SWING"],
        key="mode_radio"
    )
    preset = PRESETS[mode]
    st.caption(f"ℹ️ {preset['desc']}")

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

    _c1, _c2 = st.columns(2)
    with _c1:
        vol_lbl = width_label(st.session_state["volume_min"], 0, 50000, invert=True)
        volume_min = st.slider(f"Vol min {vol_lbl}", 0, 50000,
                               st.session_state["volume_min"], key="volume_min")
    with _c2:
        voi_lbl = width_label(st.session_state["voi_min"], 0.0, 20.0, invert=True)
        voi_min = st.slider(f"VOI min {voi_lbl}", 0.0, 20.0,
                            st.session_state["voi_min"], step=0.1, key="voi_min")

    dte_lbl = dte_label(st.session_state["dte_min"], st.session_state["dte_max"])
    st.caption(f"📅 DTE {dte_lbl}")
    _c3, _c4 = st.columns(2)
    with _c3:
        dte_min = st.slider("DTE min", 0, 365, st.session_state["dte_min"], key="dte_min")
    with _c4:
        dte_max = st.slider("DTE max", 1, 365, st.session_state["dte_max"], key="dte_max")

    strike_span = st.session_state["strike_dist_max"] - st.session_state["strike_dist_min"]
    sk_lbl = "🟢 WIDE" if strike_span >= 15 else ("🟡 MED" if strike_span >= 7 else "🔴 NARROW")
    st.caption(f"🎯 Strike % {sk_lbl}")
    _c5, _c6 = st.columns(2)
    with _c5:
        strike_dist_min = st.slider("Strike % min", 0, 30, st.session_state["strike_dist_min"],
                                    key="strike_dist_min",
                                    help="Esclude ITM/ATM. Es: 5 = solo OTM oltre 5%")
    with _c6:
        strike_dist_max = st.slider("Strike % max", 1, 50, st.session_state["strike_dist_max"],
                                    key="strike_dist_max")

    delta_span = st.session_state["delta_max"] - st.session_state["delta_min"]
    dl_lbl = "🟢 WIDE" if delta_span >= 0.6 else ("🟡 MED" if delta_span >= 0.3 else "🔴 NARROW")
    st.caption(f"Δ Delta {dl_lbl}  (0.05=OTM · 0.50=ATM · 0.90=ITM)")
    _c7, _c8 = st.columns(2)
    with _c7:
        delta_min = st.slider("Δ min", 0.0, 1.0,
                              st.session_state["delta_min"], step=0.01, key="delta_min")
    with _c8:
        delta_max = st.slider("Δ max", 0.0, 1.0,
                              st.session_state["delta_max"], step=0.01, key="delta_max")

    _c9, _c10 = st.columns(2)
    with _c9:
        spread_lbl = width_label(st.session_state["spread_max"], 0.01, 20.0)
        spread_max = st.slider(f"Spread {spread_lbl}", 0.01, 20.0,
                               st.session_state["spread_max"], step=0.01, key="spread_max")
    with _c10:
        flow_lbl = width_label(st.session_state["flow_min"], 0, 5000000, invert=True)
        flow_min = st.slider(f"Flow $ {flow_lbl}", 0, 5000000,
                             st.session_state["flow_min"], step=50000, key="flow_min",
                             help=">$500K = istituzionale. >$1M = whale.")

    ask_hit_lbl = width_label(st.session_state["ask_hit_min"], 0.0, 100.0, invert=True)
    ask_hit_min = st.slider(
        f"Ask Hit % min {ask_hit_lbl}",
        0.0, 100.0, st.session_state["ask_hit_min"], step=5.0, key="ask_hit_min",
        help="≥55% = buyer aggressivo. ≤30% = seller."
    )

    _c11, _c12 = st.columns([2, 1])
    with _c11:
        option_type = st.radio("Tipo", ["CALL", "PUT", "BOTH"], horizontal=True)
    with _c12:
        send_telegram = st.checkbox("📲 TG", value=False, help="Attiva Telegram Alerts")

    # ── EARNINGS MANUALI ──
    with st.expander("📅 Earnings (opzionale)", expanded=False):
        st.caption("Inserisci le date earnings per evitare falsi alert. Formato: YYYY-MM-DD")
        if "earnings_dates" not in st.session_state:
            st.session_state["earnings_dates"] = {}
        earn_ticker = st.text_input("Ticker", key="earn_ticker_input", placeholder="es. MRVL").upper().strip()
        earn_date   = st.text_input("Data earnings", key="earn_date_input", placeholder="es. 2026-05-29")
        col_ea, col_eb = st.columns(2)
        with col_ea:
            if st.button("➕ Aggiungi", key="earn_add"):
                if earn_ticker and earn_date:
                    try:
                        datetime.strptime(earn_date, "%Y-%m-%d")
                        st.session_state["earnings_dates"][earn_ticker] = earn_date
                        st.success(f"✅ {earn_ticker}: {earn_date}")
                    except ValueError:
                        st.error("❌ Formato data non valido")
        with col_eb:
            if st.button("🗑️ Pulisci tutti", key="earn_clear"):
                st.session_state["earnings_dates"] = {}
                st.success("✅ Rimossi")
        if st.session_state.get("earnings_dates"):
            for t, d in st.session_state["earnings_dates"].items():
                st.caption(f"📌 {t} → {d}")

    tickers_input = st.text_input("🔍 Ticker (virgola)", "SPY")
    scan_clicked = st.button("🚀 SCANSIONA", type="primary", use_container_width=True)

# =========================
# MAIN AREA — HEADER
# =========================
st.title("🔥 Options Flow Scanner PRO by Ugo Fortezze 🔥")
st.caption("Powered by Polygon.io — Greeks | Ask Hit | Sweep | Storico Cluster | Insider Trading  •  v7.0")

# =========================
# TAB PRINCIPALE
# =========================
tab_scanner, tab_insider = st.tabs(["📡 Options Flow Scanner", "🕵️ Insider Trading"])

with tab_scanner:

    # ── RIEPILOGO FILTRI ATTIVI ──
    def _fk(v):
        if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
        if v >= 1_000:     return f"{v/1_000:.0f}K"
        return str(int(v))

    _vol   = st.session_state.get("volume_min", 0)
    _voi   = st.session_state.get("voi_min", 0.0)
    _dmin  = st.session_state.get("dte_min", 0)
    _dmax  = st.session_state.get("dte_max", 365)
    _skmin = st.session_state.get("strike_dist_min", 0)
    _skmax = st.session_state.get("strike_dist_max", 50)
    _dmin2 = st.session_state.get("delta_min", 0.0)
    _dmax2 = st.session_state.get("delta_max", 1.0)
    _sprd  = st.session_state.get("spread_max", 20.0)
    _flow  = st.session_state.get("flow_min", 0)
    _ask   = st.session_state.get("ask_hit_min", 0.0)
    _mode  = st.session_state.get("mode_radio", "—")

    st.info(
        f"**{_mode}** · "
        f"Vol≥{_vol:,} · VOI≥{_voi:.1f} · "
        f"DTE {_dmin}–{_dmax} · "
        f"Strike {_skmin}–{_skmax}% · "
        f"Δ {_dmin2:.2f}–{_dmax2:.2f} · "
        f"Spread≤{_sprd:.2f} · "
        f"Flow≥{_fk(_flow)} · "
        f"AskHit≥{int(_ask)}%"
    )

    _gs_client = get_gsheet_client()
    if _gs_client:
        st.caption("📊 Google Sheets: ✅ connesso")
    else:
        st.caption("📊 Google Sheets: ⚠️ non connesso — storico salvato localmente")

    # =========================
    # LEGENDA / MANUALE IN-APP
    # =========================
    with st.expander("📖 Manuale — Options Flow Scanner PRO v6.5"):
        st.markdown("""
## 🎯 Obiettivo del Tool
Scanner di flussi istituzionali sulle opzioni USA. Identifica contratti con volumi anomali rispetto all'open interest, con focus su **smart money** e **accumulo balena**. Nessuna esecuzione automatica — il controllo finale è sempre tuo.

## 📊 Colonne della Griglia
| Colonna | Come leggerla |
|---|---|
| **SCORE** | 🔥≥75 · ⚡≥50 · 👀≥30 · 💤<30 |
| **SIG** | 🟢 GO = VOI≥5 · 🟡 HOLD = VOI≥2 · 🔴 STOP = VOI<2 |
| **FLOW $** | >$500K = istituzionale · >$1M = whale |
| **SWEEP** | 🌊 = ordine su ≥2 exchange simultaneamente |
| **ASK_HIT %** | ≥70% 🟢 buyer aggressivo · ≤30% 🔴 vendita |
| **🐋 DAYS** | Giorni distinti nello storico — ≥2 = accumulo |
| **VOI** | Volume / OI — >1 = soldi freschi |
| **DTE** | Giorni alla scadenza |
| **GEX_M** | Gamma Exposure in $M · 🟢 positivo = dealer frena · 🔴 negativo = dealer amplifica |
| **Dark Pool %** | % volume fuori mercato · >30% 🔵 accumulo istituzionale · >50% 🔵🔵 segnale forte |
| **Short Interest %** | % float venduto allo scoperto · >20% 🔴 alto · >40% 🔴🔴 squeeze fuel |
| **Days to Cover** | Giorni per coprire lo short · >5 🔴 pericoloso · >10 🔴🔴 estremo |
| **Delta** | 0.05–0.25=OTM speculativo · 0.40–0.60=ATM · 0.70–0.95=ITM |

## ⚠️ Note Operative
- Combinazione più forte: **VOI alto + ASK_HIT ≥55% + SWEEP 🌊 + 🐋 DAYS ≥2**
- Paper trading consigliato per le prime settimane.
- Nessun ordine viene eseguito automaticamente.
""")

    # =========================
    # STORICO
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
            def clean_strike(v):
                try:
                    f = float(v)
                    return str(int(f)) if f == int(f) else f"{f:.2f}"
                except: return str(v)
            def clean_voi(v):
                try: return f"{float(v):.2f}"
                except: return str(v)
            df_agg["strike"]     = df_agg["strike"].apply(clean_strike)
            df_agg["ultimo_voi"] = df_agg["ultimo_voi"].apply(clean_voi)
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
                    sheet = get_sheet("history")
                    if sheet:
                        try:
                            sheet.clear()
                            sheet.append_row(HISTORY_COLS)
                            load_history.clear()
                        except Exception:
                            pass
                    if os.path.exists("scan_history.json"):
                        os.remove("scan_history.json")
                    st.success("Storico cancellato ✅")
                    st.rerun()

    # =========================
    # WATCHLIST
    # =========================
    with st.expander("⭐ Watchlist — Monitora contratti specifici"):
        wl = load_watchlist()
        st.markdown("**Aggiungi contratto da monitorare:**")
        wl_col1, wl_col2, wl_col3, wl_col4, wl_col5 = st.columns([2,1,2,1,2])
        with wl_col1:
            wl_ticker = st.text_input("Ticker", value="SPY", key="wl_ticker").upper()
        with wl_col2:
            wl_type = st.selectbox("Tipo", ["C", "P"], key="wl_type")
        with wl_col3:
            wl_strike = st.number_input("Strike", min_value=1.0, value=735.0, step=1.0, key="wl_strike")
        with wl_col4:
            wl_exp = st.text_input("Scadenza (YYYY-MM-DD)", value="2026-08-21", key="wl_exp")
        with wl_col5:
            wl_note = st.text_input("Nota (opzionale)", value="", key="wl_note")
        if st.button("➕ Aggiungi alla Watchlist", key="wl_add"):
            ok = add_to_watchlist(wl_ticker, wl_strike, wl_exp, wl_type, wl_note)
            if ok:
                st.success(f"✅ {wl_ticker} {wl_exp} {wl_strike}{wl_type} aggiunto!")
                wl = load_watchlist()
            else:
                st.warning("⚠️ Contratto già in watchlist.")
        if wl:
            st.markdown("---")
            st.markdown("**Contratti monitorati:**")
            if st.button("🔄 Aggiorna tutti i contratti in watchlist", key="wl_refresh"):
                wl_results = []
                prog = st.progress(0, text="📡 Aggiornamento watchlist...")
                for i, entry in enumerate(wl):
                    t      = entry.get("ticker","")
                    strike = float(entry.get("strike", 0))
                    exp    = entry.get("expiration","")
                    ctype  = entry.get("type","")
                    note   = entry.get("note","")
                    added  = entry.get("added","")
                    underlying = get_stock_price(t)
                    if underlying is None:
                        wl_results.append({"Ticker":t,"Contratto":f"{t} {exp} {strike}{ctype}",
                            "Nota":note,"MID":"—","VOI":"—","IV":"—","🐋 DAYS":"—","Aggiunto":added,"Underlying":"—"})
                        continue
                    try:
                        r = requests.get(
                            f"https://api.polygon.io/v3/snapshot/options/{t}",
                            params={"apiKey":POLYGON_API_KEY,"strike_price":strike,
                                    "expiration_date":exp,"contract_type":"call" if ctype=="C" else "put","limit":5},
                            timeout=10
                        )
                        results = r.json().get("results",[]) if r.status_code==200 else []
                        match = next((x for x in results
                                      if abs(x.get("details",{}).get("strike_price",0)-strike)<0.01
                                      and x.get("details",{}).get("expiration_date","")==exp), None)
                        if match:
                            day    = match.get("day",{})
                            mid    = day.get("close") or day.get("vwap") or 0
                            oi     = match.get("open_interest") or 0
                            vol    = day.get("volume") or 0
                            iv_raw = match.get("implied_volatility") or 0
                            iv_pct = round(iv_raw*100,1) if iv_raw else None
                            voi    = round(vol/oi,2) if oi>0 else 0
                            days_r = get_cluster_repeat(t, strike, exp, ctype)
                            save_watchlist_snapshot(t, strike, exp, ctype, mid, voi, iv_pct, vol, underlying)
                            wl_results.append({"Ticker":t,"Contratto":f"{t} {exp} {strike}{ctype}",
                                "Nota":note,"Underlying":f"${underlying}","MID":f"${mid:.2f}" if mid else "—",
                                "VOI":f"{voi:.2f}","IV":f"{iv_pct:.1f}%" if iv_pct else "—",
                                "🐋 DAYS":days_r,"Aggiunto":added})
                        else:
                            wl_results.append({"Ticker":t,"Contratto":f"{t} {exp} {strike}{ctype}",
                                "Nota":note,"MID":"n/d","VOI":"n/d","IV":"n/d","🐋 DAYS":"—",
                                "Aggiunto":added,"Underlying":f"${underlying}"})
                    except Exception:
                        wl_results.append({"Ticker":t,"Contratto":f"{t} {exp} {strike}{ctype}",
                            "Nota":note,"MID":"err","VOI":"err","IV":"err","🐋 DAYS":"—",
                            "Aggiunto":added,"Underlying":"err"})
                    prog.progress((i+1)/len(wl), text=f"📡 {i+1}/{len(wl)} contratti aggiornati")
                prog.empty()
                if wl_results:
                    st.dataframe(pd.DataFrame(wl_results), use_container_width=True, hide_index=True)
            else:
                wl_display = [{"Contratto":f"{e.get('ticker','')} {e.get('expiration','')} {e.get('strike','')}{e.get('type','')}",
                    "Nota":e.get("note",""),"Aggiunto":e.get("added",""),
                    "🐋 DAYS":get_cluster_repeat(e.get("ticker",""),e.get("strike",""),e.get("expiration",""),e.get("type",""))}
                    for e in wl]
                st.dataframe(pd.DataFrame(wl_display), use_container_width=True, hide_index=True)
            st.markdown("---")
            wl_labels = [f"{e.get('ticker','')} {e.get('expiration','')} {e.get('strike','')}{e.get('type','')}" for e in wl]
            to_remove = st.selectbox("Rimuovi dalla watchlist:", ["— seleziona —"] + wl_labels, key="wl_remove")
            if st.button("🗑️ Rimuovi", key="wl_remove_btn") and to_remove != "— seleziona —":
                wl = [e for e in wl if f"{e.get('ticker','')} {e.get('expiration','')} {e.get('strike','')}{e.get('type','')}" != to_remove]
                save_watchlist(wl)
                st.success(f"Rimosso: {to_remove}")
                st.rerun()
        else:
            st.info("Nessun contratto in watchlist. Aggiungine uno sopra.")

    # =========================
    # SCAN BUTTON — LOGICA
    # =========================
    if scan_clicked:
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
                top_filtered = top[top["SIG"].str.contains("GO|HOLD", na=False)]
                if not top_filtered.empty:
                    telegram_text += f"🔥 TOP FLOW — {ticker} [{mode}]\n\n"
                    for _, row in top_filtered.head(3).iterrows():
                        ask_hit_val = row.get("ASK_HIT")
                        sweep_val   = row.get("SWEEP", "")
                        whale_days  = row.get("🐋 DAYS", 0)
                        hit_emoji   = ""
                        if ask_hit_val is not None:
                            hit_emoji = "🟢" if ask_hit_val>=70 else ("🔴" if ask_hit_val<=30 else "🟡")
                        telegram_text += (
                            f"{row.get('SCORE','')}  {row['SIG']}  {row['BIAS']}\n"
                            f"{row['OPZIONE']}\n"
                            f"Mid: ${row['MID']}  VOI: {row['VOI']}  Vol: {row['volume']}\n"
                            f"Flow: {row['FLOW $']}"
                        )
                        if ask_hit_val is not None:
                            telegram_text += f"  Ask Hit: {hit_emoji}{ask_hit_val:.0f}%"
                        if sweep_val:
                            telegram_text += f"  {sweep_val}"
                        if whale_days >= 2:
                            telegram_text += f"  🐋{whale_days}d"
                        telegram_text += "\n\n"

        if not final_df.empty:
            if send_telegram and telegram_text:
                ok = send_telegram_message(telegram_text)
                if ok:
                    st.success("📲 Alert Telegram inviato!")
                else:
                    st.error("❌ Errore invio Telegram")

            records = []
            for _, r in final_df.iterrows():
                rec = {}
                for col in final_df.columns:
                    val = r[col]
                    try:
                        if pd.isna(val): val = None
                    except: pass
                    if isinstance(val, (pd.Timestamp,)): val = str(val)
                    rec[col] = val
                records.append(rec)
            st.session_state["saved_records"] = records
            st.session_state["saved_tickers"] = tickers

        else:
            st.warning("⚠️ Nessuna opportunità trovata. Prova ad allargare i filtri.")
            st.session_state.pop("saved_records", None)

    # =========================
    # MOSTRA RISULTATI
    # =========================
    if st.session_state.get("saved_records"):
        records  = st.session_state["saved_records"]
        df_show  = pd.DataFrame(records)

        st.success(f"✅ Trovate {len(df_show)} opportunità — premi 🚀 per aggiornare")

        display_cols = [
            "SCORE", "SIG", "FLOW $", "CLUSTER", "BIAS", "SWEEP", "🐋 DAYS",
            "OPZIONE", "UNDER", "MID", "volume", "OI", "VOI", "VOI_ANOM", "DTE", "IV",
            "GEX_M", "ASK_HIT", "EARN", "ITM", "ATM", "OTM",
        ]
        display_cols = [c for c in display_cols if c in df_show.columns]

        def hl_score(val):
            try:
                v = int(str(val).split()[-1])
                if v >= 75: return "background-color:#0a2a0a; color:#00ff44; font-weight:bold"
                if v >= 50: return "background-color:#1a2a00; color:#aaff00; font-weight:bold"
                if v >= 30: return "background-color:#2a2a00; color:#ffdd00"
                return "background-color:#1a1a1a; color:#888888"
            except: return ""
        def hl_earn(val):
            if val and "⚠️" in str(val): return "background-color:#3a1a00; color:#ffaa00"
            return ""
        def hl_voi_anom(val):
            s = str(val)
            if "🚀" in s: return "background-color:#0a1a3a; color:#00aaff; font-weight:bold"
            if "📈" in s: return "background-color:#0a2a1a; color:#00ff88"
            if "📉" in s: return "background-color:#2a0a0a; color:#ff6666"
            return ""
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
        def hl_gex(val):
            try:
                v = float(val)
                if v >= 1.0:  return "background-color:#1a3a1a; color:#00ff88"
                if v <= -1.0: return "background-color:#3a0a0a; color:#ff4444"
                return ""
            except: return ""

        fmt = {
            "UNDER":   lambda x: f"${x:.2f}"        if pd.notna(x) else "—",
            "MID":     lambda x: f"${x:.2f}"        if pd.notna(x) else "—",
            "VOI":     lambda x: f"{float(x):.2f}"  if pd.notna(x) else "—",
            "ASK_HIT": lambda x: f"{float(x):.0f}%" if pd.notna(x) else "—",
            "IV":      lambda x: f"{x:.1f}%"        if pd.notna(x) else "—",
            "GEX_M":   lambda x: f"{x:+.2f}M"       if pd.notna(x) and x != 0 else "—",
            "delta":   lambda x: f"{x:.3f}"         if pd.notna(x) else "—",
            "gamma":   lambda x: f"{x:.4f}"         if pd.notna(x) else "—",
            "theta":   lambda x: f"{x:.3f}"         if pd.notna(x) else "—",
            "vega":    lambda x: f"{x:.3f}"         if pd.notna(x) else "—",
        }

        styled = (
            df_show[display_cols].reset_index(drop=True).style
            .map(hl_score,    subset=["SCORE"]    if "SCORE"    in df_show.columns else [])
            .map(hl_sig,      subset=["SIG"])
            .map(hl_ask,      subset=["ASK_HIT"]  if "ASK_HIT"  in df_show.columns else [])
            .map(hl_sweep,    subset=["SWEEP"]     if "SWEEP"    in df_show.columns else [])
            .map(hl_whale,    subset=["🐋 DAYS"]   if "🐋 DAYS"  in df_show.columns else [])
            .map(hl_earn,     subset=["EARN"]      if "EARN"     in df_show.columns else [])
            .map(hl_voi_anom, subset=["VOI_ANOM"]  if "VOI_ANOM" in df_show.columns else [])
            .map(hl_gex,      subset=["GEX_M"]     if "GEX_M"    in df_show.columns else [])
            .format(fmt, na_rep="—")
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

        # GEX Summary
        if "GEX_M" in df_show.columns and "strike" in df_show.columns:
            with st.expander("⚡ GEX — Gamma Exposure per Strike", expanded=False):
                st.markdown("""
**Come leggere il GEX:**
- 🟢 **GEX positivo** → dealer long gamma → frenano i movimenti → livello di supporto/resistenza
- 🔴 **GEX negativo** → dealer short gamma → amplificano i movimenti → livello esplosivo
""")
                gex_by_strike = (
                    df_show.groupby("strike")["GEX_M"]
                    .sum()
                    .reset_index()
                    .sort_values("GEX_M", ascending=False)
                    .rename(columns={"strike": "Strike", "GEX_M": "GEX Totale ($M)"})
                )
                gex_by_strike["Strike"] = gex_by_strike["Strike"].apply(
                    lambda x: str(int(x)) if x == int(x) else f"{x:.2f}"
                )
                gex_by_strike["GEX Totale ($M)"] = gex_by_strike["GEX Totale ($M)"].round(2)
                def hl_gex_table(val):
                    try:
                        v = float(val)
                        if v >= 1.0:  return "background-color:#1a3a1a; color:#00ff88"
                        if v <= -1.0: return "background-color:#3a0a0a; color:#ff4444"
                    except: pass
                    return ""
                st.dataframe(
                    gex_by_strike.style
                    .map(hl_gex_table, subset=["GEX Totale ($M)"])
                    .format({"GEX Totale ($M)": lambda x: f"{x:+.2f}M"}),
                    use_container_width=True, hide_index=True
                )

        # Greeks
        df_greeks = df_show.copy()
        if "bid"    in df_greeks.columns: df_greeks = df_greeks.rename(columns={"bid":    "~bid"})
        if "ask"    in df_greeks.columns: df_greeks = df_greeks.rename(columns={"ask":    "~ask"})
        if "SPREAD" in df_greeks.columns: df_greeks = df_greeks.rename(columns={"SPREAD": "~SPREAD"})
        greeks_cols = ["OPZIONE", "delta", "gamma", "theta", "vega", "~bid", "~ask", "~SPREAD"]
        greeks_cols = [c for c in greeks_cols if c in df_greeks.columns]
        with st.expander("📐 Dettaglio Greeks & Prezzi stimati"):
            fmt_greeks = {
                "delta":   lambda x: f"{x:.3f}"   if pd.notna(x) else "—",
                "gamma":   lambda x: f"{x:.4f}"   if pd.notna(x) else "—",
                "theta":   lambda x: f"{x:.3f}"   if pd.notna(x) else "—",
                "vega":    lambda x: f"{x:.3f}"   if pd.notna(x) else "—",
                "~bid":    lambda x: f"~${x:.2f}" if pd.notna(x) else "—",
                "~ask":    lambda x: f"~${x:.2f}" if pd.notna(x) else "—",
                "~SPREAD": lambda x: f"~${x:.2f}" if pd.notna(x) else "—",
            }
            st.dataframe(
                df_greeks[greeks_cols].reset_index(drop=True).style.format(fmt_greeks, na_rep="—"),
                use_container_width=True, hide_index=True
            )
            st.caption("~bid / ~ask / ~SPREAD = stime da MID ±1.5% (quote live richiedono piano Advanced)")

        # Aggiungi a Watchlist
        st.markdown("---")
        st.markdown("### ⭐ Aggiungi alla Watchlist")
        scan_records_wl = [
            {"OPZIONE": str(r.get("OPZIONE","")),
             "ticker":  str(r.get("OPZIONE","")).split()[0],
             "strike":  float(r.get("strike", 0)) if r.get("strike") else 0,
             "exp_str": str(r.get("exp_str","")),
             "type":    str(r.get("type","")),
             "flow":    str(r.get("FLOW $","")),
             "voi":     str(r.get("VOI",""))}
            for r in records
        ]
        opzioni_wl = [r["OPZIONE"] for r in scan_records_wl]
        sel_wl = st.multiselect("Seleziona opzioni:", options=opzioni_wl, key="wl_multisel_persist")
        if st.button("➕ Aggiungi alla Watchlist", key="wl_add_persist", type="secondary"):
            added   = 0
            already = 0
            for opzione in sel_wl:
                rec = next((r for r in scan_records_wl if r["OPZIONE"]==opzione), None)
                if rec:
                    type_wl = "C" if rec["type"] == "CALL" else "P"
                    note_wl = f"Flow {rec['flow']} | VOI {rec['voi']}"
                    ok = add_to_watchlist(rec["ticker"], rec["strike"], rec["exp_str"], type_wl, note_wl)
                    if ok: added += 1
                    else:  already += 1
            if not sel_wl:
                st.warning("⚠️ Seleziona almeno un'opzione.")
            elif added > 0:
                msg = f"✅ {added} aggiunt{'o' if added==1 else 'i'}!"
                if already > 0: msg += f" ({already} già in watchlist)"
                st.success(msg)
            else:
                st.info("ℹ️ Tutti già in watchlist.")

    # =========================
    # FOOTER
    # =========================
    st.divider()
    st.caption(
        "⚠️ Questo tool è uno screener di primo livello. "
        "L'analisi finale (grafico, contesto macro, greche) va completata su IBKR. "
        "Nessun ordine viene eseguito automaticamente. — v7.0"
    )

# =========================
# TAB INSIDER TRADING
# =========================
with tab_insider:
    render_insider_section()
