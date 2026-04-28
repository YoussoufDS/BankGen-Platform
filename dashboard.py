"""
BankGen Dashboard — Streamlit
- Live Stream : page fixe, seuls les chiffres/graphes se mettent à jour
- Fraude : données persistées, survivent à l'arrêt
- Historique : conservé entre les sessions
- Snowflake : analytics historiques
"""

import json, time, os
from collections import deque
from datetime import datetime
from pathlib import Path

import requests
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# ── Config ────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="BankGen Dashboard", page_icon="🏦",
                   layout="wide", initial_sidebar_state="expanded")

PERSIST_FILE = "/tmp/bankgen_state.json"
API_URL_DEFAULT = os.getenv("API_URL", "http://localhost:8000")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
.stApp{background:#f8fafc;color:#0f172a;font-family:'Inter',sans-serif}
[data-testid="stSidebar"]{background:#fff;border-right:1px solid #e2e8f0}
[data-testid="metric-container"]{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:14px 16px}
[data-testid="metric-container"] label{color:#94a3b8 !important;font-size:.65rem !important;letter-spacing:.08em;text-transform:uppercase;font-family:'IBM Plex Mono',monospace !important}
[data-testid="stMetricValue"]{color:#1d6ef5 !important;font-size:1.6rem !important;font-weight:700 !important}
[data-testid="stMetricDelta"]{font-size:.75rem !important}
h1,h2,h3{color:#0f172a !important;font-family:'Inter',sans-serif !important}
div[data-testid="stTabs"] button{font-family:'IBM Plex Mono',monospace;font-size:.72rem}
.stButton>button{font-family:'IBM Plex Mono',monospace;font-size:.75rem}
</style>
""", unsafe_allow_html=True)

# ── Palette ───────────────────────────────────────────────────────────────────
C = dict(bg="#f8fafc", surface="#fff", card="#f1f5f9",
         blue="#1d6ef5", teal="#0d9488", orange="#ea580c",
         green="#16a34a", red="#dc2626", yellow="#d97706",
         purple="#7c3aed", grid="#e2e8f0", text="#94a3b8")

def _base(h=220):
    return dict(paper_bgcolor=C["surface"], plot_bgcolor=C["bg"],
                font=dict(color=C["text"], family="IBM Plex Mono, monospace", size=10),
                margin=dict(l=40,r=10,t=36,b=10), height=h, showlegend=False,
                xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                yaxis=dict(gridcolor=C["grid"], zeroline=False))

def _hex_rgb(h):
    h=h.lstrip("#"); return f"{int(h[0:2],16)},{int(h[2:4],16)},{int(h[4:6],16)}"

# ── Persistance locale ────────────────────────────────────────────────────────
def load_persisted():
    try:
        p = Path(PERSIST_FILE)
        if p.exists():
            d = json.loads(p.read_text())
            return (deque(d.get("txn_buffer",[]), maxlen=200),
                    deque(d.get("kpi_buffer",[]), maxlen=200),
                    deque(d.get("fraud_buffer",[]), maxlen=100))
    except: pass
    return deque(maxlen=200), deque(maxlen=200), deque(maxlen=100)

def save_persisted():
    try:
        Path(PERSIST_FILE).write_text(json.dumps({
            "txn_buffer":  list(st.session_state.txn_buffer)[-200:],
            "kpi_buffer":  list(st.session_state.kpi_buffer)[-200:],
            "fraud_buffer":list(st.session_state.fraud_buffer)[-100:],
        }))
    except: pass

# ── Session state ─────────────────────────────────────────────────────────────
if "initialized" not in st.session_state:
    txn_b, kpi_b, fraud_b = load_persisted()
    st.session_state.txn_buffer   = txn_b
    st.session_state.kpi_buffer   = kpi_b
    st.session_state.fraud_buffer = fraud_b
    st.session_state.running      = False
    st.session_state.sf_connected = False
    st.session_state.sf_conn      = None
    st.session_state.api_ok       = False
    st.session_state.total_ticks  = 0
    st.session_state.initialized  = True

# ── Snowflake ─────────────────────────────────────────────────────────────────
def connect_sf(account, user, pwd, wh, db, schema, role, auth_mode="password"):
    try:
        import snowflake.connector
        params = dict(account=account, user=user, warehouse=wh,
                      database=db, schema=schema, role=role,
                      client_session_keep_alive=True)
        if auth_mode == "browser":
            params["authenticator"] = "externalbrowser"
        elif auth_mode == "keypair":
            import os
            from cryptography.hazmat.primitives.serialization import (
                load_pem_private_key, Encoding, PrivateFormat, NoEncryption)
            key_path = pwd  # pwd field reused for key path
            with open(key_path, "rb") as f:
                private_key = load_pem_private_key(f.read(), password=None)
            params["private_key"] = private_key.private_bytes(
                Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
        else:
            params["password"] = pwd
        conn = snowflake.connector.connect(**params)
        st.session_state.sf_conn = conn
        st.session_state.sf_connected = True
        return True, "Connexion réussie ✅"
    except Exception as e:
        st.session_state.sf_connected = False
        return False, str(e)

def sf_query(sql):
    if not st.session_state.sf_connected: return None
    try:
        cur=st.session_state.sf_conn.cursor()
        cur.execute(sql)
        cols=[d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(),columns=cols)
    except Exception as e:
        st.warning(f"Snowflake: {e}"); return None

def sf_insert_txns(txns):
    if not st.session_state.sf_connected: return
    try:
        cur=st.session_state.sf_conn.cursor()
        rows=[(t["txn_id"],t["timestamp"][:19].replace("T"," "),t["account_id"],
               t["account_type"],t["merchant_id"],t["merchant_category"],t["amount"],
               t["currency"],t["status"],t["bank"],t["province"],t["is_fraud"],
               t["fraud_score"],t.get("fraud_pattern"),t["is_chargeback"],
               t["channel"],t["processing_ms"]) for t in txns]
        cur.executemany("""INSERT INTO TRANSACTIONS (TXN_ID,TXN_TIMESTAMP,ACCOUNT_ID,
            ACCOUNT_TYPE,MERCHANT_ID,MERCHANT_CATEGORY,AMOUNT,CURRENCY,STATUS,BANK,
            PROVINCE,IS_FRAUD,FRAUD_SCORE,FRAUD_PATTERN,IS_CHARGEBACK,CHANNEL,PROCESSING_MS)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",rows)
    except Exception as e: st.warning(f"Insert error: {e}")

def sf_insert_kpi(k):
    if not st.session_state.sf_connected: return
    try:
        cur=st.session_state.sf_conn.cursor()
        cur.execute("""INSERT INTO KPI_SNAPSHOTS (KPI_TIMESTAMP,TICK,TOTAL_TRANSACTIONS,
            TOTAL_VOLUME_CAD,APPROVAL_RATE_PCT,DECLINE_RATE_PCT,FRAUD_RATE_PCT,
            FRAUD_COUNT,FRAUD_VOLUME_CAD,AVG_TXN_AMOUNT,ACTIVE_ACCOUNTS,
            CHARGEBACK_COUNT,TXN_PER_TICK,VOLUME_PER_TICK)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (k["timestamp"][:19].replace("T"," "),k["tick"],k["total_transactions"],
             k["total_volume_cad"],k["approval_rate_pct"],k["decline_rate_pct"],
             k["fraud_rate_pct"],k["fraud_count"],k["fraud_volume_cad"],
             k["avg_txn_amount"],k["active_accounts"],k["chargeback_count"],
             k["txn_per_tick"],k["volume_per_tick"]))
    except Exception as e: st.warning(f"KPI insert: {e}")

# ── Charts ────────────────────────────────────────────────────────────────────
def line_chart(df, col, title, color, h=210):
    fig=go.Figure(go.Scatter(x=df.index,y=df[col],mode="lines",
        line=dict(color=color,width=2),fill="tozeroy",
        fillcolor=f"rgba({_hex_rgb(color)},.07)"))
    fig.update_layout(title=dict(text=title,font=dict(size=11,color="#0f172a")),**_base(h))
    return fig

def bar_chart(df, col, title, color, h=210):
    fig=go.Figure(go.Bar(x=df.index,y=df[col],marker_color=color,marker_line_width=0,opacity=.8))
    fig.update_layout(title=dict(text=title,font=dict(size=11,color="#0f172a")),**_base(h))
    return fig

def gauge(value, title, max_val, color, suffix=""):
    fig=go.Figure(go.Indicator(
        mode="gauge+number",value=value,
        number=dict(suffix=suffix,font=dict(color=color,size=24)),
        title=dict(text=title,font=dict(color=C["text"],size=10)),
        gauge=dict(axis=dict(range=[0,max_val],tickfont=dict(size=8)),
            bar=dict(color=color,thickness=.28),bgcolor=C["bg"],borderwidth=0,
            steps=[dict(range=[0,max_val*.6],color=C["card"]),
                   dict(range=[max_val*.6,max_val*.85],color="#fef3c7"),
                   dict(range=[max_val*.85,max_val],color="#fee2e2")],
            threshold=dict(line=dict(color=C["red"],width=2),thickness=.75,value=max_val*.85))))
    fig.update_layout(paper_bgcolor=C["surface"],font=dict(color=C["text"]),
        margin=dict(l=15,r=15,t=30,b=5),height=185)
    return fig

def sf_bar(df, x, y, title, color, h=260):
    if df is None or df.empty: return None
    fig=go.Figure(go.Bar(x=df[x],y=df[y],marker_color=color,marker_line_width=0,opacity=.8))
    fig.update_layout(title=dict(text=title,font=dict(size=11,color="#0f172a")),
        **{**_base(h),"xaxis":dict(gridcolor=C["grid"],tickfont=dict(size=8),showticklabels=True)})
    return fig

def sf_pie(df, names, values, title, h=260):
    if df is None or df.empty: return None
    colors=[C["blue"],C["teal"],C["orange"],C["green"],C["purple"],
            C["yellow"],C["red"],"#0ea5e9","#f59e0b","#10b981"]
    fig=go.Figure(go.Pie(labels=df[names],values=df[values],hole=.42,
        marker=dict(colors=colors[:len(df)],line=dict(color="#fff",width=1.5)),
        textfont=dict(size=9)))
    fig.update_layout(title=dict(text=title,font=dict(size=11,color="#0f172a")),
        paper_bgcolor=C["surface"],font=dict(color=C["text"]),
        margin=dict(l=10,r=10,t=40,b=10),height=h,
        legend=dict(font=dict(size=8),bgcolor="rgba(0,0,0,0)"))
    return fig

# ── API fetch ─────────────────────────────────────────────────────────────────
def fetch_api(url):
    try:
        r=requests.get(f"{url}/snapshot?batch=8",timeout=4)
        r.raise_for_status(); return r.json()
    except: return None

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏦 BankGen Dashboard")
    st.markdown("---")
    tab_api, tab_sf = st.tabs(["⚡ API", "❄️ Snowflake"])

    with tab_api:
        api_url = st.text_input("URL API", value=API_URL_DEFAULT)
        interval = st.slider("Intervalle (s)", .5, 5., 1., .5)
        window   = st.slider("Fenêtre (ticks)", 20, 150, 80)
        c1,c2 = st.columns(2)
        start_btn = c1.button("▶ Start", use_container_width=True, type="primary")
        stop_btn  = c2.button("⏹ Stop",  use_container_width=True)
        if st.session_state.api_ok:
            st.success("● API connectée")
        else:
            st.caption("⚠ API non connectée")

    with tab_sf:
        sf_account = st.text_input("Account", value=os.getenv("SF_ACCOUNT","HAB33465"), placeholder="HAB33465")
        sf_user    = st.text_input("User",    value=os.getenv("SF_USER","SFEDU02"))
        sf_wh      = st.text_input("Warehouse", value=os.getenv("SF_WAREHOUSE","COMPUTE_WH"))
        sf_db      = st.text_input("Database",  value=os.getenv("SF_DATABASE","BANKGEN_DB"))
        sf_schema  = st.text_input("Schema",    value=os.getenv("SF_SCHEMA","BANKING"))
        sf_role    = st.text_input("Role",      value=os.getenv("SF_ROLE","TRAINING_ROLE"))

        st.markdown("**Mode de connexion**")
        auth_mode = st.radio("Authentification", 
            ["🌐 Navigateur (MFA)", "🔑 Mot de passe", "🔐 Clé RSA"],
            index=0, label_visibility="collapsed")

        if auth_mode == "🔑 Mot de passe":
            sf_pwd = st.text_input("Password", value="", type="password")
            sf_extra = sf_pwd
        elif auth_mode == "🔐 Clé RSA":
            st.caption("Chemin vers rsa_key.pem dans le container")
            sf_extra = st.text_input("Chemin clé privée", value="/app/rsa_key.pem")
        else:
            sf_extra = ""
            st.info("En Docker, utilisez Mot de passe ou Cle RSA.")

        mode_map = {"🌐 Navigateur (MFA)": "browser", "🔑 Mot de passe": "password", "🔐 Clé RSA": "keypair"}

        if st.button("❄️ Connecter Snowflake", use_container_width=True):
            with st.spinner("Connexion en cours..."):
                ok, msg = connect_sf(sf_account, sf_user, sf_extra,
                                     sf_wh, sf_db, sf_schema, sf_role,
                                     auth_mode=mode_map[auth_mode])
            if ok: st.success(msg)
            else:  st.error(msg[:300])

        if st.session_state.sf_connected:
            st.success("● Snowflake connecté")
            persist_sf = st.checkbox("💾 Persister dans Snowflake", value=True)
        else:
            persist_sf = False

    st.markdown("---")
    st.caption(f"Ticks : **{st.session_state.total_ticks}**")
    st.caption(f"Transactions : **{len(st.session_state.txn_buffer)}**")
    st.caption(f"Alertes fraude : **{len(st.session_state.fraud_buffer)}**")
    if st.button("🗑 Effacer les données", use_container_width=True):
        st.session_state.txn_buffer.clear()
        st.session_state.kpi_buffer.clear()
        st.session_state.fraud_buffer.clear()
        st.session_state.total_ticks=0
        save_persisted()
        st.success("Données effacées")

# ── Contrôles ─────────────────────────────────────────────────────────────────
if start_btn: st.session_state.running = True
if stop_btn:
    st.session_state.running = False
    st.session_state.api_ok  = False
    save_persisted()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("## 🏦 BankGen — Surveillance Bancaire")
st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_live, tab_fraud, tab_sf_tab, tab_txns = st.tabs([
    "⚡ Live Stream", "🚨 Fraude", "❄️ Snowflake Analytics", "📋 Transactions"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB LIVE — page fixe, mise à jour en place avec st.empty()
# ══════════════════════════════════════════════════════════════════════════════
with tab_live:
    if not st.session_state.running and not st.session_state.kpi_buffer:
        st.info("👆 Cliquez **▶ Start** dans la sidebar pour lancer le flux.")
    else:
        # Placeholders FIXES — créés une seule fois
        kpi_ph   = st.empty()
        gauge_ph = st.empty()
        row1_ph  = st.empty()
        row2_ph  = st.empty()

        if st.session_state.running:
            data = fetch_api(api_url)
            if data:
                st.session_state.api_ok = True
                st.session_state.total_ticks += 1
                txns = data.get("transactions", [])
                kpi  = data.get("kpis", {})
                for t in txns: st.session_state.txn_buffer.append(t)
                if kpi: st.session_state.kpi_buffer.append(kpi)
                for t in txns:
                    if t.get("is_fraud"): st.session_state.fraud_buffer.append(t)
                if persist_sf and txns: sf_insert_txns(txns)
                if persist_sf and kpi:  sf_insert_kpi(kpi)
                # Sauvegarde locale toutes les 10 ticks
                if st.session_state.total_ticks % 10 == 0: save_persisted()

        kdf = pd.DataFrame(list(st.session_state.kpi_buffer)[-window:])
        tdf = pd.DataFrame(list(st.session_state.txn_buffer)[-window*5:])

        if not kdf.empty:
            last = kdf.iloc[-1]; prev = kdf.iloc[-2] if len(kdf)>1 else last

            # KPIs
            with kpi_ph.container():
                c1,c2,c3,c4,c5,c6,c7 = st.columns(7)
                c1.metric("💳 Transactions", int(last.get("total_transactions",0)),
                          int(last.get("total_transactions",0)-prev.get("total_transactions",0)))
                c2.metric("💰 Volume",  f"${last.get('total_volume_cad',0)/1000:.1f}k",
                          f"+${last.get('volume_per_tick',0):.0f}")
                c3.metric("✅ Approbation", f"{last.get('approval_rate_pct',0):.1f}%")
                c4.metric("🚨 Fraude",  f"{last.get('fraud_rate_pct',0):.3f}%",
                          f"{last.get('fraud_rate_pct',0)-prev.get('fraud_rate_pct',0):+.4f}%")
                c5.metric("👥 Comptes", int(last.get("active_accounts",0)))
                c6.metric("📊 Moy. txn", f"${last.get('avg_txn_amount',0):.0f}")
                c7.metric("🔄 Chargebacks", int(last.get("chargeback_count",0)))

            # Jauges
            with gauge_ph.container():
                g1,g2,g3,g4 = st.columns(4)
                with g1: st.plotly_chart(gauge(last.get("approval_rate_pct",0),"Approbation",100,C["teal"],"%"),use_container_width=True,config={"displayModeBar":False})
                with g2: st.plotly_chart(gauge(last.get("fraud_rate_pct",0),"Taux Fraude",5,C["orange"],"%"),use_container_width=True,config={"displayModeBar":False})
                with g3: st.plotly_chart(gauge(last.get("decline_rate_pct",0),"Taux Refus",20,C["red"],"%"),use_container_width=True,config={"displayModeBar":False})
                with g4: st.plotly_chart(gauge(last.get("avg_txn_amount",0),"Moy. Montant",500,C["blue"],"$"),use_container_width=True,config={"displayModeBar":False})

            # Ligne 1
            with row1_ph.container():
                r1,r2,r3 = st.columns(3)
                with r1: st.plotly_chart(line_chart(kdf.reset_index(),"total_volume_cad","Volume total ($)",C["blue"]),use_container_width=True,config={"displayModeBar":False})
                with r2: st.plotly_chart(bar_chart(kdf.reset_index(),"txn_per_tick","Transactions / tick",C["teal"]),use_container_width=True,config={"displayModeBar":False})
                with r3: st.plotly_chart(line_chart(kdf.reset_index(),"fraud_rate_pct","Taux fraude (%)",C["orange"]),use_container_width=True,config={"displayModeBar":False})

            # Ligne 2
            if not tdf.empty and "merchant_category" in tdf.columns:
                with row2_ph.container():
                    r1,r2,r3 = st.columns(3)
                    cat_v = tdf[tdf["status"]=="APPROVED"].groupby("merchant_category")["amount"].sum().sort_values(ascending=False).head(8).reset_index()
                    with r1:
                        fig=go.Figure(go.Bar(x=cat_v["merchant_category"],y=cat_v["amount"],
                            marker_color=C["purple"],marker_line_width=0,opacity=.8))
                        fig.update_layout(title=dict(text="Volume par catégorie ($)",font=dict(size=11,color="#0f172a")),
                            **{**_base(210),"xaxis":dict(gridcolor=C["grid"],tickfont=dict(size=7),showticklabels=True)})
                        st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})
                    ch_v=tdf.groupby("channel").size().reset_index(name="count")
                    with r2:
                        fig=sf_pie(ch_v,"channel","count","Transactions par canal",210)
                        if fig: st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})
                    with r3:
                        fig=go.Figure(go.Histogram(x=tdf["amount"],nbinsx=25,
                            marker_color=C["yellow"],marker_line_width=0,opacity=.8))
                        fig.update_layout(title=dict(text="Distribution montants ($)",font=dict(size=11,color="#0f172a")),
                            **{**_base(210),"xaxis":dict(gridcolor=C["grid"],tickfont=dict(size=8),showticklabels=True)})
                        st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})

        if st.session_state.running:
            time.sleep(interval)
            st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB FRAUDE — données persistées
# ══════════════════════════════════════════════════════════════════════════════
with tab_fraud:
    st.markdown("### 🚨 Centre de surveillance fraude")
    fdf = pd.DataFrame(list(st.session_state.fraud_buffer))
    if fdf.empty:
        st.info("Aucune alerte fraude. Lancez le flux — les données sont conservées entre sessions.")
    else:
        fc1,fc2,fc3,fc4 = st.columns(4)
        fc1.metric("🚨 Alertes",      len(fdf))
        fc2.metric("💸 Volume",       f"${fdf['amount'].sum():,.0f}")
        fc3.metric("📊 Moy. montant", f"${fdf['amount'].mean():,.0f}")
        fc4.metric("🔴 Score moyen",  f"{fdf['fraud_score'].mean():.3f}")
        st.markdown("---")
        col1,col2 = st.columns(2)
        with col1:
            pat=fdf.groupby("fraud_pattern").size().reset_index(name="count")
            fig=sf_pie(pat,"fraud_pattern","count","Fraude par pattern",260)
            if fig: st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})
        with col2:
            fig=go.Figure(go.Histogram(x=fdf["fraud_score"],nbinsx=20,
                marker_color=C["orange"],marker_line_width=0,opacity=.8))
            fig.update_layout(title=dict(text="Distribution scores de fraude",font=dict(size=11,color="#0f172a")),
                **{**_base(260),"xaxis":dict(gridcolor=C["grid"],showticklabels=True,tickfont=dict(size=9))})
            st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})
        st.markdown("#### 📋 Dernières alertes")
        disp=fdf[["txn_id","timestamp","account_id","merchant_category","amount",
                   "currency","fraud_pattern","fraud_score","channel"]].tail(20)
        disp.columns=["ID Transaction","Horodatage","Compte","Catégorie","Montant",
                       "Devise","Pattern","Score","Canal"]
        st.dataframe(disp.sort_values("Horodatage",ascending=False),
                     use_container_width=True,hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB SNOWFLAKE
# ══════════════════════════════════════════════════════════════════════════════
with tab_sf_tab:
    st.markdown("### ❄️ Analytics Snowflake")
    if not st.session_state.sf_connected:
        st.warning("Connectez Snowflake via la sidebar (onglet ❄️).")
    else:
        if st.button("🔄 Rafraîchir"):
            st.cache_data.clear()
        c1,c2 = st.columns(2)
        with c1:
            df=sf_query("SELECT * FROM V_VOLUME_BY_CATEGORY LIMIT 12")
            fig=sf_bar(df,"MERCHANT_CATEGORY","TOTAL_VOLUME","Volume par catégorie ($)",C["blue"],280)
            if fig: st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})
        with c2:
            df=sf_query("SELECT * FROM V_APPROVAL_BY_BANK")
            fig=sf_bar(df,"BANK","APPROVAL_RATE_PCT","Approbation par banque (%)",C["teal"],280)
            if fig: st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})
        c3,c4 = st.columns(2)
        with c3:
            df=sf_query("SELECT * FROM V_FRAUD_BY_PATTERN")
            fig=sf_pie(df,"FRAUD_PATTERN","ALERT_COUNT","Fraude par pattern (Snowflake)",280)
            if fig: st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})
        with c4:
            df=sf_query("SELECT * FROM V_TXN_BY_PROVINCE")
            fig=sf_bar(df,"PROVINCE","TXN_COUNT","Transactions par province",C["purple"],280)
            if fig: st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})
        st.markdown("#### 🔍 Requête SQL libre")
        sql=st.text_area("SQL","SELECT * FROM TRANSACTIONS ORDER BY TXN_TIMESTAMP DESC LIMIT 20",height=80)
        if st.button("▶ Exécuter"):
            r=sf_query(sql)
            if r is not None:
                st.success(f"{len(r)} lignes"); st.dataframe(r,use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB TRANSACTIONS
# ══════════════════════════════════════════════════════════════════════════════
with tab_txns:
    st.markdown("### 📋 Transactions (mémoire locale + persistées)")
    tdf_all = pd.DataFrame(list(st.session_state.txn_buffer))
    if tdf_all.empty:
        st.info("Aucune transaction. Lancez le flux.")
    else:
        f1,f2,f3,f4 = st.columns(4)
        with f1: sf=st.selectbox("Statut",["Tous","APPROVED","DECLINED"])
        with f2: ff=st.selectbox("Fraude",["Tous","Oui","Non"])
        with f3:
            cats=["Toutes"]+sorted(tdf_all["merchant_category"].unique().tolist())
            cf=st.selectbox("Catégorie",cats)
        with f4:
            banks=["Toutes"]+sorted(tdf_all["bank"].unique().tolist())
            bf=st.selectbox("Banque",banks)
        filt=tdf_all.copy()
        if sf!="Tous":   filt=filt[filt["status"]==sf]
        if ff=="Oui":    filt=filt[filt["is_fraud"]==True]
        elif ff=="Non":  filt=filt[filt["is_fraud"]==False]
        if cf!="Toutes": filt=filt[filt["merchant_category"]==cf]
        if bf!="Toutes": filt=filt[filt["bank"]==bf]
        st.caption(f"**{len(filt)}** transactions affichées / {len(tdf_all)} en mémoire")
        cols=["txn_id","timestamp","account_id","account_type","merchant_category",
              "amount","currency","status","bank","province","is_fraud","fraud_score","channel"]
        st.dataframe(filt[cols].sort_values("timestamp",ascending=False).head(300),
                     use_container_width=True,hide_index=True)
