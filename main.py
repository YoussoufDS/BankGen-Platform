"""
BankGen API — Abdouramane's API
Générateur de données bancaires synthétiques en temps réel
"""

import asyncio, json, random, math, uuid, os
from collections import deque
from datetime import datetime, timezone
from typing import AsyncGenerator
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse

# ── Page d'accueil HTML ───────────────────────────────────────────────────────
HOME_HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Abdouramane's API</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#f8fafc;--white:#fff;--border:#e2e8f0;--text:#0f172a;--text2:#475569;--text3:#94a3b8;
  --blue:#1d6ef5;--blue-l:#eff6ff;--blue-m:#dbeafe;--teal:#0d9488;--teal-l:#f0fdfa;
  --green:#16a34a;--green-l:#f0fdf4;--red:#dc2626;
  --mono:'IBM Plex Mono',monospace;--sans:'Inter',sans-serif}
body{background:var(--bg);color:var(--text);font-family:var(--sans);-webkit-font-smoothing:antialiased;min-height:100vh}
nav{background:var(--white);border-bottom:1px solid var(--border);
  padding:0 2.5rem;height:58px;display:flex;align-items:center;justify-content:space-between;
  position:sticky;top:0;z-index:100}
.logo{display:flex;align-items:center;gap:.65rem}
.logo-box{width:30px;height:30px;background:var(--blue);border-radius:7px;
  display:grid;place-items:center;color:#fff;font-size:.85rem;box-shadow:0 2px 8px rgba(29,110,245,.3)}
.logo-text{font-weight:700;font-size:.9rem}.logo-text span{color:var(--blue)}
.nav-r{display:flex;gap:.6rem;align-items:center}
.badge{font-family:var(--mono);font-size:.6rem;padding:.2rem .6rem;border-radius:20px;
  background:var(--bg);border:1px solid var(--border);color:var(--text2)}
.live{display:flex;align-items:center;gap:.35rem;background:var(--green-l);
  border:1px solid #bbf7d0;padding:.2rem .65rem;border-radius:20px;
  font-size:.65rem;font-weight:600;color:var(--green)}
.dot{width:5px;height:5px;border-radius:50%;background:var(--green);animation:pg 2s infinite}

.hero{background:linear-gradient(135deg,#1d6ef5 0%,#0d9488 100%);padding:4rem 2.5rem;text-align:center}
.hero-tag{display:inline-flex;align-items:center;gap:.4rem;
  background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.25);
  color:#fff;font-size:.7rem;font-weight:500;letter-spacing:.06em;text-transform:uppercase;
  padding:.25rem .8rem;border-radius:20px;margin-bottom:1.5rem}
h1{font-size:2.4rem;font-weight:700;color:#fff;letter-spacing:-.03em;margin-bottom:.5rem}
h1 span{opacity:.75;font-weight:300}
.hero-sub{color:rgba(255,255,255,.75);font-size:.95rem;max-width:480px;margin:0 auto 2rem;line-height:1.65}
.btn-row{display:flex;gap:.75rem;justify-content:center;flex-wrap:wrap}
.btn{display:inline-flex;align-items:center;gap:.45rem;padding:.7rem 1.4rem;
  border-radius:9px;font-size:.82rem;font-weight:600;text-decoration:none;
  cursor:pointer;border:none;transition:all .18s}
.btn-w{background:#fff;color:var(--blue)}.btn-w:hover{background:#f0f4ff;transform:translateY(-1px)}
.btn-o{background:rgba(255,255,255,.15);color:#fff;border:1.5px solid rgba(255,255,255,.3)}
.btn-o:hover{background:rgba(255,255,255,.22);transform:translateY(-1px)}

.wrap{max-width:1100px;margin:0 auto;padding:0 2rem}
.section{padding:3.5rem 0}
.sh{margin-bottom:2rem}
.sk{font-size:.68rem;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:var(--blue);margin-bottom:.5rem}
.st{font-size:1.5rem;font-weight:700;letter-spacing:-.02em;color:var(--text)}
.ss{font-size:.88rem;color:var(--text2);margin-top:.4rem;line-height:1.6}

.ep-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:1rem}
.ep{background:var(--white);border:1px solid var(--border);border-radius:11px;
  padding:1.1rem 1.2rem;display:flex;gap:.85rem;align-items:flex-start;
  transition:border-color .2s,box-shadow .2s;text-decoration:none;color:inherit}
.ep:hover{border-color:var(--blue);box-shadow:0 4px 16px rgba(29,110,245,.08)}
.ep-m{font-family:var(--mono);font-size:.58rem;font-weight:700;letter-spacing:.05em;
  background:var(--blue-l);color:var(--blue);border:1px solid var(--blue-m);
  padding:.13rem .45rem;border-radius:4px;white-space:nowrap;margin-top:2px;flex-shrink:0}
.ep-body{min-width:0}
.ep-path{font-family:var(--mono);font-size:.75rem;font-weight:500;color:var(--text);margin-bottom:.25rem}
.ep-desc{font-size:.78rem;color:var(--text2);line-height:1.5}
.ep-tag{font-size:.62rem;font-weight:500;padding:.12rem .45rem;border-radius:4px;
  background:var(--teal-l);color:var(--teal);border:1px solid #99f6e4;
  display:inline-block;margin-top:.4rem}

.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;margin-bottom:2rem}
.kpi{background:var(--white);border:1px solid var(--border);border-radius:11px;
  padding:1.1rem 1.2rem;text-align:center}
.kv{font-family:var(--mono);font-size:1.4rem;font-weight:700;color:var(--blue)}
.kv.t{color:var(--teal)}.kv.g{color:var(--green)}.kv.r{color:var(--red)}
.kl{font-size:.65rem;color:var(--text3);margin-top:.25rem;font-weight:500;
  letter-spacing:.05em;text-transform:uppercase}

.info-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:1rem}
.info{background:var(--white);border:1px solid var(--border);border-radius:11px;padding:1.3rem}
.info-title{font-weight:600;font-size:.88rem;color:var(--text);margin-bottom:.5rem;
  display:flex;align-items:center;gap:.4rem}
.info-body{font-size:.8rem;color:var(--text2);line-height:1.65}
.code{font-family:var(--mono);background:var(--bg);border:1px solid var(--border);
  padding:.6rem .8rem;border-radius:7px;font-size:.72rem;color:var(--text);
  margin-top:.6rem;overflow-x:auto;white-space:pre}

.div{height:1px;background:var(--border);margin:0}

footer{background:var(--white);border-top:1px solid var(--border);
  padding:1.5rem 2.5rem;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:1rem}
.ft{font-size:.78rem;font-weight:700;color:var(--text)}.ft span{color:var(--blue)}
.fc{font-size:.72rem;color:var(--text3)}
.fstack{display:flex;gap:.4rem;flex-wrap:wrap}
.ftag{font-size:.62rem;padding:.14rem .48rem;border-radius:5px;
  background:var(--bg);border:1px solid var(--border);color:var(--text2)}

@keyframes pg{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(22,163,74,.4)}50%{opacity:.6;box-shadow:0 0 0 4px rgba(22,163,74,0)}}
@media(max-width:768px){.kpi-row,.info-grid{grid-template-columns:1fr 1fr}h1{font-size:1.8rem}}
</style>
</head>
<body>
<nav>
  <div class="logo">
    <div class="logo-box">⚡</div>
    <div class="logo-text"><span>Abdouramane</span>'s API</div>
  </div>
  <div class="nav-r">
    <div class="live"><div class="dot"></div>Running</div>
    <div class="badge">BankGen v2.0</div>
    <div class="badge">FastAPI</div>
    <a href="/docs" class="btn btn-w" style="padding:.3rem .85rem;font-size:.75rem">📄 Swagger</a>
  </div>
</nav>

<div class="hero">
  <div class="hero-tag">🏦 Banking Data Generator</div>
  <h1>Abdouramane<span>'s</span> API</h1>
  <p class="hero-sub">Générateur de transactions bancaires synthétiques en temps réel. SSE streaming, KPIs, alertes fraude et persistance Snowflake.</p>
  <div class="btn-row">
    <a href="/docs" class="btn btn-w">📄 Swagger UI</a>
    <a href="/snapshot" class="btn btn-o">⚡ Snapshot JSON</a>
    <a href="/health" class="btn btn-o">❤️ Health Check</a>
    <a href="/portfolio/summary" class="btn btn-o">📊 Portfolio</a>
  </div>
</div>

<div class="wrap">
  <!-- KPIs live -->
  <div class="section">
    <div class="sh">
      <div class="sk">Métriques en direct</div>
      <div class="st">État du générateur</div>
      <div class="ss">Mis à jour toutes les 2 secondes depuis <code style="font-family:var(--mono);font-size:.82em">/health</code></div>
    </div>
    <div class="kpi-row">
      <div class="kpi"><div class="kv" id="k-txn">—</div><div class="kl">Transactions générées</div></div>
      <div class="kpi"><div class="kv t" id="k-vol">—</div><div class="kl">Volume total ($)</div></div>
      <div class="kpi"><div class="kv g" id="k-tick">—</div><div class="kl">Ticks écoulés</div></div>
      <div class="kpi"><div class="kv r" id="k-frd">—</div><div class="kl">Alertes fraude</div></div>
    </div>
  </div>

  <div class="div"></div>

  <!-- Endpoints -->
  <div class="section">
    <div class="sh">
      <div class="sk">Documentation</div>
      <div class="st">Endpoints disponibles</div>
    </div>
    <div class="ep-grid">
      <a href="/stream" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/stream</div><div class="ep-desc">SSE — flux continu de transactions + KPIs bancaires</div><span class="ep-tag">streaming</span></div></a>
      <a href="/snapshot" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/snapshot</div><div class="ep-desc">Snapshot JSON instantané — batch de transactions</div></div></a>
      <a href="/transactions" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/transactions?n=50</div><div class="ep-desc">Dernières N transactions avec tous les détails</div></div></a>
      <a href="/kpis" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/kpis</div><div class="ep-desc">KPIs agrégés courants — taux approbation, fraude, volume</div></div></a>
      <a href="/kpis/history" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/kpis/history</div><div class="ep-desc">Historique des snapshots KPI</div></div></a>
      <a href="/fraud/alerts" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/fraud/alerts</div><div class="ep-desc">Alertes fraude récentes avec pattern et score</div></div></a>
      <a href="/portfolio/summary" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/portfolio/summary</div><div class="ep-desc">Résumé global du portefeuille depuis démarrage</div></div></a>
      <a href="/topics" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/topics</div><div class="ep-desc">Topics disponibles, catégories et patterns fraude</div></div></a>
      <a href="/health" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/health</div><div class="ep-desc">Status du serveur, ticks et volume total</div></div></a>
      <a href="/docs" class="ep"><span class="ep-m">GET</span><div class="ep-body"><div class="ep-path">/docs</div><div class="ep-desc">Swagger UI — documentation interactive complète</div><span class="ep-tag">interactive</span></div></a>
    </div>
  </div>

  <div class="div"></div>

  <!-- Infos -->
  <div class="section">
    <div class="sh"><div class="sk">Référentiels</div><div class="st">Données générées</div></div>
    <div class="info-grid">
      <div class="info">
        <div class="info-title">🏦 Banques canadiennes</div>
        <div class="info-body">RBC, TD, BMO, Scotia, CIBC, Desjardins, Laurentienne, National Bank</div>
      </div>
      <div class="info">
        <div class="info-title">🚨 Patterns de fraude</div>
        <div class="info-body">VELOCITY, GEO_ANOMALY, AMOUNT_SPIKE, NEW_MERCHANT, NIGHT_TXN, CARD_TEST</div>
      </div>
      <div class="info">
        <div class="info-title">🛍️ Catégories marchandes</div>
        <div class="info-body">Épicerie, Restaurant, Transport, E-commerce, Pharmacie, Carburant, Virement, ATM, Luxe, Voyage, Santé, Abonnement</div>
      </div>
      <div class="info">
        <div class="info-title">💳 Canaux</div>
        <div class="info-body">ONLINE, POS, ATM, MOBILE, CONTACTLESS</div>
      </div>
      <div class="info">
        <div class="info-title">💱 Devises</div>
        <div class="info-body">CAD (72%), USD (18%), EUR (6%), GBP (2%), CHF, JPY</div>
      </div>
      <div class="info">
        <div class="info-title">⚡ Exemple SSE</div>
        <div class="info-body">Connectez-vous au flux en temps réel :</div>
        <div class="code">curl http://localhost:8000/stream</div>
      </div>
    </div>
  </div>
</div>

<footer>
  <div>
    <div class="ft"><span>Abdouramane</span>'s API — BankGen Platform</div>
    <div class="fc">YoussoufDS · Données synthétiques bancaires · FastAPI</div>
  </div>
  <div class="fstack">
    <span class="ftag">Python 3.12</span><span class="ftag">FastAPI</span>
    <span class="ftag">SSE</span><span class="ftag">Docker</span><span class="ftag">Snowflake</span>
  </div>
</footer>

<script>
async function refresh(){
  try{
    const r=await fetch('/portfolio/summary',{signal:AbortSignal.timeout(2000)});
    if(!r.ok) return;
    const d=await r.json();
    const fmt=v=>v>=1e6?'$'+(v/1e6).toFixed(2)+'M':v>=1e3?'$'+(v/1e3).toFixed(1)+'k':'$'+Math.round(v);
    document.getElementById('k-txn').textContent=(d.total_transactions||0).toLocaleString();
    document.getElementById('k-vol').textContent=fmt(d.total_volume_cad||0);
    document.getElementById('k-tick').textContent=(d.uptime_ticks||0).toLocaleString();
    document.getElementById('k-frd').textContent=(d.total_fraud_count||0).toLocaleString();
  }catch{}
}
refresh();setInterval(refresh,2000);
</script>
</body>
</html>"""

app = FastAPI(
    title="Abdouramane's BankGen API",
    version="2.0.0",
    description="Générateur de données bancaires synthétiques — YoussoufDS",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Référentiels ──────────────────────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    ("Épicerie",        0.22, 45,   180),
    ("Restaurant",      0.14, 15,   95),
    ("Transport",       0.10, 8,    65),
    ("E-commerce",      0.12, 25,   350),
    ("Pharmacie",       0.07, 12,   80),
    ("Carburant",       0.08, 40,   120),
    ("Virement",        0.09, 100,  5000),
    ("Abonnement",      0.05, 8,    50),
    ("Retrait ATM",     0.06, 60,   500),
    ("Luxe/Bijouterie", 0.02, 200,  3000),
    ("Voyage",          0.03, 150,  2500),
    ("Santé",           0.02, 20,   400),
]
BANKS       = ["RBC","TD","BMO","Scotia","CIBC","Desjardins","Laurentienne","National"]
CURRENCIES  = [("CAD",.72),("USD",.18),("EUR",.06),("GBP",.02),("CHF",.01),("JPY",.01)]
ACCOUNT_TYPES = ["CHEQUING","SAVINGS","CREDIT","INVESTMENT","RRSP","TFSA"]
PROVINCES   = ["QC","ON","BC","AB","MB","SK","NS","NB"]
FRAUD_PATTERNS = ["VELOCITY","GEO_ANOMALY","AMOUNT_SPIKE","NEW_MERCHANT","NIGHT_TXN","CARD_TEST"]
CHANNELS    = ["ONLINE","POS","ATM","MOBILE","CONTACTLESS"]
ACCOUNT_IDS  = [f"ACC{str(i).zfill(6)}" for i in range(1,2001)]
MERCHANT_IDS = [f"MRC{str(i).zfill(5)}" for i in range(1,501)]

HISTORY_SIZE = 500
_history = {
    "transactions": deque(maxlen=HISTORY_SIZE),
    "fraud_alerts": deque(maxlen=200),
    "kpis":         deque(maxlen=HISTORY_SIZE),
}
_state = {
    "tick":0,"total_txn":0,"total_volume":0.0,"total_fraud":0,
    "fraud_volume":0.0,"active_accounts":set(),"hour_volume":0.0,
    "hour_txn_count":0,"avg_txn_amount":0.0,"rolling_amounts":deque(maxlen=100),
    "decline_count":0,"approval_count":0,"chargeback_count":0,
}

def _pick(items, weights):
    r,cum = random.random(),0
    for item,w in zip(items,weights):
        cum+=w
        if r<=cum: return item
    return items[-1]

def _fraud_score(amount,category,acct_type,hour_factor):
    s=0.0
    if amount>1000: s+=.25
    if amount>3000: s+=.20
    if category in("Luxe/Bijouterie","Voyage","Virement"): s+=.15
    if acct_type=="CREDIT": s+=.05
    if hour_factor<.35: s+=.18
    s+=random.gauss(.05,.15)
    return max(0.,min(1.,s))

def _generate_transaction():
    t=_state["tick"]
    now=datetime.now(timezone.utc).isoformat()
    cat_name,_,amt_min,amt_max=_pick(MERCHANT_CATEGORIES,[c[1] for c in MERCHANT_CATEGORIES])
    amount=round(min(amt_max,max(amt_min,random.lognormvariate(math.log((amt_min+amt_max)/2),.5))),2)
    currency,_=_pick(CURRENCIES,[c[1] for c in CURRENCIES])
    account_id=random.choice(ACCOUNT_IDS)
    merchant_id=random.choice(MERCHANT_IDS)
    account_type=random.choice(ACCOUNT_TYPES)
    bank=random.choice(BANKS)
    province=random.choice(PROVINCES)
    hour_factor=0.3+0.7*abs(math.sin(math.pi*(t%1440)/1440))
    status="DECLINED" if random.random()<(0.04+(0.02 if amount>500 else 0)) else "APPROVED"
    fraud_score=_fraud_score(amount,cat_name,account_type,hour_factor)
    is_fraud=fraud_score>0.72
    fraud_pattern=random.choice(FRAUD_PATTERNS) if is_fraud else None
    is_chargeback=(not is_fraud) and (random.random()<.005)
    txn={
        "txn_id":str(uuid.uuid4()),"timestamp":now,
        "account_id":account_id,"account_type":account_type,
        "merchant_id":merchant_id,"merchant_category":cat_name,
        "amount":amount,"currency":currency,"status":status,
        "bank":bank,"province":province,"is_fraud":is_fraud,
        "fraud_score":round(fraud_score,4),"fraud_pattern":fraud_pattern,
        "is_chargeback":is_chargeback,
        "channel":random.choice(CHANNELS),
        "processing_ms":round(random.lognormvariate(4.5,.6),1),
    }
    _state["total_txn"]+=1
    _state["active_accounts"].add(account_id)
    if status=="APPROVED":
        _state["approval_count"]+=1; _state["total_volume"]+=amount
        _state["hour_volume"]+=amount; _state["hour_txn_count"]+=1
        _state["rolling_amounts"].append(amount)
    else:
        _state["decline_count"]+=1
    if is_fraud:
        _state["total_fraud"]+=1; _state["fraud_volume"]+=amount
        _history["fraud_alerts"].append({"ts":now,"txn_id":txn["txn_id"],
            "account":account_id,"amount":amount,"pattern":fraud_pattern,"score":fraud_score})
    if is_chargeback: _state["chargeback_count"]+=1
    _history["transactions"].append(txn)
    return txn

def _generate_kpis():
    now=datetime.now(timezone.utc).isoformat()
    total=_state["total_txn"]
    approved=_state["approval_count"]; declined=_state["decline_count"]; fraud=_state["total_fraud"]
    rolling=list(_state["rolling_amounts"])
    avg_amount=round(sum(rolling)/max(1,len(rolling)),2)
    _state["avg_txn_amount"]=avg_amount
    kpi={
        "timestamp":now,"tick":_state["tick"],
        "total_transactions":total,"total_volume_cad":round(_state["total_volume"],2),
        "approval_rate_pct":round((approved/max(1,total))*100,2),
        "decline_rate_pct":round((declined/max(1,total))*100,2),
        "fraud_rate_pct":round((fraud/max(1,total))*100,4),
        "fraud_count":fraud,"fraud_volume_cad":round(_state["fraud_volume"],2),
        "avg_txn_amount":avg_amount,"active_accounts":len(_state["active_accounts"]),
        "chargeback_count":_state["chargeback_count"],
        "txn_per_tick":_state["hour_txn_count"],
        "volume_per_tick":round(_state["hour_volume"],2),
    }
    _state["hour_txn_count"]=0; _state["hour_volume"]=0.0
    _history["kpis"].append(kpi)
    return kpi

def _batch_tick(batch_size=5):
    _state["tick"]+=1
    txns=[_generate_transaction() for _ in range(batch_size)]
    kpis=_generate_kpis()
    return {"type":"tick","tick":_state["tick"],"transactions":txns,"kpis":kpis}

async def _stream_gen(interval=1.0, batch=5) -> AsyncGenerator[str,None]:
    while True:
        data=_batch_tick(batch)
        yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(interval)

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def home():
    return HTMLResponse(content=HOME_HTML)

@app.get("/stream")
async def stream(interval: float=1.0, batch: int=Query(5,ge=1,le=50)):
    return StreamingResponse(_stream_gen(interval,batch),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.get("/snapshot")
def snapshot(batch: int=Query(5,ge=1,le=50)):
    return _batch_tick(batch)

@app.get("/transactions")
def get_transactions(n: int=Query(50,ge=1,le=500)):
    return {"count":len(list(_history["transactions"])[-n:]),
            "transactions":list(_history["transactions"])[-n:]}

@app.get("/kpis")
def get_kpis():
    return _history["kpis"][-1] if _history["kpis"] else _generate_kpis()

@app.get("/kpis/history")
def kpis_history(n: int=Query(100,ge=1,le=500)):
    return {"count":len(_history["kpis"]),"data":list(_history["kpis"])[-n:]}

@app.get("/fraud/alerts")
def fraud_alerts(n: int=Query(20,ge=1,le=200)):
    alerts=list(_history["fraud_alerts"])[-n:]
    return {"count":len(alerts),"alerts":alerts}

@app.get("/portfolio/summary")
def portfolio_summary():
    total=_state["total_txn"]
    return {
        "total_transactions":total,
        "total_volume_cad":round(_state["total_volume"],2),
        "total_fraud_count":_state["total_fraud"],
        "total_fraud_volume":round(_state["fraud_volume"],2),
        "fraud_rate_pct":round((_state["total_fraud"]/max(1,total))*100,4),
        "approval_rate_pct":round((_state["approval_count"]/max(1,total))*100,2),
        "chargeback_count":_state["chargeback_count"],
        "unique_accounts":len(_state["active_accounts"]),
        "avg_txn_amount":_state["avg_txn_amount"],
        "uptime_ticks":_state["tick"],
    }

@app.get("/topics")
def topics():
    return {"topics":{k:len(v) for k,v in _history.items()},
            "merchant_categories":[c[0] for c in MERCHANT_CATEGORIES],
            "currencies":[c[0] for c in CURRENCIES],
            "fraud_patterns":FRAUD_PATTERNS}

@app.get("/health")
def health():
    return {"status":"ok","tick":_state["tick"],
            "total_transactions":_state["total_txn"],
            "total_volume_cad":round(_state["total_volume"],2)}
