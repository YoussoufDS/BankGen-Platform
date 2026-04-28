# 🏦 BankGen Platform — Real-Time Banking Data Generator

> **Abdouramane Youssouf (YoussoufDS)** · Master BI & Analytics · HEC Montréal

Plateforme complète de génération de données bancaires synthétiques en temps réel, avec persistance Snowflake et double dashboard analytique.

---

## 🏗️ Architecture

```
FastAPI (BankGen API)
    ↓ SSE Streaming
Streamlit Docker Dashboard ──→ Snowflake (WALRUS_DB.BANKING)
                                    ↑
                          Streamlit Snowflake App
```

## 🚀 Stack technique

| Composant | Technologie |
|-----------|-------------|
| Générateur de données | Python · FastAPI · SSE |
| Dashboard temps réel | Streamlit · Plotly |
| Persistance | Snowflake (Key Pair Auth) |
| Dashboard analytique | Snowflake Streamlit App |
| Infrastructure | Docker · Docker Compose |
| Auth Snowflake | RSA Key Pair (PKCS8) |

---

## 📊 Données générées

- **12 catégories marchandes** : Épicerie, Restaurant, E-commerce, Voyage, Luxe...
- **8 banques canadiennes** : RBC, TD, BMO, Scotia, CIBC, Desjardins...
- **6 patterns de fraude** : VELOCITY, GEO_ANOMALY, AMOUNT_SPIKE, NEW_MERCHANT, NIGHT_TXN, CARD_TEST
- **5 canaux** : ONLINE, POS, ATM, MOBILE, CONTACTLESS
- **6 devises** : CAD, USD, EUR, GBP, CHF, JPY
- **Score de fraude** calculé en temps réel (montant, heure, catégorie, type compte)

---

## ⚡ Démarrage rapide

### Prérequis
- Docker Desktop
- Compte Snowflake

### 1. Cloner le repo
```bash
git clone https://github.com/YoussoufDS/bankgen-platform.git
cd bankgen-platform
```

### 2. Configurer Snowflake
```bash
# Générer les clés RSA
openssl genrsa -out rsa_key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in rsa_key.pem -out rsa_key.p8
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub

# Enregistrer la clé publique dans Snowflake
# ALTER USER WALRUS SET RSA_PUBLIC_KEY='...contenu rsa_key.pub...';
```

### 3. Initialiser Snowflake
```sql
-- Exécuter dans Snowflake Worksheet
-- Voir sql/setup_snowflake.sql
```

### 4. Lancer Docker
```bash
docker compose up --build
```

### 5. Accéder aux services

| Service | URL |
|---------|-----|
| 🏠 Landing Page | http://localhost |
| ⚡ Abdouramane's API | http://localhost:8000 |
| 📄 Swagger UI | http://localhost:8000/docs |
| 📊 Dashboard Live | http://localhost:8501 |

---

## 📁 Structure du projet

```
bankgen-platform/
├── main.py                    ← FastAPI — BankGen API
├── dashboard.py               ← Streamlit Docker (temps réel + Snowflake)
├── dashboard_snowflake.py     ← Streamlit Snowflake App (natif)
├── Dockerfile.api
├── Dockerfile.dashboard
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── landing/
│   └── index.html             ← Page d'accueil
└── sql/
    └── setup_snowflake.sql    ← Setup tables & vues Snowflake
```

---

## 🗄️ Schéma Snowflake

```
WALRUS_DB.BANKING
├── TRANSACTIONS          ← Transactions bancaires
├── KPI_SNAPSHOTS         ← KPIs agrégés par tick
├── FRAUD_ALERTS          ← Alertes fraude
├── V_VOLUME_BY_CATEGORY  ← Vue analytique
├── V_APPROVAL_BY_BANK    ← Vue analytique
├── V_FRAUD_BY_PATTERN    ← Vue analytique
├── V_TXN_BY_PROVINCE     ← Vue analytique
└── V_KPI_LAST_HOUR       ← Vue analytique
```

---

## 🔗 Liens

- **LinkedIn** : [Abdouramane Youssouf](https://www.linkedin.com/in/youssoufds)
- **GitHub** : [YoussoufDS](https://github.com/YoussoufDS)

---

## 📄 Licence

MIT License — libre d'utilisation pour des fins éducatives et personnelles.
