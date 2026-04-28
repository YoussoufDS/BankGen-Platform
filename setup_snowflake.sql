-- ============================================================
-- BankGen — Setup Snowflake
-- Exécuter une seule fois dans votre Snowflake Worksheet
-- ============================================================

-- 1. Base & schéma
CREATE DATABASE IF NOT EXISTS BANKGEN_DB;
USE DATABASE BANKGEN_DB;

CREATE SCHEMA IF NOT EXISTS BANKING;
USE SCHEMA BANKING;

-- ============================================================
-- 2. Table principale — Transactions
-- ============================================================
CREATE TABLE IF NOT EXISTS TRANSACTIONS (
    TXN_ID              VARCHAR(36)     NOT NULL PRIMARY KEY,
    CREATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    TXN_TIMESTAMP       TIMESTAMP_NTZ   NOT NULL,
    ACCOUNT_ID          VARCHAR(12)     NOT NULL,
    ACCOUNT_TYPE        VARCHAR(20),
    MERCHANT_ID         VARCHAR(10),
    MERCHANT_CATEGORY   VARCHAR(50),
    AMOUNT              FLOAT           NOT NULL,
    CURRENCY            VARCHAR(3)      DEFAULT 'CAD',
    STATUS              VARCHAR(10),        -- APPROVED / DECLINED
    BANK                VARCHAR(20),
    PROVINCE            VARCHAR(5),
    IS_FRAUD            BOOLEAN         DEFAULT FALSE,
    FRAUD_SCORE         FLOAT,
    FRAUD_PATTERN       VARCHAR(30),
    IS_CHARGEBACK       BOOLEAN         DEFAULT FALSE,
    CHANNEL             VARCHAR(20),        -- ONLINE / POS / ATM / MOBILE / CONTACTLESS
    PROCESSING_MS       FLOAT
);

-- ============================================================
-- 3. Table KPIs — agrégats par tick
-- ============================================================
CREATE TABLE IF NOT EXISTS KPI_SNAPSHOTS (
    SNAPSHOT_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    CREATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    KPI_TIMESTAMP       TIMESTAMP_NTZ   NOT NULL,
    TICK                NUMBER,
    TOTAL_TRANSACTIONS  NUMBER,
    TOTAL_VOLUME_CAD    FLOAT,
    APPROVAL_RATE_PCT   FLOAT,
    DECLINE_RATE_PCT    FLOAT,
    FRAUD_RATE_PCT      FLOAT,
    FRAUD_COUNT         NUMBER,
    FRAUD_VOLUME_CAD    FLOAT,
    AVG_TXN_AMOUNT      FLOAT,
    ACTIVE_ACCOUNTS     NUMBER,
    CHARGEBACK_COUNT    NUMBER,
    TXN_PER_TICK        NUMBER,
    VOLUME_PER_TICK     FLOAT
);

-- ============================================================
-- 4. Table Alertes Fraude
-- ============================================================
CREATE TABLE IF NOT EXISTS FRAUD_ALERTS (
    ALERT_ID            NUMBER AUTOINCREMENT PRIMARY KEY,
    CREATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ALERT_TIMESTAMP     TIMESTAMP_NTZ   NOT NULL,
    TXN_ID              VARCHAR(36),
    ACCOUNT_ID          VARCHAR(12),
    AMOUNT              FLOAT,
    FRAUD_PATTERN       VARCHAR(30),
    FRAUD_SCORE         FLOAT
);

-- ============================================================
-- 5. Vues analytiques utiles
-- ============================================================

-- Volume par catégorie marchande
CREATE OR REPLACE VIEW V_VOLUME_BY_CATEGORY AS
SELECT
    MERCHANT_CATEGORY,
    COUNT(*)                            AS TXN_COUNT,
    SUM(AMOUNT)                         AS TOTAL_VOLUME,
    AVG(AMOUNT)                         AS AVG_AMOUNT,
    SUM(CASE WHEN IS_FRAUD THEN 1 END)  AS FRAUD_COUNT,
    SUM(CASE WHEN IS_FRAUD THEN AMOUNT END) AS FRAUD_VOLUME
FROM TRANSACTIONS
WHERE STATUS = 'APPROVED'
GROUP BY 1
ORDER BY TOTAL_VOLUME DESC;

-- Taux d'approbation par banque
CREATE OR REPLACE VIEW V_APPROVAL_BY_BANK AS
SELECT
    BANK,
    COUNT(*)                                        AS TOTAL,
    SUM(CASE WHEN STATUS='APPROVED' THEN 1 END)    AS APPROVED,
    SUM(CASE WHEN STATUS='DECLINED' THEN 1 END)    AS DECLINED,
    ROUND(SUM(CASE WHEN STATUS='APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) AS APPROVAL_RATE_PCT
FROM TRANSACTIONS
GROUP BY 1
ORDER BY TOTAL DESC;

-- Fraude par pattern
CREATE OR REPLACE VIEW V_FRAUD_BY_PATTERN AS
SELECT
    FRAUD_PATTERN,
    COUNT(*)        AS ALERT_COUNT,
    AVG(AMOUNT)     AS AVG_AMOUNT,
    SUM(AMOUNT)     AS TOTAL_VOLUME,
    AVG(FRAUD_SCORE) AS AVG_SCORE
FROM TRANSACTIONS
WHERE IS_FRAUD = TRUE
GROUP BY 1
ORDER BY ALERT_COUNT DESC;

-- Transactions par province
CREATE OR REPLACE VIEW V_TXN_BY_PROVINCE AS
SELECT
    PROVINCE,
    COUNT(*)                            AS TXN_COUNT,
    SUM(AMOUNT)                         AS TOTAL_VOLUME,
    AVG(AMOUNT)                         AS AVG_AMOUNT,
    SUM(CASE WHEN IS_FRAUD THEN 1 END)  AS FRAUD_COUNT
FROM TRANSACTIONS
WHERE STATUS = 'APPROVED'
GROUP BY 1
ORDER BY TXN_COUNT DESC;

-- KPIs dernière heure (glissant)
CREATE OR REPLACE VIEW V_KPI_LAST_HOUR AS
SELECT *
FROM KPI_SNAPSHOTS
WHERE KPI_TIMESTAMP >= DATEADD('minute', -60, CURRENT_TIMESTAMP())
ORDER BY KPI_TIMESTAMP DESC;

-- ============================================================
-- 6. Rôle & droits (optionnel — si besoin de sécurité)
-- ============================================================
-- GRANT SELECT ON ALL TABLES IN SCHEMA BANKGEN_DB.BANKING TO ROLE ANALYST;
-- GRANT INSERT ON TABLE BANKGEN_DB.BANKING.TRANSACTIONS TO ROLE DATAGEN_WRITER;

SELECT 'Setup Snowflake BankGen terminé ✅' AS STATUS;
