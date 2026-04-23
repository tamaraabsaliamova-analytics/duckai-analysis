-- ============================================
-- STEP 1. RESET DATABASE OBJECTS + CREATE TABLES
-- DuckAI SQL Case
-- ============================================

-- The data is organized in a star-schema-like structure.
-- Dimension tables describe users, campaigns, channels, countries and dates,
-- while fact tables store events, marketing data and subscriptions.
-- This makes it easier to analyze acquisition, user behavior and monetization.

-- Remove old project tables if they already exist
DROP TABLE IF EXISTS fact_events;
DROP TABLE IF EXISTS fact_marketing_daily;
DROP TABLE IF EXISTS fact_subscriptions;
DROP TABLE IF EXISTS dim_user;
DROP TABLE IF EXISTS dim_campaign;
DROP TABLE IF EXISTS dim_channel;
DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_date;


-- ----------------------------
-- Dimension tables
-- ----------------------------

CREATE TABLE dim_country (
    countrycode TEXT,
    country TEXT,
    region TEXT
);

CREATE TABLE dim_date (
    date DATE,
    year INT,
    month INT,
    monthname TEXT,
    quarter TEXT,
    week INT
);

CREATE TABLE dim_channel (
    channel TEXT,
    subchannel TEXT
);

CREATE TABLE dim_campaign (
    campaignid TEXT,
    channel TEXT,
    subchannel TEXT,
    campaignname TEXT
);

CREATE TABLE dim_user (
    userid TEXT,
    signuptimestamp TIMESTAMP,
    signupdate DATE,
    countrycode TEXT,
    acqchannel TEXT,
    acqsubchannel TEXT,
    campaignid TEXT,
    segment TEXT,
    device TEXT
);

-- ----------------------------
-- Fact tables
-- ----------------------------

CREATE TABLE fact_events (
    eventid TEXT,
    userid TEXT,
    eventtimestamp TIMESTAMP,
    eventdate DATE,
    eventname TEXT,
    sessionid TEXT,
    countrycode TEXT,
    acqchannel TEXT,
    acqsubchannel TEXT,
    campaignid TEXT,
    valueeur NUMERIC(10,2),
    meta TEXT
);

CREATE TABLE fact_marketing_daily (
    date DATE,
    campaignid TEXT,
    countrycode TEXT,
    channel TEXT,
    subchannel TEXT,
    impressions INT,
    clicks NUMERIC(12,2),
    spendeur NUMERIC(12,2),
    cpc_eur NUMERIC(12,2),
    signups NUMERIC(12,2),
    paidusers NUMERIC(12,2)
);

CREATE TABLE fact_subscriptions (
    subscriptionid TEXT,
    userid TEXT,
    startdate DATE,
    starttimestamp TIMESTAMP,
    plan TEXT,
    monthlyfeeeur NUMERIC(10,2),
    status TEXT,
    canceldate DATE,
    canceltimestamp TIMESTAMP
);

-- ============================================
-- STEP 2. LOAD CSV FILES
-- DuckAI SQL Case
-- ============================================
-- Loading data from CSV files prepared for this case.
-- Each file corresponds to one table in the model.

COPY dim_country
FROM 'C:\SQLdata\Dim_Country.csv'
DELIMITER ','
CSV HEADER;

COPY dim_date
FROM 'C:\SQLdata\Dim_Date.csv'
DELIMITER ','
CSV HEADER;

COPY dim_channel
FROM 'C:\SQLdata\Dim_Channel.csv'
DELIMITER ','
CSV HEADER;

COPY dim_campaign
FROM 'C:\SQLdata\Dim_Campaign.csv'
DELIMITER ','
CSV HEADER;

COPY dim_user
FROM 'C:\SQLdata\Dim_User.csv'
DELIMITER ','
CSV HEADER;

COPY fact_events
FROM 'C:\SQLdata\Fact_Events.csv'
DELIMITER ','
CSV HEADER;

COPY fact_marketing_daily
FROM 'C:\SQLdata\Fact_Marketing_Daily.csv'
DELIMITER ','
CSV HEADER;

COPY fact_subscriptions
FROM 'C:\SQLdata\Fact_Subscriptions.csv'
DELIMITER ','
CSV HEADER;

-- ============================================
-- STEP 3. DATA QUALITY CHECKS
-- DuckAI SQL Case
-- ============================================

-- ----------------------------
-- 3.1 Row counts by table
-- ----------------------------
SELECT 'dim_country' AS table_name, COUNT(*) AS row_count FROM dim_country
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_channel', COUNT(*) FROM dim_channel
UNION ALL
SELECT 'dim_campaign', COUNT(*) FROM dim_campaign
UNION ALL
SELECT 'dim_user', COUNT(*) FROM dim_user
UNION ALL
SELECT 'fact_events', COUNT(*) FROM fact_events
UNION ALL
SELECT 'fact_marketing_daily', COUNT(*) FROM fact_marketing_daily
UNION ALL
SELECT 'fact_subscriptions', COUNT(*) FROM fact_subscriptions
ORDER BY table_name;

-- ----------------------------
-- 3.2 Duplicate checks
-- ----------------------------
SELECT
    COUNT(*) AS total_users,
    COUNT(DISTINCT userid) AS unique_users
FROM dim_user;

SELECT
    COUNT(*) AS total_event_rows,
    COUNT(DISTINCT eventid) AS unique_event_ids
FROM fact_events;

SELECT
    COUNT(*) AS total_subscriptions,
    COUNT(DISTINCT subscriptionid) AS unique_subscription_ids
FROM fact_subscriptions;

-- ----------------------------
-- 3.3 Date range checks
-- ----------------------------
SELECT
    MIN(signupdate) AS first_signup_date,
    MAX(signupdate) AS last_signup_date
FROM dim_user;

SELECT
    MIN(eventdate) AS first_event_date,
    MAX(eventdate) AS last_event_date
FROM fact_events;

SELECT
    MIN(date) AS first_marketing_date,
    MAX(date) AS last_marketing_date
FROM fact_marketing_daily;

SELECT
    MIN(date) AS first_dim_date,
    MAX(date) AS last_dim_date
FROM dim_date;

-- Note: dim_date ends on 2026-01-31, while some marketing data extends beyond this range.
-- This should be considered when interpreting time-based results across tables.

-- ----------------------------
-- 3.4 Referential integrity checks
-- ----------------------------
SELECT
    COUNT(*) AS users_with_missing_campaign
FROM dim_user u
LEFT JOIN dim_campaign c
    ON u.campaignid = c.campaignid
WHERE c.campaignid IS NULL;

SELECT
    COUNT(*) AS marketing_rows_with_missing_campaign
FROM fact_marketing_daily m
LEFT JOIN dim_campaign c
    ON m.campaignid = c.campaignid
WHERE c.campaignid IS NULL;

SELECT
    COUNT(*) AS users_with_missing_country
FROM dim_user u
LEFT JOIN dim_country c
    ON u.countrycode = c.countrycode
WHERE c.countrycode IS NULL;

SELECT
    COUNT(*) AS subscriptions_with_missing_user
FROM fact_subscriptions s
LEFT JOIN dim_user u
    ON s.userid = u.userid
WHERE u.userid IS NULL;

-- ----------------------------
-- 3.5 Event distribution
-- ----------------------------
SELECT
    eventname,
    COUNT(*) AS event_count,
    COUNT(DISTINCT userid) AS users_count
FROM fact_events
GROUP BY eventname
ORDER BY event_count DESC;

-- ============================================
-- STEP 4. CORE FUNNEL ANALYSIS
-- ============================================
-- Funnel analysis shows how users move through the product:
-- signup → activation → first value → paid.
-- All metrics are calculated on a user level (DISTINCT users).

-- Count users at each funnel stage
WITH funnel AS (
    SELECT
        eventname,
        COUNT(DISTINCT userid) AS users
    FROM fact_events
    WHERE eventname IN (
        'signup',
        'ai_activated',
        'first_value_reached',
        'subscription_started'
    )
    GROUP BY eventname
),

-- Pivot funnel into one row
pivoted AS (
    SELECT
        MAX(CASE WHEN eventname = 'signup' THEN users END) AS signup_users,
        MAX(CASE WHEN eventname = 'ai_activated' THEN users END) AS activated_users,
        MAX(CASE WHEN eventname = 'first_value_reached' THEN users END) AS first_value_users,
        MAX(CASE WHEN eventname = 'subscription_started' THEN users END) AS paid_users
    FROM funnel
)

-- Final metrics
SELECT
    signup_users,
    activated_users,
    first_value_users,
    paid_users,

    ROUND(activated_users * 100.0 / NULLIF(signup_users, 0), 1) AS activation_rate_pct,
    ROUND(first_value_users * 100.0 / NULLIF(activated_users, 0), 1) AS first_value_rate_pct,
    ROUND(paid_users * 100.0 / NULLIF(first_value_users, 0), 1) AS paid_from_value_pct,
    ROUND(paid_users * 100.0 / NULLIF(signup_users, 0), 1) AS paid_from_signup_pct

FROM pivoted;

-- ============================================
-- STEP 5. CHANNEL PERFORMANCE
-- ============================================
-- Here I compare channels from two sides:
-- how users convert through the funnel and how much they cost (CAC).

-- ----------------------------
-- 5.1 Funnel metrics by channel
-- ----------------------------
WITH funnel_by_channel AS (
    SELECT
        acqchannel AS channel,
        eventname,
        COUNT(DISTINCT userid) AS users
    FROM fact_events
    WHERE eventname IN (
        'signup',
        'ai_activated',
        'first_value_reached',
        'subscription_started'
    )
    GROUP BY acqchannel, eventname
),

pivoted AS (
    SELECT
        channel,
        MAX(CASE WHEN eventname = 'signup' THEN users END) AS signup_users,
        MAX(CASE WHEN eventname = 'ai_activated' THEN users END) AS activated_users,
        MAX(CASE WHEN eventname = 'first_value_reached' THEN users END) AS first_value_users,
        MAX(CASE WHEN eventname = 'subscription_started' THEN users END) AS paid_users
    FROM funnel_by_channel
    GROUP BY channel
)

SELECT
    channel,
    signup_users,
    activated_users,
    first_value_users,
    paid_users,
    ROUND(activated_users * 100.0 / NULLIF(signup_users, 0), 1) AS activation_rate_pct,
    ROUND(first_value_users * 100.0 / NULLIF(activated_users, 0), 1) AS first_value_rate_pct,
    ROUND(paid_users * 100.0 / NULLIF(signup_users, 0), 1) AS paid_conversion_pct
FROM pivoted
ORDER BY paid_conversion_pct DESC;

-- ----------------------------
-- 5.2 Paid conversion and CAC by channel
-- ----------------------------
WITH paid_by_channel AS (
    SELECT
        acqchannel AS channel,
        COUNT(DISTINCT CASE WHEN eventname = 'signup' THEN userid END) AS signup_users,
        COUNT(DISTINCT CASE WHEN eventname = 'subscription_started' THEN userid END) AS paid_users
    FROM fact_events
    GROUP BY acqchannel
),

spend_by_channel AS (
    SELECT
        channel,
        SUM(spendeur) AS total_spend
    FROM fact_marketing_daily
    GROUP BY channel
)

SELECT
    p.channel,
    p.signup_users,
    p.paid_users,
    ROUND(p.paid_users * 100.0 / NULLIF(p.signup_users, 0), 1) AS paid_conversion_pct,
    ROUND(s.total_spend, 2) AS total_spend,
    ROUND(s.total_spend / NULLIF(p.paid_users, 0), 2) AS cac_eur
FROM paid_by_channel p
LEFT JOIN spend_by_channel s
    ON p.channel = s.channel
ORDER BY paid_conversion_pct DESC;

-- ============================================
-- STEP 6. ADVANCED ANALYSIS
-- ============================================

-- ----------------------------
-- 6.1 Time to paid
-- ----------------------------
-- This part shows how long it takes users to convert after signup.
-- It helps understand if users pay immediately or after some time.

WITH signup_time AS (
    SELECT
        userid,
        MIN(eventtimestamp) AS signup_ts
    FROM fact_events
    WHERE eventname = 'signup'
    GROUP BY userid
),

paid_time AS (
    SELECT
        userid,
        MIN(eventtimestamp) AS paid_ts
    FROM fact_events
    WHERE eventname = 'subscription_started'
    GROUP BY userid
),

conversion_time AS (
    SELECT
        s.userid,
        EXTRACT(EPOCH FROM (p.paid_ts - s.signup_ts)) / 86400.0 AS days_to_paid
    FROM signup_time s
    JOIN paid_time p
        ON s.userid = p.userid
)

SELECT
    COUNT(*) AS converted_users,
    ROUND(AVG(days_to_paid), 2) AS avg_days_to_paid,
    ROUND(MIN(days_to_paid), 2) AS fastest_days,
    ROUND(MAX(days_to_paid), 2) AS slowest_days
FROM conversion_time;

-- ----------------------------
-- 6.2 User event sequence (sample)
-- ----------------------------
-- This query shows the order of events for each user.
-- ROW_NUMBER() is used to rebuild the event sequence over time.

WITH ordered_events AS (
    SELECT
        userid,
        eventname,
        eventtimestamp,
        ROW_NUMBER() OVER (
            PARTITION BY userid
            ORDER BY eventtimestamp
        ) AS event_order
    FROM fact_events
)
SELECT
    userid,
    eventname,
    eventtimestamp,
    event_order
FROM ordered_events
WHERE event_order <= 5
ORDER BY userid, eventtimestamp;