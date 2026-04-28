# DuckAI SQL Analysis

This is a SQL case study where I analyze user behavior and marketing performance.

DuckAI is a synthetic product analytics case. The dataset contains around 6 months of data and was created for portfolio purposes.

The goal of this analysis is to understand how users move through the product funnel and how efficiently they are acquired and converted to paid.

---

## What I did

- created tables and loaded data into PostgreSQL  
- checked data quality (duplicates, missing data, date ranges)  
- calculated funnel metrics (signup to paid)  
- analyzed channel performance and CAC  
- explored time to conversion  
- used window functions to understand user event sequences  

---

## Data

The dataset is stored in the `data/` folder.

It includes:

- user data  
- event-level product behavior  
- marketing performance by day  
- subscription data  
- dimension tables (date, country, channel, campaign)

All data is synthetic and used only for demonstration.

---

## Tech

- PostgreSQL  
- SQL (CTE, aggregations, window functions)

---

## Notes

In this analysis, I calculate paid users using the `subscription_started` event from event data.

There is also a separate subscription table, but I didn’t use it here because the focus is on funnel analysis and user behavior.

---

## Structure

The SQL script is organized step by step:

- data setup and table creation  
- CSV loading  
- data quality checks  
- funnel analysis  
- channel performance  
- advanced analysis (time to paid, event sequences)
