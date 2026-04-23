# DuckAI SQL Analysis

This is a SQL case study where I analyze user behavior and marketing performance.

The project is based on a synthetic dataset (~6 months of data) and focuses on how users move through the product funnel and how efficiently they are acquired.

## What I did

- created tables and loaded data into PostgreSQL  
- checked data quality (duplicates, missing data, date ranges)  
- calculated funnel metrics (signup to paid)  
- analyzed channel performance and CAC  
- explored time to conversion  
- used window functions to understand user event sequences  

## Tech

- PostgreSQL  
- SQL (CTE, aggregation, window functions)

## Notes

In this analysis, I calculate paid users using the `subscription_started` event from event data.

There is also a separate subscription table, but I didn’t use it here because the focus is on funnel analysis and user behavior.
