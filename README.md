Project Background
---------------------------------------------------------------------------------------------------------------------------------------------------------------
Noon is a large online retail marketplace selling products across six international markets. The company holds significant transactional, customer, and product data that had been underused for decision-making.

This project builds the full analytics pipeline behind that data — from raw files, to a clean modeled data warehouse, to an interactive dashboard — in order to uncover the insights that improve commercial performance.

Insights and recommendations are delivered across four key areas:


- Sales Trends Analysis — historical revenue, order volume, and Average Order Value (AOV), including year-over-year growth and seasonality.
- Product Performance — which products and product families drive revenue and profit, and how concentrated the catalog is.
- Geographic & Currency Performance — sales by country and currency, and the concentration risk that comes with it.
- Customer Insights — customer base composition and segmentation.


Explore the project:


The SQL data warehouse to build and analyze(Bronze / Silver / Gold layers and modeling) → [`scripts`](./scripts) folder

The interactive Power BI report → [![View Dashboard](https://img.shields.io/badge/View-Live%20Dashboard-F2C811?logo=powerbi&logoColor=black)](https://app.powerbi.com/links/s80zKechkY?ctid=2c5ee638-a963-49c0-ac26-828dd9b78d5e&pbi_source=linkShare)

Data Structure & Initial Checks
---------------------------------------------------------------------------------------------------------------------------------------------------------------
The analytical layer is modeled as a star schema: one central fact table surrounded by four dimension tables, for a total of 79,512 records.

<img width="2558" height="1216" alt="Screenshot 2026-06-13 235954" src="https://github.com/user-attachments/assets/cce7898f-e8ec-45f5-bce9-9ec5b9488ac3" />

The data warehouse follows the Medallion Architecture:


- Bronze Layer — raw data ingested as-is from source CSV files into SQL Server.
- Silver Layer — cleansing, standardization, and normalization to make the data analysis-ready.
- Gold Layer — business-ready data modeled into the star schema above, optimized for reporting and analytics.


Before analysis, quality-control checks were run on every table — verifying row counts, key integrity between fact and dimensions, duplicate orders, null handling, and value ranges.

Executive Summary
---------------------------------------------------------------------------------------------------------------------------------------------------------------
**Overview of Findings**

The business generated $29.4M in total revenue from 27,658 orders placed by 18,484 customers, producing $12.1M in gross profit at a healthy 41.1% margin. Revenue is strong but highly concentrated: two countries account for nearly two-thirds of all sales, and growth in the most recent full year came from a clear strategic shift toward higher-volume, lower-value orders. The sections below break down where the money comes from — and where the next opportunities are.

<img width="2036" height="1144" alt="Screenshot 2026-06-14 175442" src="https://github.com/user-attachments/assets/5f83809c-5196-4f94-ad47-773fba2e5e11" />

**Sales Trends:**

- Revenue grew from $7.1M (2011) to $16.4M (2013), with 2013 as the clear breakout year — order volume rose +551% year-over-year.
- However, Average Order Value fell from ~$3,200 (2011) to ~$770 (2013). This signals a deliberate mix shift: the business moved from a small number of high-    value bike orders toward a large volume of lower-value accessory orders — expanding the customer base rather than the basket size.
- Sales are highly seasonal, peaking in December ($3.2M) and November ($3.0M), consistent with holiday demand, with a secondary mid-year lift in June ($2.9M).
- 
<img width="2044" height="1138" alt="Screenshot 2026-06-14 183304" src="https://github.com/user-attachments/assets/a46a0e87-ac4f-474c-bbd3-281b870ec54b" />

**Product Performance:**

- The catalog is less concentrated than it looks — the top 10 products make up only 42.5% of revenue, meaning no single product carries the business.
- Revenue is dominated by two product families: Mountain-200 and Road-150 bikes, which fill every one of the top 10 revenue spots.
- The single best-selling product, Mountain-200 Black (46), generated $1.37M (4.7% of revenue).


**Geographic & Currency Performance:**

- Revenue is heavily concentrated in two markets: the United States (32%, $9.4M) and Australia (31%, $9.1M) together drive 63% of total revenue.
- The remaining four markets — UK (11.5%), Germany (9.9%), France (9.0%), and Canada (6.7%) — represent a meaningful but underdeveloped growth base.
- By currency, US Dollar (50%) and Australian Dollar (31%) mirror the country concentration, while the Euro-zone currencies (Deutsche Mark, French Franc) together account for under 1.5%, suggesting unrealized potential in those markets.

<img width="1650" height="530" alt="Screenshot 2026-06-13 232524" src="https://github.com/user-attachments/assets/55711197-d99a-4548-bb7b-f00420058bfc" />

Recommendations
-
Based on the insights above, the following actions are recommended:


- Reduce geographic concentration risk. With 63% of revenue from just two countries, invest marketing and logistics in the underpenetrated European markets (Germany, France), where currency share is under 1.5% despite established demand.
- Lean into the high-volume accessory strategy. The 2013 shift toward smaller, more frequent orders grew the customer base dramatically. Double down with cross-sell and bundle offers that increase order frequency, while testing upsells to lift the now-lower AOV.
- Protect and grow the core bike families. Mountain-200 and Road-150 carry the top of the revenue chart. Ensure inventory and promotion priority for these families, and analyze their profit margin (cost data is available) to confirm they are also the most profitable, not just the highest-grossing.
- Capitalize on seasonality. With clear November–December peaks, concentrate marketing spend and stock ahead of Q4, and build a mid-year (June) campaign to reinforce the secondary peak.
- Activate customer segmentation. Use the demographic and segment data to run targeted campaigns — re-engaging high-income segments with premium products and nurturing lower-value, high-frequency buyers toward repeat purchases.
