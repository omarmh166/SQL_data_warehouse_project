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


Bronze Layer — raw data ingested as-is from source CSV files into SQL Server.
Silver Layer — cleansing, standardization, and normalization to make the data analysis-ready.
Gold Layer — business-ready data modeled into the star schema above, optimized for reporting and analytics.


Before analysis, quality-control checks were run on every table — verifying row counts, key integrity between fact and dimensions, duplicate orders, null handling, and value ranges.

Executive Summary
---------------------------------------------------------------------------------------------------------------------------------------------------------------
Overview of Findings

The business generated $29.4M in total revenue from 27,658 orders placed by 18,484 customers, producing $12.1M in gross profit at a healthy 41.1% margin. Revenue is strong but highly concentrated: two countries account for nearly two-thirds of all sales, and growth in the most recent full year came from a clear strategic shift toward higher-volume, lower-value orders. The sections below break down where the money comes from — and where the next opportunities are.
<img width="2036" height="1144" alt="Screenshot 2026-06-14 175442" src="https://github.com/user-attachments/assets/5f83809c-5196-4f94-ad47-773fba2e5e11" />
Sales Trends:

-Revenue grew from $7.1M (2011) to $16.4M (2013), with 2013 as the clear breakout year — order volume rose +551% year-over-year.
-However, Average Order Value fell from ~$3,200 (2011) to ~$770 (2013). This signals a deliberate mix shift: the business moved from a small number of high-value bike orders toward a large volume of lower-value accessory orders — expanding the customer base rather than the basket size.
-Sales are highly seasonal, peaking in December ($3.2M) and November ($3.0M), consistent with holiday demand, with a secondary mid-year lift in June ($2.9M).
