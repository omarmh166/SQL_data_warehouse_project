/*
===================
DDL Script: Create Gold Views
===================
Script Purpose:
   This script create views for the Gold layer in the data warhouse.
   The Goled layer represents the final dimension and fact tables. 

   Each view performs transformations and combines data from the silver layer
   to produce a clean, enriched, and business_ready dataset.

Usage:
   - These views can be queried directly for analytics and reporting.
===================
*/




if OBJECT_ID('gold.dim_customer', 'v') is not NULL
  DROP view gold.dim_customer
go

  
create view gold.dim_customer as
select 
row_number() over(order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
cl.CNTRY as country,
case when ci.cst_gndr != 'n/a' then upper(ci.cst_gndr)
  else upper(coalesce(ca.GEN, 'n/a'))
end as gender,
ci.cst_marital_status as marital_status,
ca.BDATE as birthdate,
ci.cst_create_date as create_date


from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 cl 
on ci.cst_key = cl.cid; 


go


----------------------------
----------------------------



if OBJECT_ID('gold.dim_products', 'v') is not NULL
  DROP view gold.dim_products
go


create view gold.dim_products as 

SELECT 
row_number() over (order by prd_start_dt, prd_id) as product_key,
pi.prd_id as product_id,
    SUBSTRING(pi.prd_key, 7, LEN(pi.prd_key)) AS product_number,
pi.prd_nm as product_name,
pi.cat_id as category_id,
pc.CAT as category,
pc.SUBCAT as subcategory,
pc.MAINTENANCE,
pi.prd_cost as cost,
pi.prd_line as product_line,
pi.prd_start_dt as start_date

from silver.crm_prd_info pi
left join silver.erp_px_cat_g1v2 pc
on pi.cat_id = pc.ID

where prd_end_dt is NULL;   --filter out all historical data--

go





-------------------
-------------------



if OBJECT_ID('gold.fact_sales', 'v') is not NULL
  DROP view gold.fact_sales
go



create view gold.fact_sales as
select
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id = cu.customer_id

go


