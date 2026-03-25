/*
======================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================
Script Purpose:
    This  strored procedure performs the ETL (Extract, Transform, LOad) process to
     populate the 'silver' schema tables from the 'bronze' schema.
Actions Perforned:
     - Truncates silver tables.
     - Inserts transformed and cleansed data from Bronze into Silver tables.
======================
*/





create or alter PROCEDURE silver.load_silver as
begin


TRUNCATE table silver.crm_cust_info
insert into silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
SELECT
cst_id,
cst_key,
trim(cst_firstname)as cst_firstname,
trim(cst_lastname)as cst_lastname,
case when  UPPER(trim(cst_marital_status)) = 's'then 'single'
     when  UPPER(trim(cst_marital_status)) = 'm'then 'married'
     else 'n/a'
end cst_marital_status,

case when  UPPER(trim(cst_gndr)) = 'f'then 'female'
     when  UPPER(trim(cst_gndr)) = 'm'then 'male' 
     else 'n/a' 
end cst_gndr,     
cst_create_date     

from(
SELECT *,
row_number() OVER (PARTITION by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info)t where flag_last = 1 and cst_id is not null ;                -- remove  NULLS or Duplicates in primary key--

 


------------------------
------------------------


TRUNCATE table silver.crm_prd_info

insert into silver.crm_prd_info ( 
prd_id,
 prd_key,
 cat_id,
 sls_prd_key,
 prd_nm ,
 prd_cost ,
 prd_line ,
 prd_start_dt ,
 prd_end_dt
 )
SELECT
prd_id,
prd_key,
 replace(SUBSTRING (prd_key, 1, 5), '-','_') as cat_id,  -- extract category id
 SUBSTRING(prd_key, 7, len(prd_key)) as sls_prd_key,  -- extract product key
 prd_nm,
isnull(prd_cost, 0) as prd_cost,
case UPPER(TRIM(prd_line)) 
     when 'r' then 'road'
     when 'm' then 'mountaion'
     when 's' then 'other sales'
     when 't' then 'tourting'
     else 'n/a'
end as prd_line ,    -- code to descriptive values
prd_start_dt,
LEAD(prd_start_dt) over(PARTITION by prd_key order by prd_start_dt) as prd_end_dt            -- calculate end date as one day the next start day

from bronze.crm_prd_info;





----------------------------
----------------------------
TRUNCATE table silver.crm_sales_details

 insert into silver.crm_sales_details (
sls_ord_num ,
sls_prd_key ,
sls_cust_id ,
sls_order_dt ,
sls_ship_dt ,
sls_due_dt ,
sls_sales ,
sls_quantity,
sls_price
 )

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or LEN(sls_order_dt) != 8 then null
else cast(cast(sls_order_dt as NVARCHAR)as DATE)
end as sls_order_dt,
cast(cast(sls_ship_dt as nvarchar)as date) as sls_ship_dt,
cast(cast(sls_due_dt as nvarchar)as date) as sls_due_dt,
case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price)
        then sls_quantity * abs(sls_price)
     else sls_sales
end sls_sales,                                                       --- recalculate sales if original value is missing or incorrect  
sls_quantity,
case when sls_price is null or sls_price <= 0
        then sls_sales / nullif(sls_quantity, 0)
      else sls_price
end as sls_price                                                      --- derive price if original value is invalid
from bronze.crm_sales_details




------------------------
------------------------




TRUNCATE table silver.erp_cust_az12
 
insert into silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)

select 
case when cid like'nas%' then SUBSTRING(cid, 4, LEN(cid))                             -- remove 'nas' --
else cid
end as cid,
case when bdate > GETDATE() then null         -- set futer birthdate to null --
       else bdate
end as bdate,       
CASE 
    WHEN UPPER(LTRIM(RTRIM(gen))) LIKE 'F%'  THEN 'FEMALE'
    WHEN UPPER(LTRIM(RTRIM(gen))) LIKE 'M%' THEN 'MALE'
    ELSE 'N/A'
END AS gen                                                                         -- normaliz gender valus and handel unknown case --
from bronze.erp_cust_az12





------------------------
------------------------




TRUNCATE table silver.erp_loc_a101

insert into silver.erp_loc_a101 (
    cid,
    cntry
)

select

REPLACE(cid, '-','') cid,
 CASE 
        WHEN cntry IS NULL OR LTRIM(RTRIM(cntry)) = '' THEN 'N/A' 
        WHEN TRIM(cntry) LIKE 'DE%' THEN 'Germany'
        WHEN TRIM(cntry) LIKE 'US%' THEN 'United States'
        ELSE TRIM(cntry)
    END AS cntry   
from bronze.erp_loc_a101





------------------------
------------------------



TRUNCATE table silver.erp_px_cat_g1v2

insert into silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance)

select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2



end


 



