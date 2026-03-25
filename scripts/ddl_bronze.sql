/*
===================
DDL Script: Create Bronze Tables
===================
Script purpose:
   This script create tables in the 'bronze' schema, dropping existing tables if they already exist.
Run this script to re-define the DDL structure of 'bronze' tables
===================
*/




create or alter PROCEDURE bronze.load_bronze as
begin
 

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);

BULK INSERT bronze.crm_cust_info
FROM '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);


-----------------------
-----------------------


IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info; 

create TABLE bronze.crm_prd_info (
 prd_id int,
 prd_key NVARCHAR(50),
 prd_nm NVARCHAR(50),
 prd_cost NVARCHAR(50),
 prd_line NVARCHAR(50),
 prd_start_dt date,
 prd_end_dt date    
);


BULK INSERT bronze.crm_prd_info
FROM '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);


------------------
------------------


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
    

create table bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price INT
);


BULK INSERT bronze.crm_sales_details
FROM '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
with(
    FIRSTROW = 2,
     FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK

);


----------------------
----------------------



if OBJECT_ID('bronze.erp_cust_az12', 'u') is not null
drop table bronze.erp_cust_az12;


create table bronze.erp_cust_az12(
CID NVARCHAR(50),
BDATE date,
GEN NVARCHAR(50)   
)

BULK insert bronze.erp_cust_az12
from '/var/opt/mssql/data/datasets/source_erp/CUST_AZ12.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    rowterminator = '\n',
    tablock
);



---------------------
----------------------



if OBJECT_ID('bronze.erp_loc_a101', 'U') is not NULL
drop table bronze.erp_loc_a101;

create table bronze.erp_loc_a101(
 CID NVARCHAR(50),  
 CNTRY NVARCHAR(50)

);

BULK insert bronze.erp_loc_a101
from '/var/opt/mssql/data/datasets/source_erp/LOC_A101.csv'
with(
    firstrow = 2,
    FIELDTERMINATOR = ',',
    rowterminator = '\n',
    tablock 

);


------------------
------------------




if object_id('bronze.erp_px_cat_g1v2', 'U')is not NULL
drop table bronze.erp_px_cat_g1v2;

create table bronze.erp_px_cat_g1v2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)

);


BULK INSERT bronze.erp_px_cat_g1v2
FROM '/var/opt/mssql/data/datasets/source_erp/PX_CAT_G1V2.csv'
with(
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '\n',
    tablock
);










end
