/*
==================
DDL Script: Create Silver Tables
==================
Script purpose:
    This script create tables in the 'silver' schema, droppind existing tables 
    if they already exist.
Run this script to re-define the DDL structure of 'bronze' tables
==================
*/



create or alter PROCEDURE silver.load_bronze as
begin
 

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);



-----------------------
-----------------------


IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info; 

create TABLE silver.crm_prd_info (
 prd_id int,
 prd_key NVARCHAR(50),
 cat_id NVARCHAR (50),
 sls_prd_key NVARCHAR(50),
 prd_nm NVARCHAR(50),
 prd_cost NVARCHAR(50),
 prd_line NVARCHAR(50),
 prd_start_dt date,
 prd_end_dt date    
);




------------------
------------------


IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
    

create table silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price INT
);




----------------------
----------------------



if OBJECT_ID('silver.erp_cust_az12', 'u') is not null
drop table silver.erp_cust_az12;


create table silver.erp_cust_az12(
CID NVARCHAR(50),
BDATE date,
GEN NVARCHAR(50)   
)




---------------------
----------------------



if OBJECT_ID('silver.erp_loc_a101', 'U') is not NULL
drop table silver.erp_loc_a101;

create table silver.erp_loc_a101(
 CID NVARCHAR(50),  
 CNTRY NVARCHAR(50)

);



------------------
------------------




if object_id('silver.erp_px_cat_g1v2', 'U')is not NULL
drop table silver.erp_px_cat_g1v2;

create table silver.erp_px_cat_g1v2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)

);











end








exec silver.load_silver
