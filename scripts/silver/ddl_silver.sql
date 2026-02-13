--To create Silver layer table, drop tables if it exists.

DROP TABLE IF EXISTS silver.crm_cust_info;

create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.crm_prd_info;

create table silver.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	cat_id nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.erp_cust;

CREATE TABLE silver.erp_cust(
	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50)
);

DROP TABLE IF EXISTS silver.erp_loc;

CREATE TABLE silver.erp_loc(
	CID nvarchar(50),
	CNTRY nvarchar(50)
);

DROP TABLE IF EXISTS silver.erp_px_cat;

CREATE TABLE silver.erp_px_cat(
	ID nvarchar(50),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50)
);





