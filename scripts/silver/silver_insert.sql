--This script is to be run after the data validation test. Please make sure to run the validation in Test folder for silver layer before running this script!

------ TO BULK LOAD CLEANED AND VALIDATED ROWS FROM BRONZE TABLE CRM_CUST_INFO --------



truncate table silver.crm_cust_info;

insert into silver.crm_cust_info (
	cst_id, 
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date

)

select cst_id, cst_key, 
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_marital_status)) = 'S' then 'Single'
when upper(trim(cst_marital_status)) = 'M' then 'Married'
else 'n/a'
end as cst_marital_status,
case when upper(trim(cst_gndr)) = 'F' then 'Female'
when upper(trim(cst_gndr)) = 'M' then 'Male'
else 'n/a'
end as cst_gndr,
cst_create_date
from (select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id != 0)x
where flag_last = 1;


select count(*) from silver.crm_cust_info;


------ TO BULK LOAD CLEANED AND VALIDATED ROWS FROM BRONZE TABLE CRM_PRD_INFO --------

truncate table silver.crm_prd_info;


insert into silver.crm_prd_info (
	prd_id,
	prd_key,
	cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt

)

select prd_id,
substring(prd_key, 7, length(prd_key)) as prd_key,
replace(substring(prd_key, 1, 5),'-','_') as cat_id,
prd_nm,
ifnull(prd_cost,0) as prd_cost,
case upper(trim(prd_line))
when 'R' then 'Road'
when 'M' then 'Mountain'
when 'S' then 'Other Sales'
when 'T' then 'Touring'
else 'n/a'
end as prd_line,
prd_start_dt,
date_sub(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt), interval 1 day) as prd_end_dt
from bronze.crm_prd_info;

select * from silver.crm_prd_info;

------ TO BULK LOAD CLEANED AND VALIDATED ROWS FROM BRONZE TABLE CRM_SALES_DETAILS --------

truncate table silver.crm_sales_details;


insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price

)

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0000-00-00 then null
else sls_order_dt
end as sls_order_dt,
case when sls_ship_dt = 0000-00-00 then null
else sls_ship_dt
end as sls_ship_dt,
case when sls_due_dt = 0000-00-00 then null
else sls_due_dt
end as sls_due_dt,
case when sls_sales is null or sls_sales <=  0 or sls_sales <> sls_quantity * abs( CASE
              WHEN sls_price IS NULL OR sls_price = 0 THEN sls_sales / NULLIF(sls_quantity, 0)
              ELSE sls_price
            END)
then sls_quantity * abs( CASE
              WHEN sls_price IS NULL OR sls_price = 0 THEN sls_sales / NULLIF(sls_quantity, 0)
              ELSE sls_price
            END)
else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price = 0
then sls_sales / nullif(sls_quantity,0)
when sls_price < 0 then abs(sls_price)
else sls_price
end as sls_price
from bronze.crm_sales_details;

select * from silver.crm_sales_details;


------ TO BULK LOAD CLEANED AND VALIDATED ROWS FROM BRONZE TABLE ERP_CUST --------

truncate table silver.erp_cust;


insert into silver.erp_cust (
	CID,
	BDATE,
	GEN 

)

select
case when cid like 'NAS%' then substring(cid, 4, length(cid))
else cid
end as cid,
case when bdate > sysdate() then null
else bdate
end as bdate, 
case 
	when cleaned in ('F', 'Female') then 'Female' 
	when cleaned in ('M', 'Male') then 'Male'
	else 'n/a'
end as gen
from (
  SELECT
  	cid,
  	bdate,
    gen,
    UPPER(REGEXP_REPLACE(COALESCE(gen,''), '[[:space:]]+', '')) AS cleaned
  FROM bronze.erp_cust
) x;

select * from silver.erp_cust;

------ TO BULK LOAD CLEANED AND VALIDATED ROWS FROM BRONZE TABLE ERP_LOC --------

truncate table silver.erp_loc;


insert into silver.erp_loc (
	CID,
	CNTRY 

)

select replace(cid,'-', '') cid,
case
	when cleaned_cntry = 'DE' then 'Germany'
	when cleaned_cntry in ('US','USA', 'UnitedStates') then 'United States'
	when cleaned_cntry in ('UK','UnitedKingdom') then 'United Kingdom'
	when cleaned_cntry = '' or cleaned_cntry is null then 'n/a'
	else cleaned_cntry
end as cntry
from (SELECT
    cid,
    cntry,
    REGEXP_REPLACE(COALESCE(cntry, ''), '[[:space:]]+', '') AS cleaned_cntry -- this is due to values contain hidden whitespace characters (especially \r carriage return from Windows line endings)
  FROM bronze.erp_loc)x;

select * from silver.erp_loc;

------ TO BULK LOAD CLEANED AND VALIDATED ROWS FROM BRONZE TABLE ERP_PX_CAT --------

truncate table silver.erp_px_cat;

insert into silver.erp_px_cat (
	id,
	cat,
	subcat,
	maintenance
)
SELECT
id,
cat, 
subcat,
REGEXP_REPLACE(COALESCE(maintenance, ''), '[[:space:]]+', '')
from bronze.erp_px_cat;

select * from silver.erp_px_cat;
