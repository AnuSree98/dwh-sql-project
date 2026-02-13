--This script should be used to validate the data from bronze layer before inserting into the Silver layer.

----- TO CHECK AND VALIDATE CUST_INFO TABLE BEFORE INSERTING INTO SILVER LAYER -----


-- To check for nulls or duplicates in primary key values

-- in table crm_cust_info

select * from bronze.crm_cust_info;

select cst_id, count(*) 
from bronze.crm_cust_info
group by cst_id
having count(*) >1 or cst_id = 0;

-- to select values that are duplicates

select * from (select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id != 0)x
where flag_last = 1;

----- TO CHECK FOR UNWANTED SPACES IN STRING COLUMNS AND REMOVE THEM -----

-- to detect unwanted spaces

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname) ;

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname) ;

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr) ;


-- to remove unwanted spaces
select cst_id, cst_key, 
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
from (select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id != 0)x
where flag_last = 1;


----- FOR DATA STANDARDIZATION AND CONSISTENCY -----

-- to put full name as FEMALE/MALE instead of F/M | MARRIED/SINGLE instead of M/S

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


----- TO CHECK AND VALIDATE PRD_INFO TABLE BEFORE INSERTING INTO SILVER LAYER -----


-- query to check for duplicate primary key values
select prd_id, count(*) from bronze.crm_prd_info
group by prd_id
HAVING count(*) > 1 or prd_id = 0;

-- query to clean and validate prd_key column so as to be able to join table with erp_px_cat

select prd_id,
prd_key,
replace(substring(prd_key, 1, 5),'-','_') as cat_id,
substring(prd_key, 7, length(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;


-- to check if prd_nm has any unwanted spaces

select prd_nm from bronze.crm_prd_info
where prd_nm != trim(prd_nm);

-- to check for costs that might be negative

select prd_cost from bronze.crm_prd_info
where prd_cost < 0;

-- to standardize values for prd_line since its cardinality is low

select distinct prd_line from bronze.crm_prd_info;

select prd_id,
prd_key,
replace(substring(prd_key, 1, 5),'-','_') as cat_id,
substring(prd_key, 7, length(prd_key)) as prd_key,
prd_nm,
prd_cost,
case upper(trim(prd_line))
when 'R' then 'Road'
when 'M' then 'Mountain'
when 'S' then 'Other Sales'
when 'T' then 'Touring'
else 'n/a'
end as prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;

--- to correct date issues for start and end date cols (where end date is lesser than start date)

select * from bronze.crm_prd_info 
where prd_end_dt < prd_start_dt ;

/*select prd_id,
prd_key,
prd_nm, 
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt,
date_sub(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt), interval 1 day) as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509');*/

select prd_id,
prd_key,
replace(substring(prd_key, 1, 5),'-','_') as cat_id,
substring(prd_key, 7, length(prd_key)) as prd_key,
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
date_sub(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt), interval 1 day) as prd_end_dt_test
from bronze.crm_prd_info;


----- TO CHECK AND VALIDATE SALES_DETAILS TABLE BEFORE INSERTING INTO SILVER LAYER -----

-- to check for unwanted spaces in columns
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num);


-- to check if we can join reference tables for prd_key and cust_id
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

-- to validate and change date columns, keep weird dates as null instead

select
nullif(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where
sls_order_dt is null
OR sls_order_dt < DATE('1900-01-01')
OR sls_order_dt > DATE('2050-01-01');

select
nullif(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where
sls_order_dt is null
OR sls_order_dt < DATE('1900-01-01')
OR sls_order_dt > DATE('2050-01-01');

select
nullif(sls_ship_dt,0) as sls_ship_dt
from bronze.crm_sales_details
where
sls_ship_dt is null
OR sls_ship_dt < DATE('1900-01-01')
OR sls_ship_dt > DATE('2050-01-01');

select
nullif(sls_due_dt,0) as sls_due_dt
from bronze.crm_sales_details
where
sls_due_dt is null
OR sls_due_dt < DATE('1900-01-01')
OR sls_due_dt > DATE('2050-01-01');


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
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

-- to check if order date is greater than ship date/due date and change it

select * from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt ;

-- to check data consistency where sales should be quantity * price and values should not be null, zero or negative.

select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

/* correcting sales, quantity and price, when sales is negative, zero or null derive it from quantity and price;
when price is zero or null, calculate it using sales and quantity
when price is negative, convert it to a positive value */

select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
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
case when sls_price is null or sls_price = 0
then sls_sales / nullif(sls_quantity,0)
when sls_price < 0 then abs(sls_price)
else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;



/* new query */

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

----- TO CHECK AND VALIDATE ERP_CUST TABLE BEFORE INSERTING INTO SILVER LAYER -----

-- to normalize cid column inorder to join with cst_key col in silver.cust_info table.

select * from bronze.erp_cust where cid like '%AW00011000%';

select
cid,
case when cid like 'NAS%' then substring(cid, 4, length(cid))
else cid
end as cid,
bdate, 
gen
from bronze.erp_cust
where case when cid like 'NAS%' then substring(cid, 4, length(cid))
else cid
end
not in (Select distinct cst_key from silver.crm_cust_info);

-- to check for invalid dates (out of range)

select distinct bdate from bronze.erp_cust 
where bdate > sysdate();


select
cid,
case when cid like 'NAS%' then substring(cid, 4, length(cid))
else cid
end as cid,
case when bdate > sysdate() then null
else bdate
end as bdate, 
gen
from bronze.erp_cust;

-- to check for gender 

select distinct gen from bronze.erp_cust;

select distinct gen,
case 
	when cleaned in ('F', 'Female') then 'Female' 
	when cleaned in ('M', 'Male') then 'Male'
	else 'n/a'
end as new_gen
from (
  SELECT
    gen,
    UPPER(REGEXP_REPLACE(COALESCE(gen,''), '[[:space:]]+', '')) AS cleaned
  FROM bronze.erp_cust
) x;

select count(*) from bronze.erp_cust;

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

----- TO CHECK AND VALIDATE ERP_LOC TABLE BEFORE INSERTING INTO SILVER LAYER -----

select * from bronze.erp_loc;

-- to remove hyphen in cid in order to join with silver.epr_cust table's cid

select replace(cid,'-', '') cid,
cntry
from bronze.erp_loc
where replace(cid,'-', '') not in (select cst_key from silver.crm_cust_info);


-- to standardize cntry column 

select distinct cntry from silver.erp_loc;

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

----- TO CHECK AND VALIDATE ERP_PX_CAT TABLE BEFORE INSERTING INTO SILVER LAYER -----

select * from bronze.erp_px_cat;

select * from bronze.erp_px_cat
where MAINTENANCE != trim(MAINTENANCE);

select distinct cat from bronze.erp_px_cat;

select distinct subcat from bronze.erp_px_cat;

select distinct MAINTENANCE from bronze.erp_px_cat;

SELECT
id,
cat, 
subcat,
REGEXP_REPLACE(COALESCE(maintenance, ''), '[[:space:]]+', '')
from bronze.erp_px_cat;





