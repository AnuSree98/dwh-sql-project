--To create GOLD view CUSTOMER, this includes data validation checks as well.

-- THIS IS USED TO JOIN CRM CUST TABLE WITH ERP CUST AND LOC TABLE TO GET FULL CUST DETAILS

-- left join tables so as to not lose customer data from crm customer table

select
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	cu.bdate,
	cu.gen,
	lo.cntry
from silver.crm_cust_info ci
left join silver.erp_cust cu 
on ci.cst_key = cu.cid
left join silver.erp_loc lo
on ci.cst_key = lo.cid;

-- to check if any duplicates were formed after table joins

select cst_id, count(*) from (
select
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	cu.bdate,
	cu.gen,
	lo.cntry
from silver.crm_cust_info ci
left join silver.erp_cust cu 
on ci.cst_key = cu.cid
left join silver.erp_loc lo
on ci.cst_key = lo.cid
)x
group by cst_id 
having count(*) > 1; 

-- we have two gender columns, which is a data integration issue.
-- we need to pick column that comes from Master table which is the CRM Table (depends on business/src team)

select DISTINCT 
ci.cst_gndr,
cu.gen
from silver.crm_cust_info ci
left join silver.erp_cust cu 
on ci.cst_key = cu.cid
left join silver.erp_loc lo
on ci.cst_key = lo.cid;

-- fix data based on Master data

select DISTINCT 
ci.cst_gndr,
cu.gen,
case 
	when ci.cst_gndr != 'n/a' then ci.cst_gndr
	else COALESCE(cu.gen,'n/a')
end new_gen
from silver.crm_cust_info ci
left join silver.erp_cust cu 
on ci.cst_key = cu.cid
left join silver.erp_loc lo
on ci.cst_key = lo.cid;


-- Fresh CUSTOMER data:
select
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	lo.cntry as country,
	ci.cst_marital_status as marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else COALESCE(cu.gen,'n/a')
	end gender,
	cu.bdate as birth_date,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust cu 
on ci.cst_key = cu.cid
left join silver.erp_loc lo
on ci.cst_key = lo.cid;


-- this is a dimension table since it provides info about object and not transactional or quantitative data
-- we will also create sys-generated key (surrogate key) for the view for usability instead of using cust_id as primary key 

select
	row_number() over(order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	lo.cntry as country,
	ci.cst_marital_status as marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else COALESCE(cu.gen,'n/a')
	end gender,
	cu.bdate as birth_date,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust cu 
on ci.cst_key = cu.cid
left join silver.erp_loc lo
on ci.cst_key = lo.cid;

-- FINAL CUSTOMER GOLD VIEW --

create view gold.dim_customers as 

select
	row_number() over(order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	lo.cntry as country,
	ci.cst_marital_status as marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else COALESCE(cu.gen,'n/a')
	end gender,
	cu.bdate as birth_date,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust cu 
on ci.cst_key = cu.cid
left join silver.erp_loc lo
on ci.cst_key = lo.cid;
