-- To create GOLD view PRODUCT

-- TO COMBINE PRODUCT TABLE WITH CRM_SALES AND ERP_PX TO GET ALL PRODUCT DETAILS

-- we don't need historical data and only current data

SELECT 
	pr.prd_id,
	pr.prd_key,
	pr.cat_id,
	pr.prd_nm,
	pr.prd_cost,
	pr.prd_line,
	pr.prd_start_dt
from silver.crm_prd_info pr
where pr.prd_end_dt is null; -- to filter out historical data 

-- to join product table with erp_px table

SELECT 
	pr.prd_id,
	pr.prd_key,
	pr.cat_id,
	pr.prd_nm,
	pr.prd_cost,
	pr.prd_line,
	pr.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
from silver.crm_prd_info pr
left join silver.erp_px_cat pc
on pr.cat_id = pc.id
where pr.prd_end_dt is null;
	
-- to check if any duplicates were formed after join
select prd_key, count(*) from(
SELECT 
	pr.prd_id,
	pr.prd_key,
	pr.cat_id,
	pr.prd_nm,
	pr.prd_cost,
	pr.prd_line,
	pr.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
from silver.crm_prd_info pr
left join silver.erp_px_cat pc
on pr.cat_id = pc.id
where pr.prd_end_dt is null)x
group by prd_key
having count(*)>1;


-- final query: This is a dimension since it describes the object which is product, so we need to produce a primary key for it

SELECT 
	row_number() over(order by pr.prd_start_dt, pr.prd_key) as product_key,
	pr.prd_id as product_id,
	pr.prd_key as product_number,
	pr.prd_nm as product_name,
	pr.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pr.prd_cost as cost,
	pr.prd_line as product_line,
	pr.prd_start_dt as start_date
from silver.crm_prd_info pr
left join silver.erp_px_cat pc
on pr.cat_id = pc.id
where pr.prd_end_dt is null;


-- FINAL PRODUCT GOLD VIEW --

create view gold.dim_products as 
SELECT 
	row_number() over(order by pr.prd_start_dt, pr.prd_key) as product_key,
	pr.prd_id as product_id,
	pr.prd_key as product_number,
	pr.prd_nm as product_name,
	pr.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pr.prd_cost as cost,
	pr.prd_line as product_line,
	pr.prd_start_dt as start_date
from silver.crm_prd_info pr
left join silver.erp_px_cat pc
on pr.cat_id = pc.id
where pr.prd_end_dt is null;



