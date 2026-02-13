-- To create GOLD view SALES, this includes data validation checks as well.

-- THIS IS USED TO JOIN CRM SALES TABLE WITH CRM CUST AND PROD TABLE TO GET FULL DETAILS, THIS MAKES SALES TABLE A FACT TABLE

select 
	sal.sls_ord_num,
	pr.product_key,
	cu.customer_key,
	sal.sls_order_dt,
	sal.sls_ship_dt,
	sal.sls_due_dt,
	sal.sls_sales,
	sal.sls_quantity,
	sal.sls_price
from silver.crm_sales_details sal
left join gold.dim_products pr
on sal.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sal.sls_cust_id = cu.customer_id;

-- FINAL SALES GOLD VIEW --

create view gold.fact_sales as
select 
	sal.sls_ord_num order_number,
	pr.product_key,
	cu.customer_key,
	sal.sls_order_dt order_date,
	sal.sls_ship_dt shipping_date,
	sal.sls_due_dt due_date,
	sal.sls_sales sales_amount,
	sal.sls_quantity quantity,
	sal.sls_price price
from silver.crm_sales_details sal
left join gold.dim_products pr
on sal.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sal.sls_cust_id = cu.customer_id;

-- to check for integrity issues

select *
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key 
where c.customer_key is null;

select *
from gold.fact_sales f
left join gold.dim_products p
on p.product_key  = f.product_key  
where p.product_key  is null;
	
	
	
