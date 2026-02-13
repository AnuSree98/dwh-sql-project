--This script is to insert data from CRM, ERP files to bronze layer.

truncate table bronze.crm_cust_info;

load data local 
infile '/Users/anuvindasreenivas/Desktop/Work (Plis Give)/DWH Project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
into table bronze.crm_cust_info
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 lines;


select * from bronze.crm_cust_info;

------------------------------------------------------------------------------

truncate table bronze.crm_prd_info;

load data local 
infile '/Users/anuvindasreenivas/Desktop/Work (Plis Give)/DWH Project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
into table bronze.crm_prd_info
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 lines;

select * from bronze.crm_prd_info;

------------------------------------------------------------------------------

truncate table bronze.crm_sales_details;

load data local 
infile '/Users/anuvindasreenivas/Desktop/Work (Plis Give)/DWH Project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
into table bronze.crm_sales_details
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 lines;

select * from bronze.crm_sales_details;

------------------------------------------------------------------------------

truncate table bronze.erp_cust;

load data local 
infile '/Users/anuvindasreenivas/Desktop/Work (Plis Give)/DWH Project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
into table bronze.erp_cust
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 lines;

select * from bronze.erp_cust;

------------------------------------------------------------------------------

truncate table bronze.erp_loc;

load data local 
infile '/Users/anuvindasreenivas/Desktop/Work (Plis Give)/DWH Project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
into table bronze.erp_loc
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 lines;

select * from bronze.erp_loc;

------------------------------------------------------------------------------

truncate table bronze.erp_px_cat;

load data local 
infile '/Users/anuvindasreenivas/Desktop/Work (Plis Give)/DWH Project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
into table bronze.erp_px_cat
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 lines;

select * from bronze.erp_px_cat;

------------------------------------------------------------------------------



