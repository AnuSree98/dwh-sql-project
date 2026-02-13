# dwh-sql-project
A modern data warehouse with MySQL Server + DBeaver, including ETL processes, data modelling and analytics.

# Data Architecture:
The architecture for this project uses the Medallion Architecture with Bronze-Silver-Gold layers
<img width="1281" height="834" alt="image" src="https://github.com/user-attachments/assets/eaf72b71-cf65-4b92-b7de-5876ec0db49a" />

1. Bronze Layer: Extracts data from source files and stores them as is.
2. Silver Layer: Extracts data from bronze layer, performs data cleaning, validations, normalizations and standardisations.
3. Gold Layer: Extracts data from silver layer, modelled into star schema and ready for business use.


All credit goes to Data With Baraa for a comprehensive and wonderful course.
