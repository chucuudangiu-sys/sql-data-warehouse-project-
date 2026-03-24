/*
===============================================================================
SQL Script: Load Bronze Layer (Source -> Bronze)
===============================================================================
Purpose:
    Load data from CSV files into bronze schema tables.
    - Truncates all bronze tables before loading
    - Loads CRM data (customer, product, sales)
    - Loads ERP data (customer, location, category)

Tables Loaded:
    CRM: crm_cust_info, crm_prd_info, crm_sales_details
    ERP: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2

File Format:
    - Delimiter: Comma (,)
    - Enclosed: Double quotes (")
    - Skip rows: 1 (header)

Usage:
    mysql -u root -p --local-infile=1 < load_bronze_data.sql

Prerequisites:
    - SET GLOBAL local_infile = 1;
    - CSV files must exist at specified paths
    - Bronze tables must be created
    - User needs FILE and INSERT privileges

===============================================================================
*/

DROP PROCEDURE IF EXISTS bronze.load_procedure;

DELIMITER //

CREATE PROCEDURE bronze.load_procedure()
BEGIN
	DECLARE v_start_time DATETIME;
	DECLARE v_end_time DATETIME;
	DECLARE v_duration INT;
	
	SET v_start_time = NOW();
	
	SELECT 'Starting Data Load into Bronze Layer';


	SELECT '>> Truncating all tables...';
	TRUNCATE TABLE bronze.crm_cust_info;
	TRUNCATE TABLE bronze.crm_prd_info;
	TRUNCATE TABLE bronze.crm_sales_details;
	TRUNCATE TABLE bronze.erp_cust_az12;
	TRUNCATE TABLE bronze.erp_loc_a101;
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	SELECT '>> Tables truncated successfully';
	
END//

DELIMITER ;

CALL bronze.load_procedure();

SELECT '>> Loading CRM Data...';

LOAD DATA LOCAL INFILE '/Users/admin/Desktop/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/admin/Desktop/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/admin/Desktop/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT '>> Loading ERP Data...';

LOAD DATA LOCAL INFILE '/Users/admin/Desktop/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/admin/Desktop/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/admin/Desktop/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT 'Data Load Completed Successfully';
