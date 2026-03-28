/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
	CALL silver.load_silver;
===============================================================================
*/




DROP PROCEDURE silver.load_silver;


DELIMITER // 

	
CREATE PROCEDURE silver.load_silver ()   

BEGIN

		DECLARE start_time DATETIME;
		DECLARE end_time DATETIME;
		DECLARE batch_start_time DATETIME;
		DECLARE batch_end_time DATETIME;
	
		SET start_time = NOW();
			SET batch_start_time = NOW();

	
	SELECT 'Loading Silver Layer' AS message;


	SELECT 'Loading CRM TABLE' AS message;
 
 		-- Loading silver.crm_cust_info
 		
 	 SET @start_time = CURRENT_TIME();
		SELECT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
		SELECT '>> Inserting Data Into: silver.crm_cust_info';
	
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_fistname,
		TRIM(cst_lastname) as cst_lastname,
		
		CASE WHEN TRIM(cst_material_status) = 'S' then 'Single'
			 WHEN TRIM(cst_material_status) = 'M' then 'Married'
			 else 'n/a'
			 
		END cst_marital_status,
		
		CASE WHEN TRIM(cst_gndr) = 'F' then 'Female'
			 WHEN TRIM(cst_gndr) = 'M' then 'Male'
			 else 'n/a'
		END cst_gndr,
			
		cst_create_date
	FROM 
	(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	from bronze.crm_cust_info) t 
	where flag_last = 1 and cst_id != 0;
	
	-- Loading silver.crm_prd_info-- 
	
	TRUNCATE TABLE silver.crm_prd_info;
	
	
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') as cat_id,
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) as pr_key,
		prd_nm,
		COALESCE(prd_cost,0) as prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' then 'Mountain'
		     WHEN 'S' then 'Other sales'
		     WHEN 'R' then 'Road'
		     WHEN 'T' then 'Touring'
		ELSE 'n/a'
		END AS prd_line, 
		CAST(prd_start_dt as DATE) as prd_start_dt ,
		CAST(lead(prd_start_dt) over(PARTITION BY prd_key order by prd_start_dt) - INTERVAL 1 DAY  as DATE) AS prd_end_dt 
	 
	from bronze.crm_prd_info;
	
	
	
	
	
	TRUNCATE TABLE silver.crm_sales_details;
	
	
	
	
	
	INSERT INTO silver.crm_sales_details(
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
	
	
	
	
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		
		CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS CHAR)) != 8 THEN NULL  
			 ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
			 end as sls_order_dt,
			 
		CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS CHAR)) != 8 THEN NULL  
			 ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
			 end as sls_ship_dt,
	
		CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_ship_dt AS CHAR)) != 8 THEN NULL  
			 ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
			 end as sls_due_dt,
			   
		CASE WHEN sls_sales IS NULL or sls_sales <= 0 
			then  sls_quantity * ABS(sls_price)
			else sls_sales  
			end as sls_sales,
		
		sls_quantity,
			
		CASE WHEN sls_price IS NULL or sls_price <= 0 
			THEN round(sls_sales/NULLIF(sls_quantity ,0))   
			else sls_price
			end as sls_price 
			
	FROM bronze.crm_sales_details;
	 
	SELECT 'Loading ERP TABLE' AS message;

	
	TRUNCATE TABLE silver.erp_cust_az12;
	
	INSERT INTO
	
		silver.erp_cust_az12 (cid, bdate, gen)

	SELECT
	
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if present
			ELSE cid
		END AS cid,
		
		CASE
			WHEN bdate > CURRENT_TIME() THEN NULL
			ELSE bdate
		END AS bdate, -- Set future birthdates to NULL
		
		CASE
			WHEN TRIM(gen) like 'F%' THEN 'Female'
			WHEN TRIM(gen) like 'M%' THEN 'Male'
			ELSE 'n/a'
		END AS gen -- Normalize gender values and handle unknown cases
		
	FROM bronze.erp_cust_az12;
	
	
	-- next -- 
	
	TRUNCATE TABLE silver.erp_loc_a101; 
	
	
	INSERT INTO silver.erp_loc_a101 (cid,cntry)
	
	SELECT 
	
		REPLACE(cid,'-','') id,
		
		CASE WHEN TRIM(cntry) LIKE 'DE%' THEN 'Germany'
			 WHEN TRIM(cntry) LIKE 'US%' OR TRIM(cntry) LIKE 'USA%' THEN 'United States' 
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END as cntry
		
	FROM bronze.erp_loc_a101; 
	
	-- next -- 
	TRUNCATE TABLE silver.erp_px_cat_g1v2; 
	
	
	INSERT INTO silver.erp_px_cat_g1v2
	(id,cat,subcat,maintenance)
	
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2; 
	
	SELECT 
		start_time,
		end_time,
		TIMEDIFF(end_time, start_time) AS duration; 
	
	SELECT 'All silver tables loaded successfully!' AS message; 


END //

DELIMITER ;


CALL silver.load_silver;
