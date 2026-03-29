/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================


-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers as 

SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id ) as customer_key,  -- Surrogate key
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for  gender info -- 
		else COALESCE(ca.gen, 'n/a')     -- Fallback to ERP data
	end as gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
	
	
from silver.crm_cust_info as ci 
left join silver.erp_cust_az12 as ca 
on ca.cid = ci.cst_key 
LEFT JOIN silver.erp_loc_a101 as la 
on la.cid = ci.cst_key;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_products;



CREATE VIEW gold.dim_products as 

SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key) as product_key,  -- Surrogate key
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategpry,
	pc.maintenance as maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date

from silver.crm_prd_info as pn 
LEFT JOIN silver.erp_px_cat_g1v2 as pc 
on pn.cat_id = pc.id

WHERE prd_end_dt is NULL -- Filter out all historical data 
;

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

DROP VIEW IF EXISTS gold.fact_sales;


CREATE VIEW gold.fact_sales as 

SELECT 
	sd.sls_ord_num as order_number,
	pr.product_key ,
	dc.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as dua_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price 

	
from silver.crm_sales_details as sd 
LEFT JOIN gold.dim_products as pr 
on sd.sls_prd_key = pr.product_number 
LEFT JOIN gold.dim_customers as dc 
on sd.sls_cust_id = dc.customer_id ;





