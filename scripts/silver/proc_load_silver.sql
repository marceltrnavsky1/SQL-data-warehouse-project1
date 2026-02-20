/*
===============================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================================
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
    EXEC Silver.load_silver;
===============================================================================================
*/


INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
SELECT
cst_id,
cst_key,
TRIM (cst_firstname) AS cts_firstname,
TRIM (cst_lastname) AS cts_lastname,
CASE WHEN UPPER (TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER (TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status, -- Normalize marital status values to readable format
CASE WHEN UPPER (TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
	 WHEN UPPER (TRIM(cst_gndr)) = 'M' THEN 'MALE'
	 ELSE 'n/a'
END cst_gndr, -- Normalize gender values to readable format
cst_create_date
FROM (
SELECT
*,
ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1; -- Select the most recent record per customer


;


INSERT INTO silver.crm_prd_info (
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
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, /* -> Extract a specific part of a string value, first 5 characters 
																-> aby sme mali rovnaký cat_id ako je v bronze.erp_px_cat_g1v2 tak treba REPLACE '-' ZA '_' (CO-RF vs CO_RF)*/
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        /* -> tento prd_key potrebujeme na spojenie s sls_prd_key (bronze.crm_sales_datails) */
	prd_nm,
	ISNULL (prd_cost, 0) AS prd_cost,        /* Všetky Null budú číslo 0 */
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN  'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN  'Road'
	     WHEN UPPER(TRIM(prd_line)) = 'S' THEN  'Other Sales'
	     WHEN UPPER(TRIM(prd_line)) = 'T' THEN  'Touring'
		 ELSE 'n/a'
	END AS prd_line, -- Map product line codes to descriptive values
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST (
		LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 
		AS DATE
	) AS prd_end_dt -- Calculate end date as one day before the next start date
	FROM bronze.crm_prd_info



;
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
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	 END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	 END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	 END AS sls_due_dt,
	 CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0
	THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
-- WHERE sls_ord_num != TRIM(sls_ord_num)									CHECKING
-- WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)		CHECKING
-- WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)		CHECKING



;

INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12
