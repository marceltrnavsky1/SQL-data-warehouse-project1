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
