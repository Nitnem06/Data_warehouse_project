/*
  AIM: To load and clean the data from bronze layer into silver layer by applying transformations on it.
*/

USE DataWarehouse;
EXEC Silver.cleaning_data;

USE [DataWarehouse]
	GO
	DECLARE	@return_value int
	EXEC	@return_value = [Silver].[loading_data]
	SELECT	'Return Value' = @return_value
	GO

	DROP TABLE Silver.crm_sales_details
	CREATE TABLE Silver.crm_sales_details (
    sls_ord_num INT,
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales FLOAT,
    sls_quantity INT,
    sls_price FLOAT
);

GO

IF COL_LENGTH('Silver.crm_prd_info', 'cat_id') IS NOT NULL
	BEGIN
    ALTER TABLE Silver.crm_prd_info DROP COLUMN cat_id;
	END;
	ALTER TABLE Silver.crm_prd_info ADD cat_id VARCHAR(50);

GO

IF COL_LENGTH('Silver.crm_sales_details', 'sls_ord_num') IS NOT NULL
	BEGIN
    ALTER TABLE Silver.crm_sales_details DROP COLUMN sls_ord_num;
	END;
	ALTER TABLE Silver.crm_sales_details ADD sls_ord_num VARCHAR(50);

GO

IF COL_LENGTH('Silver.crm_prd_info', 'prd_line') IS NOT NULL
	BEGIN
    ALTER TABLE Silver.crm_prd_info DROP COLUMN prd_line;
	END;
	ALTER TABLE Silver.crm_prd_info ADD prd_line VARCHAR(50);

GO

IF COL_LENGTH('Silver.erp_CUST_AZ12', 'CreateDate') IS NULL
BEGIN
    ALTER TABLE Silver.erp_CUST_AZ12 
    ADD CreateDate DATE DEFAULT (CURRENT_TIMESTAMP);
END

GO

CREATE OR ALTER PROCEDURE Silver.cleaning_data AS
BEGIN

	--check for duplicate values, and taking the recent most data by taking the highest value of created date by using a windows function ROW_NUMBER()
	--TRIM() function for avoiding any white spaces in the string columns
	TRUNCATE TABLE Silver.crm_cust_info;
	INSERT INTO Silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
	SELECT cst_id,
		cst_key,
		TRIM(cst_firstname),
		TRIM(cst_lastname),
		CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			ELSE 'N/A'
		END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			ELSE 'N/A'
		END cst_gndr,
		cst_create_date FROM 
	(SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) 
	AS flag_last FROM Bronze.crm_cust_info)t WHERE flag_last=1;

	SELECT * FROM Silver.crm_cust_info;

	

	--For cases where start_dt>end_dt, we are using the windows fucntion LEAD()
	--We are overwriting the end_dt with the start_dt of the next row of the same product order placed
	TRUNCATE TABLE Silver.crm_prd_info;
	INSERT INTO Silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
	SELECT prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'OtherSales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'N/A'
		END AS prd_line,
		prd_start_dt,
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
		FROM Bronze.crm_prd_info;

	SELECT * FROM Silver.crm_prd_info;

	

	TRUNCATE TABLE Silver.crm_sales_details;
	INSERT INTO Silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112),
		TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8)), 112),
		TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8)), 112),

		CASE WHEN sls_sales<=0 OR sls_sales IS NULL OR sls_sales!=sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price<=0 THEN ABS(sls_price)
			 WHEN sls_price IS NULL OR sls_price!=sls_sales/NULLIF(sls_quantity,0) THEN sls_sales/NULLIF(sls_quantity,0)
			 ELSE sls_price
		END AS sls_price
		FROM Bronze.crm_sales_details;

	SELECT * FROM Silver.crm_sales_details;

	

	TRUNCATE TABLE Silver.erp_CUST_AZ12;
	INSERT INTO Silver.erp_CUST_AZ12(CID,BDATE,GEN)
	SELECT
		CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID
		END AS CID,-- removing the extra NAS at the start of the CID in order to make it compatible for JOINS with the crm_cust_info table
		CASE WHEN BDATE > GETDATE() THEN NULL
			 ELSE BDATE
		END AS BDATE,
		CASE UPPER(TRIM(GEN))
			 WHEN 'M' THEN 'Male'
			 WHEN 'F' THEN 'Female'
			 WHEN '' THEN 'N/A'
			 ELSE 'N/A'
		END AS GEN
		FROM Bronze.erp_CUST_AZ12;

	SELECT * FROM Silver.erp_CUST_AZ12;

	

	TRUNCATE TABLE Silver.erp_LOC_A101;
	INSERT INTO Silver.erp_LOC_A101(CID,CNTRY)
	SELECT 
		REPLACE(CID,'-','') CID,
		CASE WHEN TRIM(CNTRY)='DE' THEN 'Germany'
			 WHEN TRIM(CNTRY) IN ('USA','US') THEN 'United States'
			 WHEN TRIM(CNTRY)='' OR TRIM(CNTRY) IS NULL THEN 'N/A'
			 ELSE TRIM(CNTRY)
		END AS CNTRY
		FROM Bronze.erp_LOC_A101;

	SELECT * FROM Silver.erp_LOC_A101;

	

	TRUNCATE TABLE Silver.erp_PX_CAT_G1V2;
	INSERT INTO Silver.erp_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
	SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		FROM Bronze.erp_PX_CAT_G1V2;

	SELECT * FROM Silver.erp_PX_CAT_G1V2;

END;
	SELECT * FROM Silver.erp_PX_CAT_G1V2;

END;
