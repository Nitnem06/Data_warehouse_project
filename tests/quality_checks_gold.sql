/*
  AIM: Qualitty checks to validate for the integrity, consistency and accuracy of the gold layer.
*/
USE DataWarehouse;

--For checking if there are any duplicate/redundant cst_id values after joining both tables
--Expectation: No rows
SELECT cst_id,COUNT(*) FROM
(SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	CA.BDATE AS cst_bdate,
	CA.GEN,
	LA.CNTRY AS cst_country
	FROM Silver.crm_cust_info AS ci 
	LEFT JOIN Silver.erp_CUST_AZ12 AS CA ON ci.cst_key=CA.CID
	LEFT JOIN Silver.erp_LOC_A101 AS LA ON ci.cst_key=LA.CID)t
	GROUP BY cst_id HAVING COUNT(*)>1;

	--We have two columns ofr gender, so to find out if there are any mismatches in these columns after joining both tables
	SELECT DISTINCT
	ci.cst_gndr,
	CA.GEN
	FROM Silver.crm_cust_info AS ci 
	LEFT JOIN Silver.erp_CUST_AZ12 AS CA ON ci.cst_key=CA.CID
	LEFT JOIN Silver.erp_LOC_A101 AS LA ON ci.cst_key=LA.CID;

	--For checking if there are any duplicate/redundant prd_key values after joining both tables
	--Expectation: No rows
	SELECT prd_key,COUNT(*) FROM
	(SELECT
		pi.prd_id,
		pi.prd_key,
		pi.prd_nm,
		pi.prd_cost,
		pi.prd_start_dt,
		pi.cat_id,
		pi.prd_line,
		PC.CAT,
		PC.SUBCAT,
		PC.MAINTENANCE
		FROM Silver.crm_prd_info AS pi
		LEFT JOIN Silver.erp_PX_CAT_G1V2 AS PC
		ON pi.cat_id=PC.ID
		WHERE pi.prd_end_dt IS NULL)t
	GROUP BY prd_key HAVING COUNT(*)>1;

	--Foreign key integrity (Dimensions)
	--Expectation: No rows
	SELECT * FROM Gold.fact_sales AS f
	LEFT JOIN Gold.dim_customers AS c
	ON c.CUSTOMER_KEY=f.CUSTOMER_KEY
	WHERE c.CUSTOMER_KEY IS NULL;

	--Foreign key integrity (Dimensions)
	--Expectation: No rows
	SELECT * FROM Gold.fact_sales AS f
	LEFT JOIN Gold.dim_products AS p
	ON f.PRODUCT_KEY=p.PRODUCT_KEY
	WHERE p.PRODUCT_KEY IS NULL;
