/*
  AIM: Creating gold views for business and analytics purpose.
*/
USE DataWarehouse;
EXEC Silver.cleaning_data;

GO 

SELECT * FROM SIlver.crm_cust_info;
SELECT * FROM SIlver.crm_prd_info;
SELECT * FROM SIlver.crm_sales_details;
SELECT * FROM SIlver.erp_CUST_AZ12;
SELECT * FROM SIlver.erp_LOC_A101;
SELECT * FROM SIlver.erp_PX_CAT_G1V2;

GO 

CREATE OR ALTER VIEW Gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS CUSTOMER_KEY,		--A surrogate key
	ci.cst_id AS CUSTOMER_ID,
	ci.cst_key AS CUSTOMER_NUMBER,
	ci.cst_firstname AS FIRST_NAME,
	ci.cst_lastname AS LAST_NAME,
	LA.CNTRY AS COUNTRY,
	ci.cst_marital_status AS MARITAL_STATUS,
	CASE WHEN ci.cst_gndr!='N/A' THEN ci.cst_gndr		--CRM is the Master Table
		 ELSE COALESCE(CA.GEN,'N/A')
	END AS GENDER,
	CA.BDATE AS BIRTHDATE,
	ci.cst_create_date AS CREATE_DATE
	FROM Silver.crm_cust_info AS ci 
	LEFT JOIN Silver.erp_CUST_AZ12 AS CA ON ci.cst_key=CA.CID
	LEFT JOIN Silver.erp_LOC_A101 AS LA ON ci.cst_key=LA.CID;

GO

CREATE OR ALTER VIEW Gold.dim_products AS
SELECT
  ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt,pi.prd_key) AS PRODUCT_KEY,
  pi.prd_id AS PRODUCT_ID,
  pi.prd_key AS PRODUCT_NUMBER,
  pi.prd_nm AS PRODUCT_NAME,
  pi.cat_id AS CATEGORY_ID,
  PC.CAT AS CATEGORY,
  PC.SUBCAT AS SUBCATEGORY,
  PC.MAINTENANCE ,
  pi.prd_cost AS COST,
  pi.prd_line AS PRODUCT_LINE,
  pi.prd_start_dt AS START_DATE
  FROM Silver.crm_prd_info AS pi
  LEFT JOIN Silver.erp_PX_CAT_G1V2 AS PC
  ON pi.cat_id=PC.ID
  WHERE pi.prd_end_dt IS NULL;		--Taking only the current product data and filtering out the historical ones

GO

CREATE OR ALTER VIEW Gold.fact_sales AS
SELECT
  sd.sls_ord_num AS ORDER_NUMBER,
  pr.PRODUCT_KEY,
  cs.CUSTOMER_KEY,
  sd.sls_order_dt AS ORDER_DATE,
  sd.sls_ship_dt AS SHIPPING_DATE,
  sd.sls_due_dt AS DUE_DATE,
  sd.sls_sales AS SALES_AMOUNT,
  sd.sls_quantity AS QUANTITY,
  sd.sls_price AS PRICE
  FROM Silver.crm_sales_details AS sd
  LEFT JOIN Gold.dim_products AS pr
  ON sd.sls_prd_key=pr.PRODUCT_NUMBER
  LEFT JOIN Gold.dim_customers AS cs
  ON sd.sls_cust_id=cs.CUSTOMER_ID;

  

