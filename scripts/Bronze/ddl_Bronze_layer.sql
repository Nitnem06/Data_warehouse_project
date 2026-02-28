/*
 PURPOSE: Creating Bronze tables using SQL.
          This script focuses on creating data tables, dropping existing tables if they already exist in the databse.
          Writing DDDL queries and loading data into the bronze layer tables.
*/
USE DataWarehouse;
GO 

CREATE OR ALTER PROCEDURE Bronze.loading_data AS
BEGIN
	BEGIN TRY
		-- This ensures that is the table already exists, it drops to to avoid any error
		--'U' stands for user defined table here
		 IF OBJECT_ID ('Bronze.crm_cust_info', 'U') IS NOT NULL
			DROP TABLE Bronze.crm_cust_info;
		 CREATE TABLE Bronze.crm_cust_info(
			cst_id INT,
			cst_key VARCHAR(10),
			cst_firstname NVARCHAR(25),
			cst_lastname NVARCHAR(25),
			cst_marital_status VARCHAR(10),
			cst_gndr VARCHAR(10),
			cst_create_date DATE
		 );
	 
		 IF OBJECT_ID ('Bronze.crm_prd_info', 'U') IS NOT NULL
			DROP TABLE Bronze.crm_prd_info;
		 CREATE TABLE Bronze.crm_prd_info(
			prd_id INT,
			prd_key VARCHAR(25),
			prd_nm VARCHAR(50),
			prd_cost INT,
			prd_line VARCHAR(10),
			prd_start_dt DATE,
			prd_end_dt DATE
		 );
	 
		 IF OBJECT_ID ('Bronze.crm_sales_details', 'U') IS NOT NULL
			DROP TABLE Bronze.crm_sales_details;
		 CREATE TABLE Bronze.crm_sales_details(
			sls_ord_num NVARCHAR(25),
			sls_prd_key NVARCHAR(25),
			sls_cust_id INT,
			sls_order_dt INT,
			sls_ship_dt INT,
			sls_due_dt INT,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT
		 );
	 
		 IF OBJECT_ID ('Bronze.erp_CUST_AZ12', 'U') IS NOT NULL
			DROP TABLE Bronze.erp_CUST_AZ12;
		 CREATE TABLE Bronze.erp_CUST_AZ12(
			CID VARCHAR(25),
			BDATE DATE,
			GEN VARCHAR(10)
		 );
	 
		 IF OBJECT_ID ('Bronze.erp_LOC_A101', 'U') IS NOT NULL
			DROP TABLE Bronze.erp_LOC_A101;
		 CREATE TABLE Bronze.erp_LOC_A101(
			CID VARCHAR(25),
			CNTRY VARCHAR(25)
		 );
	 
		 IF OBJECT_ID ('Bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
			DROP TABLE Bronze.erp_PX_CAT_G1V2;
		 CREATE TABLE Bronze.erp_PX_CAT_G1V2(
			ID VARCHAR(25),
			CAT VARCHAR(25),
			SUBCAT VARCHAR(25),
			MAINTENANCE VARCHAR(25)
		 );
	 
		 TRUNCATE TABLE Bronze.crm_cust_info;
		 BULK INSERT Bronze.crm_cust_info FROM 'C:\Users\Nitnem Kaur Juneja\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' 
		 WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR = '0x0d0a',
			CODEPAGE='65001',
			TABLOCK
		 );
		 PRINT('LOADING DATA FROM crm FILE');
		 PRINT('Data bulk inserted into Bronze.crm_cust_info');

		 TRUNCATE TABLE Bronze.crm_prd_info;
		 BULK INSERT Bronze.crm_prd_info FROM 'C:\Users\Nitnem Kaur Juneja\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
		 WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR = '0x0d0a',
			TABLOCK
		 );
		 PRINT('Data bulk inserted into Bronze.crm_prd_info');

		 TRUNCATE TABLE Bronze.crm_sales_details;
		 BULK INSERT Bronze.crm_sales_details FROM 'C:\Users\Nitnem Kaur Juneja\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		 WITH (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 ROWTERMINATOR = '0x0d0a',
			 CODEPAGE = '65001',
			 TABLOCK
		 );
		 PRINT('Data bulk inserted into Bronze.crm_sales_details');

		 TRUNCATE TABLE Bronze.erp_CUST_AZ12;
		 BULK INSERT Bronze.erp_CUST_AZ12 FROM 'C:\Users\Nitnem Kaur Juneja\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
		 WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR = '0x0d0a',
			TABLOCK
		 );
		 PRINT('LOADING DATA FROM erp FILE');
		 PRINT('Data bulk inserted into Bronze.erp_CUST_AZ12');

		 TRUNCATE TABLE Bronze.erp_LOC_A101;
		 BULK INSERT Bronze.erp_LOC_A101 FROM 'C:\Users\Nitnem Kaur Juneja\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' 
		 WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR = '0x0d0a',
			TABLOCK
		 );
		 PRINT('Data bulk inserted into Bronze.erp_LOC_A101');

		 TRUNCATE TABLE Bronze.erp_PX_CAT_G1V2;
		 BULK INSERT Bronze.erp_PX_CAT_G1V2 FROM 'C:\Users\Nitnem Kaur Juneja\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
		 WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			ROWTERMINATOR = '0x0d0a',
			TABLOCK
		 );
		 PRINT('Data bulk inserted into Bronze.erp_PX_CAT_G1V2');
	 
		SELECT COUNT(*) FROM Bronze.crm_cust_info;
		SELECT COUNT(*) FROM Bronze.crm_prd_info;
		SELECT COUNT(*) FROM Bronze.crm_sales_details;
		SELECT COUNT(*) FROM Bronze.erp_CUST_AZ12;
		SELECT COUNT(*) FROM Bronze.erp_LOC_A101;
		SELECT COUNT(*) FROM Bronze.erp_PX_CAT_G1V2;
	
	END TRY
	BEGIN CATCH
		PRINT('*****AN ERROR OCCURED DURING LOADING BRONZE LAYER*****');
		PRINT('Error Message:'+ERROR_MESSAGE());
		PRINT('Error Message:'+CAST (ERROR_NUMBER() AS NVARCHAR));
	END CATCH
END;
GO
