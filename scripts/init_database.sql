/*
	OBJECTIVE: Creating Database named 'DataWarehouse' and its Schemas- Bronze, Silver, nad Gold.
	WARNING: Running this script will drop the entire 'DataWarehouse' database if it already exists.
	         Proceed with caution and ensure you have proper backups before running this script.
*/

USE master;
GO

--If the database already exists, it deletes it and creates a new one
--Sets single_user because if there are multiple connections/users active, dropping db becomes impossible
--rollback immediate ensures that all connections are dropped immediately and ongoing transactions are rolled back without any changes in db
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--GO is a batch separator in microsoft SQL, and not an SQL keyword
--a schema cannot be created unless it is the first line of a batch, thats why GO used
CREATE SCHEMA Bronze;
GO

CREATE SCHEMA Silver;
GO

CREATE SCHEMA Gold;
GO
