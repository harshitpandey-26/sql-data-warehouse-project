/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


--BULK INSERT :- RATHER THAN INSERTING VALUES IN TABLE ROW BY ROW, WE ARE INSERTING IT IN BULK AND IN ONE GO FROM
--SOURCE TO TABLE DIRECTLY.

---#WRITING BULK INSERT QUERY
---**SCRIPT FOR FULL LOAD

-->>** HINT :- STORED FREQUENTLY USED SQL CODE IN STORE DPROCEDURES IN DATABASE.

--#ADDING PRINT FOR BETTER DEBUGGING AND TRACKING DOWN TH ACTIONS.

-->> TO LOAD THE SCRIPT WE JUST NEED TO WRITE QUERY:- EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS -- STEP AFTER WRITING THE WHOLE QUERY

BEGIN
	--MOST IMP:- TRACK ETL DURATION:- HELPS TO IDENTIFY BOTTLENECKS, OPTIMIZE PERFORMANCE, MONITOR TRENDS, DETECT ISSUES.
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME,@end_batch_time DATETIME;
	BEGIN TRY -- NEXT STEP AFTER PROCEDURE :- ADDING TRY CATCH - FOR LOGGING ERRORS AND PROPER DEBUGGING
		SET @start_batch_time = GETDATE();
		PRINT '===============================================';
		PRINT 'Loading BRONZE LAYER';
		PRINT '===============================================';
	
		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Table';
		PRINT '-----------------------------------------------';
	
		
		--TABLE bronze.crm_cust_info 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info ;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\Lenovo\Desktop\Sheriyans Coding\SQL_Learnig\SQL_DATA_WAREHOUSE_PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -------------'

		--DO QUALITY CHECK OF DATA:- CHECK THAT THE DATA HAS NOT SHIFTED AND IS IN THE CORRECT COLUMNS.
		--JUST TO CONFIRM THE INSERTION:- SELECT COUNT(*) FROM bronze.crm_cust_info ;

		--IF THINGS ARE GETTING WRONG:- HERE WE USING TRUNCATE AND INSERT METHOD SO JUST TRUNCATE THE TABLE AND AGAIN INSERT.

		--TABLE bronze.crm_prd_info 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Lenovo\Desktop\Sheriyans Coding\SQL_Learnig\SQL_DATA_WAREHOUSE_PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -------------'
		
		-->>**TABLE bronze.crm_sales_details
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT 'Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Lenovo\Desktop\Sheriyans Coding\SQL_Learnig\SQL_DATA_WAREHOUSE_PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -------------'

		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';
	

		-->>**TABLE bronze.erp_cust_az12_stage
		-- DO THE BULK INSERT IN THE STAGING TABLE TO AVOID THE SAME DATE FORMAT PROBLEM.
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12_stage';
		TRUNCATE TABLE bronze.erp_cust_az12_stage;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12_stage';
		BULK INSERT bronze.erp_cust_az12_stage
		FROM 'C:\Users\Lenovo\Desktop\Sheriyans Coding\SQL_Learnig\SQL_DATA_WAREHOUSE_PROJECT\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -------------'

		-->>**TABLE bronze.erp_cust_az12
		--HERE I JUST DO INSERT BECAUSE INSERTED VALUES FROM STAGING TABLE TO MAIN TABLE AVOIDED THE DATE FORMAT PROBLEM.
		SET @start_time = GETDATE();
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12 FROM bronze.erp_cust_az12_stage';
		INSERT INTO bronze.erp_cust_az12 (cid, bdate, gen)
		SELECT
			cid,
			TRY_CONVERT(DATE, bdate, 105),
			gen
		FROM bronze.erp_cust_az12_stage;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -------------'

    -->>**TABLE bronze.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Lenovo\Desktop\Sheriyans Coding\SQL_Learnig\SQL_DATA_WAREHOUSE_PROJECT\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -------------'

		-->>**TABLE bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Lenovo\Desktop\Sheriyans Coding\SQL_Learnig\SQL_DATA_WAREHOUSE_PROJECT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -------------'

		SET @end_batch_time = GETDATE();

		PRINT '====================================================='
		PRINT 'LOADING BRONZE LAYER COMPLETED.'
		PRINT '	    - TIME TOOK TO LOAD THE WHOLE BATCH: ' + CAST(DATEDIFF(second,@start_batch_time,@end_batch_time) AS
		NVARCHAR) + ' seconds';
		PRINT '====================================================='
		
	END TRY
	BEGIN CATCH
		PRINT '============================================'
		PRINT 'ERROR OCCURRED DURIG LOADING BRONZE LAYER'
		PRINT '============================================'
		PRINT 'ERROR_MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR_NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR_STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END


