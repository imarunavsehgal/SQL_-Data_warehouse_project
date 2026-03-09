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

exec bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    declare @start_time datetime, @end_time DATETIME ,@batch_start_time DATETIME, @batch_end_time DATETIME
Begin Try
SET @batch_start_time = GETDATE();

PRINT '=============================================='
PRINT 'Loading Bronze Layer'
PRINT '=============================================='

PRINT '----------------------------------------------'
PRINT 'Loading CRM Tables'
PRINT '----------------------------------------------'

    set @start_time=GETDATE();
PRINT '>> Truncating Table : bronze.crm_cust_info'
TRUNCATE TABLE bronze.crm_cust_info

PRINT '>> Inserting Data Into : bronze.crm_cust_info'
BULK INSERT bronze.crm_cust_info
FROM 'E:\data analysis with python\lwarning sql\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
)
    set @end_time=GETDATE();
    print'>> Load Duration ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' Seconds';
    
PRINT '----------------------------------------------'

set @end_time=GETDATE();
PRINT '>> Truncating Table : bronze.crm_prd_info'
PRINT '----------------------------------------------'

TRUNCATE TABLE bronze.crm_prd_info

PRINT '>> Inserting Data Into : bronze.crm_prd_info'
BULK INSERT bronze.crm_prd_info
FROM 'E:\data analysis with python\lwarning sql\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
)
 set @end_time=GETDATE();
    print'>> Load Duration ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' Seconds';
    
PRINT '----------------------------------------------'

set @end_time=GETDATE();
PRINT '>> Truncating Table : bronze.crm_sales_details'
PRINT '----------------------------------------------'

TRUNCATE TABLE bronze.crm_sales_details

PRINT '>> Inserting Data Into : bronze.crm_sales_details'
BULK INSERT bronze.crm_sales_details
FROM 'E:\data analysis with python\lwarning sql\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
)
 set @end_time=GETDATE();
    print'>> Load Duration ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' Seconds';
    
PRINT '----------------------------------------------'

set @end_time=GETDATE();
PRINT '>> Truncating Table : bronze.erp_cust_az12'
PRINT '----------------------------------------------'

TRUNCATE TABLE bronze.erp_cust_az12

PRINT '>> Inserting Data Into : bronze.erp_cust_az12'
BULK INSERT bronze.erp_cust_az12
FROM 'E:\data analysis with python\lwarning sql\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
)
 set @end_time=GETDATE();
    print'>> Load Duration ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' Seconds';
    
PRINT '----------------------------------------------'

set @end_time=GETDATE();
PRINT '>> Truncating Table : bronze.erp_loc_a101'
PRINT '----------------------------------------------'

TRUNCATE TABLE bronze.erp_loc_a101

PRINT '>> Inserting Data Into : bronze.erp_loc_a101'
BULK INSERT bronze.erp_loc_a101
FROM 'E:\data analysis with python\lwarning sql\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
)
 set @end_time=GETDATE();
    print'>> Load Duration ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' Seconds';
    
PRINT '----------------------------------------------'

set @end_time=GETDATE();
PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2'
PRINT '----------------------------------------------'

TRUNCATE TABLE bronze.erp_px_cat_g1v2

PRINT '>> Inserting Data Into : bronze.erp_px_cat_g1v2'
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'E:\data analysis with python\lwarning sql\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
)
 set @end_time=GETDATE();
    print'>> Load Duration ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' Seconds';
    
    SET @batch_end_time = GETDATE();
    PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

end try 
begin catch 
    Print' ============================='
    
    Print' error Occured during Loading Bronze layer'
    print' Error Message ' + Error_message();
    print' Error Message ' + cast(Error_number() as Nvarchar);
    print' Error Message ' + cast(Error_state()as Nvarchar);
    Print' ============================='
end catch
END
