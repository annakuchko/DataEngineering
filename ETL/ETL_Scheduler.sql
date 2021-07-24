USE [msdb]
GO

/****** Object:  Job [etl_adventureWorks2019]    Script Date: 13/07/2021 23:30:30 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 13/07/2021 23:30:30 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'etl_adventureWorks2019', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'DESKTOP-QQHLHJV\Asus', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [full_etl]    Script Date: 13/07/2021 23:30:32 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'full_etl', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=1, 
		@retry_interval=1, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE [AdventureWorks2019]
USE [stg]
GO 

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N''Production'' )
    EXEC(''CREATE SCHEMA [Production]'');
GO

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N''Sales'' )
    EXEC(''CREATE SCHEMA [Sales]'');
GO

SELECT * INTO stg.Sales.SalesOrderDetail FROM 
[AdventureWorks2019].Sales.SalesOrderDetail
SELECT * INTO stg.Sales.SalesOrderHeader FROM 
[AdventureWorks2019].Sales.SalesOrderHeader
SELECT * INTO stg.Sales.SpecialOffer FROM 
[AdventureWorks2019].Sales.SpecialOffer;
GO


SELECT * INTO stg.Production.Product FROM 
[AdventureWorks2019].Production.Product;
SELECT * INTO stg.Production.ProductCategory FROM 
[AdventureWorks2019].Production.ProductCategory
SELECT * INTO stg.Production.ProductSubcategory FROM 
[AdventureWorks2019].Production.ProductSubcategory
GO

USE [ods]
GO
DROP TABLE IF EXISTS [ods].[dbo].Fact_SalesOrderDetail;
CREATE TABLE [ods].[dbo].Fact_SalesOrderDetail (
	SalesOrderDetailKey int PRIMARY KEY,
	ProductKey INT NOT NULL,
	SpecialOfferKey INT NOT NULL,
	SalesOrderDateKey INT NOT NULL,
	OrderQty INT NOT NULL,
	UnitPrice MONEY NOT NULL,
	UnitPriceDiscount MONEY NOT NULL,
);
INSERT INTO [ods].[dbo].Fact_SalesOrderDetail SELECT
SD.SalesOrderDetailID AS SalesOrderDetailKey,
SD.ProductID AS ProductKey,
SD.SpecialOfferID AS SpecialOfferKey,
CONVERT(INT, CONVERT(VARCHAR(8),SH.OrderDate,112)) AS SalesOrderDateKey,
SD.OrderQty,
SD.UnitPrice,
SD.UnitPriceDiscount
FROM [stg].Sales.SalesOrderDetail SD
	LEFT JOIN [stg].Sales.SalesOrderHeader SH ON SD.SalesOrderID = SH.SalesOrderID;
 SELECT * from [ods].[dbo].Fact_SalesOrderDetail;
DROP TABLE IF EXISTS [ods].[dbo].Dim_Product;
CREATE TABLE [ods].[dbo].Dim_Product (
	ProductKey INT PRIMARY KEY,
	ProductName	nvarchar(50) NOT NULL,
	Color nvarchar(50),
	StandardCost MONEY NOT NULL,
	ListPrice MONEY NOT NULL,
	Size nvarchar(5),
	Weight decimal(8,2),
	ProductLine nchar(2),
	Class nchar(2),
	Style nchar(2),
	ProductSubcategory nvarchar(50),
);
INSERT INTO [ods].[dbo].Dim_Product 
SELECT
PP.ProductID AS ProductKey,
PP.Name AS ProductName,
PP.Color,
PP.StandardCost,
PP.ListPrice,
PP.Size,
PP.Weight,
PP.ProductLine,
PP.Class,
PP.Style,
PCJ.Name as ProductSubcategory
FROM [stg].Production.Product PP LEFT JOIN
(SELECT PC.Name, PSC.ProductCategoryID, PSC.ProductSubcategoryID FROM 
	([stg].Production.ProductCategory PC LEFT JOIN 
	[stg].Production.ProductSubcategory PSC 
				ON PC.ProductCategoryID = PSC.ProductCategoryID)) AS PCJ
ON PP.ProductSubcategoryID = PCJ.ProductSubcategoryID;
SELECT * from [ods].[dbo].Dim_Product;
DROP TABLE IF EXISTS [ods].[dbo].Dim_SpecialOffer;
CREATE TABLE [ods].[dbo].Dim_SpecialOffer (
	SpecialOfferKey INT PRIMARY KEY,
	OfferName nvarchar(50) NOT NULL,
	DiscountPct SMALLMONEY NOT NULL,
	Type nvarchar(50) NOT NULL,
	Category nvarchar(50) NOT NULL
	);
INSERT INTO [ods].[dbo].Dim_SpecialOffer
SELECT SpecialOfferID AS SpecialOfferKey,
Description as OfferName,
DiscountPct,
Type,
Category 
FROM [stg].Sales.SpecialOffer;
SELECT * from [ods].[dbo].Dim_SpecialOffer; 
GO

DROP TABLE stg.Sales.SalesOrderDetail;
DROP TABLE stg.Sales.SalesOrderHeader;
DROP TABLE stg.Sales.SpecialOffer;

DROP TABLE stg.Production.Product;
DROP TABLE stg.Production.ProductCategory;
DROP TABLE stg.Production.ProductSubcategory;', 
		@database_name=N'AdventureWorks2019', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'etl_scheduler', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210712, 
		@active_end_date=20210718, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'64762728-49df-47b3-9359-640bf62b8ba0'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


