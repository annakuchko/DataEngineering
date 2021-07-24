USE [AdventureWorks2019]
USE [stg]
GO 

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'Production' )
    EXEC('CREATE SCHEMA [Production]');
GO

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'Sales' )
    EXEC('CREATE SCHEMA [Sales]');
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
DROP TABLE stg.Production.ProductSubcategory;