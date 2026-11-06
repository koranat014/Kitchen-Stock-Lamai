-- Run this script while connected to the SQL Server instance used by the app.
-- SQL Server chooses the database file locations from the instance defaults.
IF DB_ID(N'KitchenStock') IS NULL
BEGIN
    CREATE DATABASE [KitchenStock];
END;
GO

USE [KitchenStock];
GO

IF OBJECT_ID(N'dbo.Users', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Users (
        Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Username nvarchar(50) NOT NULL UNIQUE,
        PasswordHash nvarchar(255) NOT NULL,
        Role nvarchar(20) NOT NULL
    );
END;
GO

IF OBJECT_ID(N'dbo.Items', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Items (
        Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Name nvarchar(200) NOT NULL,
        Qty decimal(12,3) NOT NULL CONSTRAINT DF_Items_Qty DEFAULT (0),
        Unit nvarchar(30) NOT NULL,
        Category nvarchar(100) NOT NULL,
        UpdatedAt datetime2(3) NOT NULL CONSTRAINT DF_Items_UpdatedAt DEFAULT (SYSUTCDATETIME())
    );
END;
GO

IF OBJECT_ID(N'dbo.Requisitions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Requisitions (
        Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ItemId int NOT NULL,
        ItemName nvarchar(200) NOT NULL,
        Unit nvarchar(30) NOT NULL,
        Qty decimal(12,3) NOT NULL,
        ChefName nvarchar(100) NOT NULL,
        CreatedAt datetime2(3) NOT NULL CONSTRAINT DF_Requisitions_CreatedAt DEFAULT (SYSUTCDATETIME()),
        Status nvarchar(20) NOT NULL CONSTRAINT DF_Requisitions_Status DEFAULT (N'pending'),
        PreparedAt datetime2(3) NULL
    );
    CREATE INDEX IX_Requisitions_Status_CreatedAt ON dbo.Requisitions(Status, CreatedAt DESC, Id DESC);
END;
GO
