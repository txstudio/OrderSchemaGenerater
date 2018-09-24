/*
EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'OrderSchemaGenerater'
GO

USE [master]
GO

ALTER DATABASE [OrderSchemaGenerater]
	SET SINGLE_USER WITH ROLLBACK IMMEDIATE
GO

USE [master]
GO

DROP DATABASE [OrderSchemaGenerater]
GO

USE [master]
GO
*/
CREATE DATABASE [OrderSchemaGenerater]
GO

/*
	此設定與 Azure SQL Database 相同
	https://blogs.msdn.microsoft.com/sqlcat/2013/12/26/be-aware-of-the-difference-in-isolation-levels-if-porting-an-application-from-windows-azure-sql-db-to-sql-server-in-windows-azure-virtual-machine/
*/

--啟用 SNAPSHOT_ISOLATION
ALTER DATABASE [OrderSchemaGenerater]
	SET ALLOW_SNAPSHOT_ISOLATION ON
GO

--啟用 READ_COMMITTED_SNAPSHOT
ALTER DATABASE [OrderSchemaGenerater]
	SET READ_COMMITTED_SNAPSHOT ON
	WITH ROLLBACK IMMEDIATE
GO

USE [OrderSchemaGenerater]
GO

CREATE SCHEMA [Orders]
GO

CREATE SCHEMA [Events]
GO


--儲存預存程序錯誤的事件紀錄資料表
CREATE TABLE [Events].[EventDatabaseErrorLog] (
	[No]                INT IDENTITY(1, 1),
	[ErrorTime]         DATETIME DEFAULT (SYSDATETIMEOFFSET()),
	[ErrorDatabase]     NVARCHAR(100),
	[LoginName]         NVARCHAR(100),
	[UserName]          NVARCHAR(128),
	[ErrorNumber]       INT,
	[ErrorSeverity]     INT,
	[ErrorState]        INT,
	[ErrorProcedure]    NVARCHAR(130),
	[ErrorLine]         INT,
	[ErrorMessage]      NVARCHAR(MAX),
	
    CONSTRAINT [PK_Events_DatabaseErrorLog] PRIMARY KEY ([No] ASC)
)
GO


CREATE PROCEDURE [Events].[AddEventDatabaseError] 
    @No INT = 0 OUTPUT
AS
    DECLARE @seed INT

    SET NOCOUNT ON

    BEGIN TRY
        IF ERROR_NUMBER() IS NULL
        BEGIN
            RETURN
        END

        --
        --如果有進行中的交易正在使用時不進行記錄
        -- (尚未 rollback 或 commit)
        --
        IF XACT_STATE() = (- 1)
        BEGIN
            RETURN
        END

        INSERT INTO [Events].[EventDatabaseErrorLog] (
            [ErrorDatabase]
            ,[LoginName]
            ,[UserName]
            ,[ErrorNumber]
            ,[ErrorSeverity]
            ,[ErrorState]
            ,[ErrorProcedure]
            ,[ErrorLine]
            ,[ErrorMessage]
            )
        VALUES (
            CONVERT(NVARCHAR(100), DB_NAME())
            ,CONVERT(NVARCHAR(100), SYSTEM_USER)
            ,CONVERT(NVARCHAR(128), CURRENT_USER)
            ,ERROR_NUMBER()
            ,ERROR_SEVERITY()
            ,ERROR_STATE()
            ,ERROR_PROCEDURE()
            ,ERROR_LINE()
            ,ERROR_MESSAGE()
            )
    END TRY

    BEGIN CATCH
        RETURN (- 1)
    END CATCH
GO

--產生 CHAR(15) 訂單編號的純量函數
--	YYYYMMDD0000000
CREATE FUNCTION [Orders].[OrderSchemaGenerater] 
(
	@CurrentDate	DATE
	,@Index			SMALLINT
)
RETURNS CHAR(15)
BEGIN
	DECLARE @Code		CHAR(7)
	DECLARE @VarCode	VARCHAR(7)
	DECLARE @Prefix		CHAR(8)
	DECLARE @Length		SMALLINT

	SET @Code = '0000000'
	SET @VarCode = CONVERT(VARCHAR(8),@Index)
	SET @Prefix = CONVERT(CHAR(8),@CurrentDate,112)
	SET @Length = LEN(@Code)

	SET @Code = RIGHT((@Code + @VarCode),@Length)

	--RETURN (YYYYMMDD + 0000000)
	RETURN (@Prefix + @Code)
END
GO

/* 儲存訂單編號的資料表 */
CREATE TABLE [Orders].[OrderSchemaBuffer]
(
	[PresentDate]	DATE NOT NULL,
	[Index]			SMALLINT NOT NULL,
	[Schema]		AS (
						[Orders].[OrderSchemaGenerater]([PresentDate],[Index])
					),

	CONSTRAINT [pk_Orders_OrderSchemaBuffer]
		PRIMARY KEY([PresentDate])
)
GO

--訂單資料表主索引鍵使用序列
CREATE SEQUENCE [Orders].[OrderMainSeq]
	START WITH 1
	INCREMENT BY 1
GO

/* 訂單編號主資料表 */
CREATE TABLE [Orders].[OrderMains]
(
	[No]			INT NOT NULL,
	[Schema]		CHAR(15),
	[OrderDate]		DATETIMEOFFSET DEFAULT (SYSDATETIMEOFFSET())

	CONSTRAINT [pk_Orders_OrderMains] PRIMARY KEY ([No]),

	CONSTRAINT [un_Orders_OrderMains_Schema] UNIQUE ([Schema])
)
GO

/* 初始化訂單編號儲存資料表的資料列內容 */
INSERT INTO [Orders].[OrderSchemaBuffer] ([PresentDate],[Index]) VALUES (DATEADD(DAY,-2,GETDATE()),999)
INSERT INTO [Orders].[OrderSchemaBuffer] ([PresentDate],[Index]) VALUES (DATEADD(DAY,-1,GETDATE()),999)
INSERT INTO [Orders].[OrderSchemaBuffer] ([PresentDate],[Index]) VALUES (GETDATE(),1)
INSERT INTO [Orders].[OrderSchemaBuffer] ([PresentDate],[Index]) VALUES (DATEADD(DAY,1,GETDATE()),1)
GO

/* 取得新一筆訂單要使用的訂單編號 */
CREATE PROCEDURE [Orders].[GetNewOrderSchema]
	@CurrentDate	DATE,
	@OutSchema		CHAR(15) OUT,
	@Success		BIT OUT
AS
	DECLARE @IsTransaction	BIT
	DECLARE @NextDate		DATE
	DECLARE @output			TABLE
	(
		[PresentDate]		DATE,
		[Index]				SMALLINT,

		PRIMARY KEY ([PresentDate])
	)

	SET @NextDate = DATEADD(DAY,1,@CurrentDate)

	BEGIN TRY
		IF XACT_STATE() = 0
		BEGIN
			BEGIN TRANSACTION
			SET @IsTransaction = 1
		END

		UPDATE [Orders].[OrderSchemaBuffer]
			SET [Index] = [Index] + 1
		OUTPUT DELETED.[PresentDate]
			,DELETED.[Index]
		INTO @output
		WHERE [PresentDate] = @CurrentDate

		--取得要新增到訂單的訂單編號
		SET @OutSchema = (
			SELECT [Orders].[OrderSchemaGenerater]([PresentDate],[Index]) [OutSchema]
			FROM @output
		)

		--清除逾時的訂單緩衝資料
		DELETE FROM [Orders].[OrderSchemaBuffer]
		WHERE [PresentDate] < DATEADD(DAY,-1,@CurrentDate)


		--預先建立下一個日期的訂單緩衝資料
		IF NOT EXISTS (
			SELECT * FROM [Orders].[OrderSchemaBuffer]
			WHERE [PresentDate] = @NextDate
		)
		BEGIN
			INSERT INTO [Orders].[OrderSchemaBuffer] (
				[PresentDate]
				,[Index]
			) VALUES (
				@NextDate
				,1
			)
		END
		
		IF @IsTransaction = 1
		BEGIN
			COMMIT TRANSACTION
		END

		SET @Success = 1
	END TRY

	BEGIN CATCH
		IF @IsTransaction = 1
		BEGIN
			ROLLBACK TRANSACTION
		END

		EXEC [Events].[AddEventDatabaseError]

		SET @Success = 0
	END CATCH
GO

--建立訂單資料的預存程序/部分
CREATE PROCEDURE [Orders].[AddOrder]
	@CurrentDate	DATE,
	@Success		BIT OUT
AS
BEGIN TRY
	BEGIN TRANSACTION
	
	DECLARE @No				INT
	DECLARE @OutSchema		CHAR(15)

	IF @CurrentDate IS NULL
		SET @CurrentDate = GETDATE()

	--取得此筆訂單的訂單編號
	EXEC [Orders].[GetNewOrderSchema] 
		@CurrentDate
		, @OutSchema OUT
		, @Success OUT

	SET @No = NEXT VALUE FOR [Orders].[OrderMainSeq]

	INSERT INTO [Orders].[OrderMains] (
		[No]
		,[Schema]
	) VALUES (
		@No
		,@OutSchema
	)

	SET @Success = 1

	COMMIT TRANSACTION
END TRY

BEGIN CATCH
	ROLLBACK TRANSACTION

	EXEC [Events].[AddEventDatabaseError]

	SET @Success = 0
END CATCH
GO