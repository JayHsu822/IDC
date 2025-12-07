USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[sp_ManageCustViewAccess]    Script Date: 2025/12/7 上午 11:02:07 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






/*
================================================================================
儲存程序名稱: sp_ManageCustViewAccess
版本: 2.0.0
建立日期: 2025-08-26
修改日期: 2025-09-12
作者: Jay
描述: 管理客戶視圖存取權限，支援新增或刪除代理人或同群組分享的權限設定。
      此版本包含完整的交易控制和權限驗證，確保資料庫權限操作的原子性與正確性。
      如果權限授予/撤銷失敗，整個操作將會回滾。

使用方式:
1. 新增代理權限：
   EXEC sp_ManageCustViewAccess 
       @CustViewId = '212DA30E-FF52-4AE3-B5F7-9BFE24E9E793', 
       @AccountId = 'CEA7EEAD-C1D8-452E-97BE-DB7F3D1D2E6A', 
       @Kind = 1, 
       @Oper = 1

2. 刪除代理權限：
   EXEC sp_ManageCustViewAccess 
       @CustViewId = '212DA30E-FF52-4AE3-B5F7-9BFE24E9E793', 
       @AccountId = 'CEA7EEAD-C1D8-452E-97BE-DB7F3D1D2E6A', 
       @Kind = 1, 
       @Oper = 2

參數說明:
@CustViewId - View的Id (NVARCHAR(36), 必填, GUID格式)
@AccountId - 代理人Id (NVARCHAR(36), 必填, GUID格式)
@Kind - 權限類型 (INT, 必填, 1=代理, 2=同群組分享)
@Oper - 操作類型 (INT, 必填, 1=新增, 2=刪除)

功能說明:
- 根據 CustViewId 取得對應的 ViewName 和 SchemaName (DbName欄位)
- 根據 AccountId 取得對應的 Organize 和 EmpNo 組合
- 新增操作：在單一交易內，插入權限記錄、授予資料庫 SELECT 權限，並驗證權限是否成功授予。
- 刪除操作：在單一交易內，刪除權限記錄、撤銷資料庫 SELECT 權限，並驗證權限是否成功撤銷。
- 任何步驟失敗，所有操作將會回滾。

版本歷程:
Jay  v1.0.0 (2025-08-26) - 初始版本，支援客戶視圖存取權限管理功能。
Jay  v2.0.0 (2025-09-12) - 新增交易控制、權限操作後驗證、使用者存在性檢查及強化的錯誤處理。
================================================================================
*/

CREATE OR ALTER       PROCEDURE [dbo].[sp_ManageCustViewAccess]
    @CustViewId NVARCHAR(36), --212DA30E-FF52-4AE3-B5F7-9BFE24E9E793
    @AccountId NVARCHAR(36),  --CEA7EEAD-C1D8-452E-97BE-DB7F3D1D2E6A
    @Kind INT, --1=代理, 2=同群組分享
    @Oper INT  --1=新增, 2=刪除
AS
BEGIN
    SET NOCOUNT ON;
	--使用idc要包一個SP去代執行IOpen的SP
	EXEC [iOPEN].[dbo].[sp_ManageCustViewAccess]
					@CustViewId = @CustViewId,
					@AccountId = @AccountId,
					@Kind = @Kind,
					@Oper = @Oper;    
	/*
    -- 宣告變數
    DECLARE @SchemaName NVARCHAR(255);
    DECLARE @ViewName NVARCHAR(255);
    DECLARE @AccEmp NVARCHAR(255);
    DECLARE @SqlCommand NVARCHAR(MAX);
    
    -- 宣告變數用於錯誤處理
    DECLARE @ErrorNumber INT;
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @RowCount INT = 0;
    
    BEGIN TRY
        -- 參數驗證
        IF @CustViewId IS NULL OR LEN(LTRIM(RTRIM(@CustViewId))) = 0
        BEGIN
            RAISERROR('參數 @CustViewId 不能為空值或空字串', 16, 1);
            RETURN;
        END
        
        IF @AccountId IS NULL OR LEN(LTRIM(RTRIM(@AccountId))) = 0
        BEGIN
            RAISERROR('參數 @AccountId 不能為空值或空字串', 16, 1);
            RETURN;
        END
        
        -- 驗證GUID格式
        IF TRY_CAST(@CustViewId AS UNIQUEIDENTIFIER) IS NULL
        BEGIN
            RAISERROR('參數 @CustViewId 必須為有效的GUID格式', 16, 1);
            RETURN;
        END
        
        IF TRY_CAST(@AccountId AS UNIQUEIDENTIFIER) IS NULL
        BEGIN
            RAISERROR('參數 @AccountId 必須為有效的GUID格式', 16, 1);
            RETURN;
        END
        
        IF @Kind NOT IN (1, 2)
        BEGIN
            RAISERROR('參數 @Kind 必須為 1(代理) 或 2(同群組分享)', 16, 1);
            RETURN;
        END
        
        IF @Oper NOT IN (1, 2)
        BEGIN
            RAISERROR('參數 @Oper 必須為 1(新增) 或 2(刪除)', 16, 1);
            RETURN;
        END

        -- 傳CustViewid進來,抓出viewname和SchemaName(r.DbName)
        SELECT @SchemaName = r.DbName, @ViewName = cv.viewname 
        FROM iDataCenter.dbo.tbCustView cv
        INNER JOIN iDataCenter.dbo.tbRes r ON cv.MasterId = r.Id AND cv.Enable = r.Enable
        WHERE cv.id = @CustViewId AND cv.enable = '1';

        -- 檢查是否找到對應的View
        IF @ViewName IS NULL OR @SchemaName IS NULL
        BEGIN
            RAISERROR('找不到對應的 CustView 或該 View 未啟用', 16, 1);
            RETURN;
        END

        -- 傳accountid進來,抓出Organize及EmpNo
        SELECT @AccEmp = Organize + '\' + Empno 
        FROM iDataCenter.dbo.tbSysAccount
        WHERE id = @AccountId AND enable = '1';
        
        -- 檢查是否找到對應的Account
        IF @AccEmp IS NULL
        BEGIN
            RAISERROR('找不到對應的 Account 或該 Account 未啟用', 16, 1);
            RETURN;
        END

        BEGIN TRANSACTION;

        -- 根據 @Oper 參數執行對應操作
        IF @Oper = 1  -- 新增
        BEGIN
            -- 插入權限記錄
            INSERT INTO [iOpen].[dbo].[UserDateRange_rls_unified]
            ([UserName], [StartDate], [EndDate], [IsADUser], [IsOwner], [ViewName])
            SELECT 
                @AccEmp,
                [StartDate],
                [EndDate],
                [IsADUser],
                [IsOwner],
                [ViewName]
            FROM [iOpen].[dbo].[UserDateRange_rls_unified]
            WHERE viewname = @ViewName 
                AND IsOwner = '1' 
                AND username <> 'IOPEN';
            
            SET @RowCount = @@ROWCOUNT;
            
            -- 授予資料庫權限前，檢查使用者是否存在
            IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @AccEmp)
            BEGIN
                RAISERROR('資料庫使用者 [%s] 不存在，無法授予權限。請系統管理員先建立對應的資料庫使用者。', 16, 1, @AccEmp);
            END

            -- 授予資料庫權限
            SET @SqlCommand = 'GRANT SELECT ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ViewName) + ' TO ' + QUOTENAME(@AccEmp);
            EXEC sp_executesql @SqlCommand;

            -- 驗證權限是否成功授予
            DECLARE @PermissionGranted BIT = 0;
            SELECT @PermissionGranted = 1
            FROM sys.database_permissions AS p
            JOIN sys.objects AS o ON p.major_id = o.object_id
            JOIN sys.schemas AS s ON o.schema_id = s.schema_id
            JOIN sys.database_principals AS u ON p.grantee_principal_id = u.principal_id
            WHERE p.class_desc = 'OBJECT_OR_COLUMN'
              AND p.permission_name = 'SELECT'
              AND p.state_desc = 'GRANT'
              AND s.name = @SchemaName
              AND o.name = @ViewName
              AND u.name = @AccEmp;

            IF @PermissionGranted = 0
            BEGIN
                RAISERROR('授予使用者 [%s] 對視圖 [%s].[%s] 的 SELECT 權限失敗，驗證未通過。交易已回滾。', 16, 1, @AccEmp, @SchemaName, @ViewName);
            END
            
            PRINT '成功新增權限設定，影響 ' + CAST(@RowCount AS NVARCHAR(10)) + ' 筆記錄';
            PRINT '已成功授予使用者 [' + @AccEmp + '] 對視圖 [' + @SchemaName + '].[' + @ViewName + '] 的 SELECT 權限';
        END
        ELSE IF @Oper = 2  -- 刪除
        BEGIN
            -- 刪除權限記錄
            DELETE FROM [iOpen].[dbo].[UserDateRange_rls_unified]
            WHERE viewname = @ViewName 
                AND Username = @AccEmp 
                AND IsOwner = '1';
            
            SET @RowCount = @@ROWCOUNT;
            
            -- 如果資料庫使用者存在，才執行撤銷權限操作
            IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @AccEmp)
            BEGIN
                -- 撤銷資料庫權限
                SET @SqlCommand = 'REVOKE SELECT ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ViewName) + ' FROM ' + QUOTENAME(@AccEmp);
                EXEC sp_executesql @SqlCommand;

                -- 驗證權限是否成功撤銷
                DECLARE @PermissionExists BIT = 0;
                SELECT @PermissionExists = 1
                FROM sys.database_permissions AS p
                JOIN sys.objects AS o ON p.major_id = o.object_id
                JOIN sys.schemas AS s ON o.schema_id = s.schema_id
                JOIN sys.database_principals AS u ON p.grantee_principal_id = u.principal_id
                WHERE p.class_desc = 'OBJECT_OR_COLUMN'
                  AND p.permission_name = 'SELECT'
                  AND p.state_desc = 'GRANT'
                  AND s.name = @SchemaName
                  AND o.name = @ViewName
                  AND u.name = @AccEmp;

                IF @PermissionExists = 1
                BEGIN
                    RAISERROR('撤銷使用者 [%s] 對視圖 [%s].[%s] 的 SELECT 權限失敗，驗證未通過。交易已回滾。', 16, 1, @AccEmp, @SchemaName, @ViewName);
                END
                
                PRINT '已成功撤銷使用者 [' + @AccEmp + '] 對視圖 [' + @SchemaName + '].[' + @ViewName + '] 的 SELECT 權限';
            END
            ELSE
            BEGIN
                 PRINT '資料庫使用者 [' + @AccEmp + '] 不存在，略過撤銷資料庫權限的步驟。';
            END
            
            PRINT '成功刪除權限設定，影響 ' + CAST(@RowCount AS NVARCHAR(10)) + ' 筆記錄';
        END

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        -- 檢查是否有活動中的交易，若有則回滾
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- 取得錯誤資訊
        SELECT 
            @ErrorNumber = ERROR_NUMBER(),
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();
        
        -- 記錄錯誤資訊
        PRINT '執行發生錯誤，交易已回滾:';
        PRINT '錯誤編號: ' + CAST(@ErrorNumber AS NVARCHAR(10));
        PRINT '錯誤訊息: ' + @ErrorMessage;
        PRINT '錯誤嚴重性: ' + CAST(@ErrorSeverity AS NVARCHAR(10));
        PRINT '錯誤狀態: ' + CAST(@ErrorState AS NVARCHAR(10));
        PRINT '參數資訊: CustViewId=' + ISNULL(@CustViewId, 'NULL') + 
                  ', AccountId=' + ISNULL(@AccountId, 'NULL') + 
                  ', Kind=' + CAST(@Kind AS NVARCHAR(10)) + 
                  ', Oper=' + CAST(@Oper AS NVARCHAR(10));
        
        -- 重新拋出錯誤，讓呼叫端知道執行失敗
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
        
    END CATCH*/
END
GO


