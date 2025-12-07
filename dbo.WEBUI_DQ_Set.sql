USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[WEBUI_DQ_Set]    Script Date: 2025/12/7 上午 11:06:35 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[WEBUI_DQ_Set]
    @TB_NM NVARCHAR(100),
    @SetStatus CHAR(1) -- '1:更新Set時間 0:清空Set時間 
AS
BEGIN
    -- 錯誤處理
    SET NOCOUNT ON;
    BEGIN TRY
        -- 依據 SetStatus 進行不同的更新
        IF @SetStatus = '1'
        BEGIN
            -- 更新時間
            UPDATE tbRes
            SET DQSetTime = GETDATE()  -- SQL Server 現在時間
            -- 或用 CURRENT_TIMESTAMP
            WHERE ResName = @TB_NM;
        END
        ELSE IF @SetStatus = '0'
        BEGIN
            -- 清空時間
            UPDATE tbRes
            SET DQSetTime = NULL
            WHERE ResName = @TB_NM;
        END
        
        -- 檢查是否更新成功
        IF @@ROWCOUNT > 0
            PRINT '更新成功'
        ELSE
            PRINT '未找到記錄'
    END TRY
    BEGIN CATCH
        PRINT '更新發生錯誤：' + ERROR_MESSAGE()
    END CATCH
END
GO


