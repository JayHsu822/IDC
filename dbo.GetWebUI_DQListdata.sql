USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[GetWebUI_DQListdata]    Script Date: 2025/12/7 上午 11:11:41 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO








CREATE OR ALTER PROCEDURE [dbo].[GetWebUI_DQListdata]
    @TB_NM NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;

        -- 執行日誌記錄存儲過程
        EXEC [iLog].[dbo].[sp_GetWebUI_DQListdata_WithLogging] @TB_NM;
    
        -- 主查詢
        SELECT 
            A.DQ_Seq, 
            A.DQ_Description, 
            STRING_AGG(CONVERT(varchar(10), DQ_No), ',') AS DQ_No, 
            Max(isnull(B.Enable,'0')) AS Enable
        FROM (
            SELECT 
                b.TB_NM, 
                b.TB_OWNER, 
                A.DQ_Seq, 
                A.DQ_Description 
            FROM [dbo].[iPJT_WebUI_DQ_Config] A
            CROSS JOIN [dbo].[iPJT_WebUI_DQTB_LIST] B
        ) A
        LEFT JOIN (
            SELECT DISTINCT 
                A.TB_NM, 
                A.WEB_DQ_No,  
                CASE WHEN (A.WEB_DQ_No = '6' AND A.Enable = '1') OR A.WEB_DQ_No <> '6' THEN A.Exception_Type+A.DQ_NBR ELSE NULL END AS DQ_No, 
                A.Enable 
            FROM [dbo].[iPJT_FinWeb_UI_Raw_DQ_LIST] A 
            WHERE A.Enable in ('1','0')
        ) B
        ON A.TB_NM = B.TB_NM
        AND SUBSTRING(A.DQ_Seq, 3, 1) = B.WEB_DQ_No
        WHERE A.TB_NM = @TB_NM
        GROUP BY A.DQ_Seq, A.DQ_Description
        ORDER BY A.DQ_Seq;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Rollback the transaction if an error occurs
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Declare local variables to store error information
        DECLARE 
            @ErrorMessage NVARCHAR(4000),
            @ErrorSeverity INT,
            @ErrorState INT,
            @ErrorNumber INT,
            @ErrorLine INT;

        -- Capture the error details
        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE(),
            @ErrorNumber = ERROR_NUMBER(),
            @ErrorLine = ERROR_LINE();

        -- Log the error 
        INSERT INTO [iLog].[dbo].[ErrorLog] (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage,
            ErrorDateTime
        )
        VALUES (
            @ErrorNumber,
            @ErrorSeverity,
            @ErrorState,
            OBJECT_NAME(@@PROCID),
            @ErrorLine,
            @ErrorMessage,
            GETDATE()
        );

        -- Re-throw the error to the calling process
        RAISERROR (
            @ErrorMessage, -- Message text
            @ErrorSeverity, -- Severity
            @ErrorState -- State
        );
    END CATCH;
END
GO


