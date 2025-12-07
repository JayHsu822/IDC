USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[UpdatetbPublishWhere]    Script Date: 2025/12/7 上午 11:04:46 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







CREATE OR ALTER PROCEDURE [dbo].[UpdatetbPublishWhere]
    @CustViewId VARCHAR(36) = 'ALL' -- 默認為 ALL
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;

        IF @CustViewId = 'ALL'
        BEGIN
            UPDATE A
            SET A.PublishWhere = ' WHERE ' + B.PublishWhere
            FROM [iDataCenter].[dbo].[tbPublish] A
            INNER JOIN (
                    SELECT A.[Id],
                           A.[CustViewId],
                            CASE WHEN Frequency = '1' THEN
                                    ' AND ( ShareDT <= ''' +
                                    CASE 
                                        -- 當天是PDay號且時間超過PTime點，或是PDay號之後的日期
                                        WHEN (DAY(GETDATE()) = A.PDay AND DATEPART(HOUR, GETDATE()) >= A.PTime) 
                                             OR DAY(GETDATE()) > PDay
                                        THEN FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM')
                                        -- 未到PDay號或當天未到PTime點
                                        ELSE FORMAT(DATEADD(MONTH, -2, GETDATE()), 'yyyyMM')
                                    END + ''')'
                                ELSE '' END
                                AS billing_month,
                                '(' +
                           STRING_AGG(CASE 
                                        WHEN LEN(B.StartYM) > 0 AND LEN(B.VariableId) = 0 THEN '(ShareDT BETWEEN ''' + trim(B.StartYM) + ''' AND ''' + trim(B.EndYM) + ''')' 
										WHEN LEN(B.VariableId) > 0 THEN '(ShareDT BETWEEN ''' + trim(C.StartYM) + ''' AND ''' + trim(C.EndYM) + ''')' 
                                       END, ' OR ') 
                                       + ')'AS PublishWhere
                    FROM [iDataCenter].[dbo].[tbPublish] A
                    LEFT JOIN [iDataCenter].[dbo].[tbPublishPeriod] B 
					ON A.Id = B.PublishId and A.Enable = b.Enable
                    LEFT JOIN [iDataCenter].[dbo].[vTimeVariable] C 
					ON B.VariableId = C.Id AND B.Enable = C.Enable
				    AND CASE WHEN CAST(DAY(GETDATE()) AS VARCHAR) + CAST(DATEPART(hour, GETDATE()) AS VARCHAR) >= 
                        CAST(A.PDay AS VARCHAR) + CASE WHEN A.PTime = '0' THEN '00' ELSE CAST(A.PTime AS VARCHAR) END THEN '1' ELSE '0' END = C.Var_DT_Open
                    WHERE A.Enable = '1' 
                GROUP BY A.Id, A.CustViewId, A.PDay, A.PTime, A.Frequency
            ) B ON A.Id = B.Id AND A.CustViewId = B.CustViewId
            WHERE B.PublishWhere IS NOT NULL;
        END
        ELSE
        BEGIN
            UPDATE A
            SET A.PublishWhere = ' WHERE ' + B.PublishWhere /*+ B.billing_month*/
            FROM [iDataCenter].[dbo].[tbPublish] A
            INNER JOIN (
                SELECT A.[Id],
                       A.[CustViewId],
                        CASE WHEN Frequency = '1' THEN
                                ' AND ( ShareDT <= ''' +
                                CASE 
                                    -- 當天是PDay號且時間超過PTime點，或是PDay號之後的日期
                                    WHEN (DAY(GETDATE()) = A.PDay AND DATEPART(HOUR, GETDATE()) >= A.PTime) 
                                         OR DAY(GETDATE()) > PDay
                                    THEN FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM')
                                    -- 未到PDay號或當天未到PTime點
                                    ELSE FORMAT(DATEADD(MONTH, -2, GETDATE()), 'yyyyMM')
                                END + ''')'
                            ELSE '' END
                            AS billing_month,
                            '(' +
                           STRING_AGG(CASE 
                                        WHEN LEN(B.StartYM) > 0 AND LEN(B.VariableId) = 0 THEN '(ShareDT BETWEEN ''' + trim(B.StartYM) + ''' AND ''' + trim(B.EndYM) + ''')' 
										WHEN LEN(B.VariableId) > 0 THEN '(ShareDT BETWEEN ''' + trim(C.StartYM) + ''' AND ''' + trim(C.EndYM) + ''')' 
                                       END, ' OR ') 
                                       + ')'AS PublishWhere
                FROM [iDataCenter].[dbo].[tbPublish] A
                LEFT JOIN [iDataCenter].[dbo].[tbPublishPeriod] B 
				ON A.Id = B.PublishId AND A.Enable = B.Enable
                LEFT JOIN [iDataCenter].[dbo].[vTimeVariable] C 
				ON B.VariableId = C.Id AND B.Enable = C.Enable 
				AND CASE WHEN CAST(DAY(GETDATE()) AS VARCHAR) + CAST(DATEPART(hour, GETDATE()) AS VARCHAR) >= 
                    CAST(A.PDay AS VARCHAR) + CASE WHEN A.PTime = '0' THEN '00' ELSE CAST(A.PTime AS VARCHAR) END THEN '1' ELSE '0' END = C.Var_DT_Open
                WHERE A.Enable = '1' 
                  AND A.CustViewId = @CustViewId
                GROUP BY A.Id, A.CustViewId, A.PDay, A.PTime, A.Frequency
            ) B ON A.Id = B.Id AND A.CustViewId = @CustViewId AND A.Enable = '1'
            WHERE B.PublishWhere IS NOT NULL;
        END

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

        -- Log the error (you can modify this to use your preferred logging method)
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
END;
GO


