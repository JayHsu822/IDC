USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[PuslishNotifyPROC]    Script Date: 2025/12/7 上午 11:10:08 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/*
=============================================================================
  描述：[分享/派送通知]
  
  版本記錄：
  ------------------------------------------------------------------------------
  版本   日期          作者        描述
  1.0    2024-12-10    Jay Hsu     初始版本
  1.1    2025-01-21    VicJH Wang  增加分享通知情境於 PublishToFile
  1.2    2025-01-24    VicJH Wang  調整 vPublish 與 tbPublishUsers 關聯 (vPublish.id 為非一次性PublishId)
  
  參數說明：
  ------------------------------------------------------------------------------
  @inCustViewId NVARCHAR(36)  自訂View Id       派送產檔必填
  @inPublishId  NVARCHAR(36)  Publish Id        派送產檔必填
  @inFileId     NVARCHAR(36)  File Id  
  @inMessageId  NVARCHAR(36)  Message Id        發送Mail必填
  @inIsEnable   CHAR(2)       狀態              派送作業填74,其餘不用
  @WebFunction  NVARCHAR(100) 功能              必填 PublishToFile:派送產檔,PublishClientMsg:發送Mail         
  @kind			CHAR(2)		  1:分享、2:派送

  返回值：
  ------------------------------------------------------------------------------
  返回型態：資料表
  返回說明：[提供數據平台資料轉換呈現使用]
  
  相依性：
  ------------------------------------------------------------------------------
  Tables：
    - Table1 (Select)
    - Table2 (Insert/Update)
  Views：
    - View1 (Select)
    - View2 (Insert/Update)
  Functions：
    - AccountId : dbo.fnGetiDataCenterInfo('6', @CustViewid,'1','');
    - dbo.Function2
  
  使用範例：
  ------------------------------------------------------------------------------
	1.派送產檔 : Exec iDataCenter.dbo.PublishNotifyPROC @inCustViewId = 'EEB8F98C-1BC7-45ED-8CD6-71C8AF2DB2A6', @inPublisId = 'BC84F777-3DDD-491E-A5B7-3E8B4AA3ED8F', @inMessageId = '', @inIsEnable = '74', @WebFunction = 'PublishToFile'
	2.ClientMessage : 
	  (1) 派送檔案用 : Exec iDataCenter.dbo.PublishNotifyPROC @inCustViewId = '', @inPublisId = '',@inFileId ='', @inMessageId = '259E1AC9-63A1-48AE-8922-7352743AE68E', @inIsEnable = '74', @WebFunction = 'PublishClientMsg'
	  (2) 單純發Mail : Exec iDataCenter.dbo.PublishNotifyPROC @inCustViewId = '', @inPublisId = '',@inFileId ='', @inMessageId = '259E1AC9-63A1-48AE-8922-7352743AE68E', @inIsEnable = '', @WebFunction = 'PublishClientMsg'
  
  注意事項：
  ------------------------------------------------------------------------------
  1. [重要注意事項1]
  2. [效能考量說明]
  3. [業務邏輯特殊處理]
=============================================================================
*/
CREATE OR ALTER PROCEDURE [dbo].[PuslishNotifyPROC]
    @inCustViewId NVARCHAR(36),
    @inPublishId NVARCHAR(36),
	@inFileId NVARCHAR(36),
	@inMessageId NVARCHAR(36),
    @inIsEnable CHAR(2), -- 74:派送
	@WebFunction NVARCHAR(100),
	@kind CHAR(2)  = '0'
AS
BEGIN

    SET NOCOUNT ON;
	--EXEC [iLog].[dbo].[sp_PuslishNotifyPROC_WithLogging]
      --      @inCustViewid, @inPublishId, @inFileId, @inMessageId, @inIsEnable, @WebFunction
    
    -- 宣告變數來存儲參數值用於錯誤記錄
    DECLARE @Parameters NVARCHAR(MAX) = 
        N'@inCustViewId=' + ISNULL(@inCustViewId, 'NULL') + 
        N', @inPublishId=' + ISNULL(@inPublishId, 'NULL') + 
        N', @inIsEnable=' + ISNULL(@inIsEnable, 'NULL') +
		N', @WebFunction=' + ISNULL(@WebFunction, 'NULL');

    BEGIN TRY
        DECLARE @ViewName NVARCHAR(100),
                @CustViewId NVARCHAR(36),
                @PublishId NVARCHAR(36),
                @ServerPath NVARCHAR(100),
                @FilePath NVARCHAR(200),
                @FileName NVARCHAR(100),
                @Level CHAR(2),
                @FILEID NVARCHAR(36),
                @DQL NVARCHAR(MAX),
                @AccountID NVARCHAR(36),
                @Frequency INT,
                @ResColumnNm NVARCHAR(36),
                @PDay INT,
                @PTime INT,
                @Period INT,
                @VariableId NVARCHAR(36),
                @StartYM CHAR(6),
                @EndYM CHAR(6),
                @DomainUser NVARCHAR(36),
                @DBName NVARCHAR(10);

        IF @WebFunction = 'PublishToFile'
        BEGIN
            DECLARE @CursorError BIT = 0;
            DECLARE PublsihCursor CURSOR FOR
            SELECT CustViewId, PublishId, LEVEL, FileName, ServerPath, FilePath, FILEID, AccountId, Frequency
            FROM [iDataCenter].[dbo].[vPublish]
            WHERE PublishId = @inPublishId
            GROUP BY CustViewId, PublishId, LEVEL, FileName, ServerPath, FilePath, FILEID, AccountId, Frequency;

            BEGIN TRY
                OPEN PublsihCursor;
                
                FETCH NEXT FROM PublsihCursor INTO @CustViewId, @PublishId, @LEVEL, @FileName, @ServerPath, @FilePath, @FILEID, @AccountId, @Frequency;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    BEGIN TRY
						--分享
						IF @kind = 1 BEGIN
							-- 插入檔案資訊
							INSERT INTO [dbo].[tbFiles]
							([Id], [ServerPath], [FilePath], [FileName], [Level], [Extension],
							 [DQL], [ExpTime], [Exception], [Enable], [Synced], [SyncTime],
							 [CreateTime], [DownloadTime], [PublishId])
							VALUES
							(NEWID(), NULL, NULL, @FileName, NULL, NULL,
							 NULL, NULL, NULL, 1, 10, NULL,
							 GETDATE(), NULL, @PublishId);

							INSERT INTO [dbo].[tbPublishHistory]
								  ([PublishId]
								  ,[Frequency]
								  ,[PublishTime])
							VALUES
								  (@PublishId
								  ,@Frequency
								  ,GETDATE());
						END;

						--派送
						IF @kind = 2 BEGIN
							-- 建立臨時表
							IF OBJECT_ID('tempdb..#DQL') IS NOT NULL 
								DROP TABLE #DQL;

							CREATE TABLE #DQL (
								DQL NVARCHAR(MAX), 
								RunId NVARCHAR(36)
							);

							-- 執行存儲過程並插入結果
							INSERT INTO #DQL (DQL)
							EXEC sp_GetViewFieldsAndFilters_SQL 
								@CustViewid = @inCustViewId,
								@ViewName = '',
								@IsEnable = @inIsEnable,
								@AccountId = @AccountId,
								@ViewNo = @PublishId,  --填PublishId
								@QueryQty = '';

							-- 更新RunId
							UPDATE #DQL
							SET RunId = @PublishId;
							

							-- 獲取DQL
							SELECT @DQL = DQL FROM #DQL WHERE RunId = @PublishId;

							-- 獲取其他資訊
							SELECT @DomainUser = dbo.fnGetDomainUserByAccountId(@AccountId);
							SELECT @DBName = dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1','');

							-- 插入檔案資訊
							INSERT INTO [dbo].[tbFiles]
							([Id], [ServerPath], [FilePath], [FileName], [Level], [Extension],
							 [DQL], [ExpTime], [Exception], [Enable], [Synced], [SyncTime],
							 [CreateTime], [DownloadTime], [PublishId])
							VALUES
							(NEWID(), @ServerPath, @FilePath, @FileName, @Level, 1,
							 @DQL, DATEADD(DAY, 30, GETDATE()), NULL, 1, 1, NULL,
							 GETDATE(), NULL, @PublishId);
							 

							--1.2    2025-01-24    VicJH Wang  調整 vPublish 與 tbPublishUsers 關聯 (vPublish.id 為非一次性PublishId)
							Update tbPublishUsers WITH (ROWLOCK, UPDLOCK)
							SET OTP = NULL
							where PublishId = (
								SELECT ID FROM [iDataCenter].[dbo].[vPublish] WHERE PublishId = @PublishId GROUP BY ID
							);

							print 'sql1';

							INSERT INTO [dbo].[tbPublishHistory]
								  ([PublishId]
								  ,[Frequency]
								  ,[PublishTime])
							VALUES
								  (@PublishId
								  ,@Frequency
								  ,GETDATE());

							-- 清理臨時表
							DROP TABLE #DQL;
						END;
                    END TRY
                    BEGIN CATCH
                        SET @CursorError = 1;
                        -- 記錄每次迭代中的錯誤
                        EXEC [iLog].[dbo].[LogErrorPROC] 
                            @ProcedureName = 'PuslishNotifyPROC - Cursor Operation',
                            @Parameters = @Parameters;
                        
                        -- 清理臨時表（如果存在）
                        IF OBJECT_ID('tempdb..#DQL') IS NOT NULL 
                            DROP TABLE #DQL;
                    END CATCH

                    -- 繼續下一筆資料
                    FETCH NEXT FROM PublsihCursor INTO @CustViewId, @PublishId, @LEVEL, @FileName, @ServerPath, @FilePath, @FILEID, @AccountId, @Frequency;
                END;

            END TRY
            BEGIN CATCH
                SET @CursorError = 1;
                -- 記錄游標操作的錯誤
                EXEC [iLog].[dbo].[LogErrorPROC] 
                    @ProcedureName = 'PuslishNotifyPROC - Cursor Operation',
                    @Parameters = @Parameters;
            END CATCH

            -- 清理游標（移至外部以確保一定會執行）
            IF EXISTS (SELECT 1 FROM sys.dm_exec_cursors(@@SPID) WHERE is_open = 1)
            BEGIN
                CLOSE PublsihCursor;
                DEALLOCATE PublsihCursor;
            END

            -- 如果有錯誤發生，拋出異常
            IF @CursorError = 1
                THROW 50000, 'Error occurred during cursor operations', 1;
        END
        IF @WebFunction = 'PublishClientMsg'
        BEGIN
            BEGIN TRY
                -- 獲取其他資訊
                SELECT @DomainUser = dbo.fnGetDomainUserByAccountId(@AccountId);
                SELECT @DBName = dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1','');

                -- 插入檔案資訊
				INSERT INTO [identity].dbo.tbClientMessage(
				   [Id]
				  ,[ClientId]
				  ,[Title]
				  ,[ToUser]
				  ,[ToDept]
				  ,[CcUser]
				  ,[CcDept]
				  ,[Msg]
				  ,[MsgPath]
				  ,[MsgKind]
				  ,[MsgAttach]
				  ,[MsgTime]
				  ,[Exception]
				  ,[CreateTime])
				SELECT [Id]
					  ,[ClientId]
					  ,[Title]
					  ,[ToUser]
					  ,[ToDept]
					  ,[CcUser]
					  ,[CcDept]
					  ,[Msg]
					  ,[MsgPath]
					  ,[MsgKind]
					  ,[MsgAttach]
					  ,[MsgTime]
					  ,[Exception]
					  ,GETDATE()
				FROM itemp.dbo.tmpClientMessage
				WHERE id = @inMessageId;
				print @inIsEnable
				print @inFileId
				print 'out alive'
				IF @inIsEnable = '74'
				BEGIN
					print 'in alive'
					Update iDataCenter.dbo.tbFiles WITH (ROWLOCK, UPDLOCK)
					set Synced = '0'
					WHERE ID = @inFileId;
				END

				DELETE iTemp.dbo.tmpClientMessage
				WHERE id = @inMessageId;

            END TRY
            BEGIN CATCH
                SET @CursorError = 1;
                -- 記錄每次執行中的錯誤
                EXEC [iLog].[dbo].[LogErrorPROC] 
                    @ProcedureName = 'PuslishNotifyPROC - Cursor Operation',
                    @Parameters = @Parameters;
            END CATCH

            -- 清理游標（移至外部以確保一定會執行）
            IF EXISTS (SELECT 1 FROM sys.dm_exec_cursors(@@SPID) WHERE is_open = 1)
            BEGIN
                CLOSE PublsihCursor;
                DEALLOCATE PublsihCursor;
            END

            -- 如果有錯誤發生，拋出異常
            IF @CursorError = 1
                THROW 50000, 'Error occurred during cursor operations', 1;
        END;
    END TRY
    BEGIN CATCH
        -- 記錄主要程序錯誤
        EXEC [ilog].[dbo].[LogErrorPROC] 
            @ProcedureName = 'PuslishNotifyPROC',
            @Parameters = @Parameters;
        
        -- 重新拋出錯誤
        THROW;
    END CATCH;
END;
GO


