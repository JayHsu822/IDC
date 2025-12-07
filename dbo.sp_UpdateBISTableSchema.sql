USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[sp_UpdateBISTableSchema]    Script Date: 2025/12/7 上午 11:03:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- 作者：Jay Hsu
-- 建立日期：2025-03-06
-- 描述：根據資源名稱更新或新增欄位映射資訊
-- 版本記錄：
------------------------------------------------------------------------------
-- 版本   日期          作者             描述
-- 1.0    2025-03-06    Jay Hsu        初始版本
-- 1.1    2025-03-07    Jay Hsu        新增重建結構機制
-- 參數：
--   @TB_NM - 資源名稱，例如'iVIEW.FCT_Headcount_All'
-- 返回值：無直接返回值，更新tbResSchema表
-- 更新注意事項
-- 1.BIS Table View更新
-- 2.SQL Server iOPEN View更新
-- 3.執行此Store Procedure 例 : exec [dbo].[sp_UpdateBISTableSchema] @TB_NM = 'iVIEW.FCT_Headcount_All'
-- =============================================
CREATE OR ALTER PROCEDURE [dbo].[sp_UpdateBISTableSchema]
    @TB_NM NVARCHAR(255),  -- 資源名稱參數
	@RunMode NVARCHAR(10)  -- '1':新增欄位更新 '2':重建 
AS
BEGIN
    SET NOCOUNT ON;  -- 設定不返回受影響行數的訊息
    
    BEGIN TRY
        -- 參數驗證
        IF @TB_NM IS NULL OR LEN(TRIM(@TB_NM)) = 0
        BEGIN
            RAISERROR('資源名稱參數不能為空', 16, 1);
            RETURN;
        END

        -- 宣告變數
        DECLARE @tbresid NVARCHAR(4000);       -- tbResID
		DECLARE @CustViewid NVARCHAR(4000);    -- CustViewID
		DECLARE @tbResSchemaId NVARCHAR(36);   -- tbResSchemaId
		DECLARE @multicustviewid NVARCHAR(MAX);
        DECLARE @FieldName NVARCHAR(255);      -- 欄位名稱
        DECLARE @FieldType INT;                -- 欄位類型
        DECLARE @Seq INT;                      -- 序列號
        DECLARE @RecordCount INT = 0;          -- 處理記錄計數
        DECLARE @CurrentDate DATETIME = GETDATE(); -- 當前日期時間

        -- 獲取資源ID
        SELECT @tbresid = id 
        FROM tbRes
        WHERE resname = @TB_NM AND enable = '1';

		SELECT @multicustviewid = '(' + STUFF((
			SELECT ',' + QUOTENAME(id, '''')
			FROM tbcustview
			WHERE masterid = @tbresid AND enable = '1'
			FOR XML PATH('')
		), 1, 1, '') + ')';

		IF @RunMode = '1'
		BEGIN -- @runMode = '1' Start
        
			-- 檢查資源是否存在
			IF @tbresid IS NULL
			BEGIN
				RAISERROR('找不到指定的資源名稱: %s', 16, 1, @TB_NM);
				RETURN;
			END

			IF CURSOR_STATUS('global','tbres_cursor') >= 0
			BEGIN
				CLOSE tbres_cursor;
				DEALLOCATE tbres_cursor;
			END

			-- 宣告游標，用於遍歷欄位資訊
			DECLARE tbres_cursor CURSOR FOR
				SELECT 
					B.ColumnName,                  -- 欄位名稱
					CASE                           -- 欄位類型轉換
						WHEN B.ColumnType IN ('CV', 'CF') THEN 1  -- 字符型
						WHEN B.ColumnType IN ('I') THEN 2         -- 整數型
						WHEN B.ColumnType IN ('D') THEN 3         -- 日期型
						ELSE 9                                    -- 其他類型
					END AS FieldType,
					B.ColumnID                     -- 序列號
				FROM 
					tbRes A                        -- 資源表
				LEFT JOIN
					iOpen.ipjt.columnsvx B         -- 欄位資訊表
				ON UPPER(A.ResName) = UPPER(B.DatabaseName + '.' + B.TableName)
				WHERE 
					A.Enable = 1 
					AND A.id = @tbresid
					AND B.DatabaseName IS NOT NULL;
        
			-- 開啟游標並開始處理
			OPEN tbres_cursor;
			FETCH NEXT FROM tbres_cursor INTO @FieldName, @FieldType, @Seq;
        
			-- 遍歷每個欄位進行處理
			WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @RecordCount = @RecordCount + 1;  -- 增加計數器
			
				SELECT @tbResSchemaId = id FROM tbResSchema WHERE resid = @tbresid AND enable = '1' AND FieldName = @FieldName;  
            

				print 'tbResSchemaId:'+@tbResSchemaId;

				-- 檢查記錄是否存在，存在則更新，不存在則新增
				IF EXISTS (SELECT 1 FROM tbResSchema WHERE resid = @tbresid AND enable = '1' AND FieldName = @FieldName)
				BEGIN
					-- 更新現有欄位資訊
					UPDATE tbResSchema
					SET 
						FieldType = @FieldType, 
						seq = @Seq, 
						modifytime = @CurrentDate,
						ModifyUser = 'Sys'
					WHERE 
						resid = @tbresid 
						AND enable = '1' 
						AND FieldName = @FieldName;
				END
				ELSE
				BEGIN
					-- 插入新欄位資訊
					INSERT INTO tbResSchema (
						ID, resid, FieldName, FieldType, IsTimeVar, 
						TimeFormat, Inheritable, locked, Enable, 
						Seq, CreateUser, CreateTime, ModifyUser, ModifyTime
					)
					VALUES (
						NEWID(),                   -- 生成新的唯一識別碼
						@tbresid,                  -- 資源ID
						@FieldName,                -- 欄位名稱
						@FieldType,                -- 欄位類型
						0,                         -- 不是時間變數
						NULL,                      -- 無時間格式
						1,                         -- 可繼承
						NULL,                      -- 未鎖定
						1,                         -- 啟用
						@Seq,                      -- 序列號
						'Init',                    -- 建立使用者
						@CurrentDate,              -- 建立時間
						'Sys',                     -- 修改使用者
						@CurrentDate               -- 修改時間
					);
				END
			
				Print 'tbresschema';

				-- 使用動態 SQL 執行更新操作
				DECLARE @sql NVARCHAR(MAX);
				SET @sql = N'
					UPDATE dbo.tbCustviewSchema
					SET 
						seq = @Seq, 
						modifytime = @CurrentDate,
						ModifyUser = ''Sys''
					WHERE 
						CustViewId IN ' + @multicustviewid + '
						AND enable = ''1'' 
						AND ResSchemaId = @tbResSchemaId;
				';

				-- 執行動態 SQL
				EXEC sp_executesql 
					@sql,
					N'@Seq INT, @CurrentDate DATETIME, @tbResSchemaId NVARCHAR',
					@Seq = @Seq,
					@CurrentDate = @CurrentDate,
					@tbResSchemaId = @tbResSchemaId;
                    
				-- 讀取下一筆資料
				FETCH NEXT FROM tbres_cursor INTO @FieldName, @FieldType, @Seq;
			END
        
			-- 關閉並釋放游標
			CLOSE tbres_cursor;
			DEALLOCATE tbres_cursor;
        
			-- 檢查處理結果
			IF @RecordCount = 0
			BEGIN
				RAISERROR('未找到符合條件的欄位記錄，請檢查資源名稱 "%s" 是否正確', 16, 1, @TB_NM);
			END
			ELSE
			BEGIN
				-- 可選：返回更新後的資料
				SELECT 
					FieldName, 
					FieldType, 
					Seq 
				FROM 
					tbResSchema 
				WHERE 
					resid = @tbresid 
					AND enable = '1' 
				ORDER BY 
					Seq;
			END
		END -- @runMode = '1' END
		ELSE IF @RunMode = '2'
		BEGIN

		        DELETE tbResSchema
				WHERE ResId = @tbresid and enable = '5';

		        Update tbResSchema
				set enable = '5' -- 結構調整
				where ResId = @tbresid and enable = '1';

				insert into tbResSchema
				SELECT newid() as ID
					  ,A.ID as [ResId]
					  ,B.ColumnName as [FieldName]
					  ,case when B.ColumnType in ('CV','CF') then 1  when B.ColumnType in ('I') then 2   when B.ColumnType in ('D') then 3  else 9 end  as [FieldType]
					  ,0 as [IsTimeVar]
					  ,null as [TimeFormat]
					,1 as [Inheritable]
					,null as [locked]
						, 1 as [Enable]
				,B.ColumnID as [Seq]
				,'Init' as CreateUser
				,CURRENT_TIMESTAMP as [CreateTime] 
				,'Sys' as [ModifyUser] 
				,CURRENT_TIMESTAMP as [ModifyTime]
				--,* 
				FROM 
					tbRes A                        -- 資源表
				LEFT JOIN
					iOpen.ipjt.columnsvx B         -- 欄位資訊表
				ON UPPER(A.ResName) = UPPER(B.DatabaseName + '.' + B.TableName)
				WHERE 
					A.Enable = 1 
					AND A.id = @tbresid
					AND B.DatabaseName IS NOT NULL;

				INSERT INTO tbResSchema (ResId,FieldName,FieldType,Locked,Seq,Enable,CreateUser,ModifyUser)
				SELECT a.id,'ShareDT',1,1,99999,1,'Init','Sys'
				FROM tbRes A
				INNER JOIN tbResInitTmp B
				ON A.id = b.id AND a.Enable = b.Enable and len(b.ShareDT) > 0
				WHERE a.id = @tbresid and a.enable = '1' ;

				IF CURSOR_STATUS('global','tbres_cursor') >= 0
				BEGIN
					CLOSE tbCustView_cursor;
					DEALLOCATE tbCustView_cursor;
				END

				-- 宣告游標，用於遍歷欄位資訊
				DECLARE tbCustView_cursor CURSOR FOR
					SELECT id FROM tbCustView
					WHERE masterid = @tbresid and layer <> '1' and Enable = '1';

				OPEN tbCustView_cursor;
				FETCH NEXT FROM tbCustView_cursor INTO @custviewid;
					-- 遍歷每個欄位進行處理
					WHILE @@FETCH_STATUS = 0
					BEGIN

					DELETE tbCustViewSchema
					WHERE CustViewId = @CustViewid and enable = '5';

					update tbCustViewSchema
					set enable = '5'
					where CustViewId = @CustViewid and enable = '1';

					insert into [dbo].[tbCustViewSchema](
					[Id]
					,[CustViewId]
					,[ResSchemaId]
					,[FieldName]
					,[ShowField]
					,[NickName]
					,[Summarize]
					,[SortKind]
					,[SortKey]
					,[Seq]
					,[Enable]
					,[CreateUser]
					,[CreateTime]
					,[ModifyUser]
					,[ModifyTime]
					)
					SELECT 
					NewID()
					,@CustviewID
					,id
					,[FieldName]
					,'1'
					,NULL
					,'0'
					,NULL
					,NULL
					,[Seq]
					,'1'
					,'Init'
					,GETDATE()
					,'Sys'
					,GETDATE()
					FROM iDataCenter.dbo.tbResSchema
					WHERE resid = @tbresid and Enable = '1';


					-- 讀取下一筆資料
					FETCH NEXT FROM tbCustView_cursor INTO @custviewid;
				END
        
				-- 關閉並釋放游標
				CLOSE tbCustView_cursor;
				DEALLOCATE tbCustView_cursor;
		END 
    END TRY
    BEGIN CATCH
        -- 錯誤處理
        IF CURSOR_STATUS('local', 'tbres_cursor') >= 0
        BEGIN
            CLOSE tbres_cursor;
            DEALLOCATE tbres_cursor;
        END
        
        -- 將錯誤訊息返回給調用者
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        -- 記錄錯誤日誌（可根據需求自行實現）
        -- INSERT INTO ErrorLog (ErrorMessage, ErrorProcedure, ErrorLine, ErrorTime)
        -- VALUES (@ErrorMessage, ERROR_PROCEDURE(), ERROR_LINE(), GETDATE());
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END
GO


