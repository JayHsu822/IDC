USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[sp_InitializeDCPlatformData_ALL_TB]    Script Date: 2025/12/7 上午 11:00:48 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO











CREATE OR ALTER PROCEDURE [dbo].[sp_InitializeDCPlatformData_ALL_TB]
AS
BEGIN
		DECLARE @TBNM NVARCHAR(100);

				-- 宣告游標
				IF CURSOR_STATUS('global','InitCustView_cursor') >= 0
				BEGIN
					CLOSE InitCustView_cursor
					DEALLOCATE InitCustView_cursor
				END
				DECLARE InitCustView_cursor CURSOR FOR 
				SELECT ResName
				FROM tbResInitTmp ;
				--WHERE EmpNo like '%C9309';
				--WHERE ResName like 'IDATA.RPM_COST_AHC_DATA%';
				--and EmpNo like '%C2534';

				-- 開啟游標
				OPEN InitCustView_cursor

				-- 擷取第一筆資料
				FETCH NEXT FROM InitCustView_cursor INTO @TBNM;

				-- 使用 @@FETCH_STATUS 檢查是否還有資料
				WHILE @@FETCH_STATUS = 0
				BEGIN
					EXEC [dbo].[sp_InitializeDCPlatformData] @TB_NM = @TBNM;
					-- 擷取下一筆
					FETCH NEXT FROM InitCustView_cursor INTO @TBNM;
				END

				-- 關閉游標
				CLOSE InitCustView_cursor

				-- 釋放游標
				DEALLOCATE InitCustView_cursor
			--刪除結尾

END;
GO


