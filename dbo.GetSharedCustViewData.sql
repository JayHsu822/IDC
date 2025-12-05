USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[GetSharedCustViewData]    Script Date: 2025/12/5 下午 03:04:37 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/*
================================================================================
儲存程序名稱: [dbo].[GetSharedCustViewData]
版本: 1.0.0
建立日期: 2025-05-02
修改日期: 2025-05-02
作者: Jay
描述: 處理自訂 View 相關作業，主要依據 @inIsEnable 狀態碼執行不同操作：
      '6', '61', '63' - 處理分享 View 的權限設定 (執行 [iOPEN].[dbo].[SetupUnifiedSecurity])
      '11' - 刪除分享後的自訂 View (Sx系列)

使用方式:
1. 刪除自訂 View (狀態 '11'):
   EXEC GetSharedCustViewData 
       @inCustViewid = 'FC4AA5EA-91EE-4B68-B42E-4109A6B654CE',
    	@inPublishId = '',
    	@inIsEnable = '11',
    	@AccountId = 'CEA7EEAD-C1D8-452E-97BE-DB7F3D1D2E6A';

2. 處理權限 (例如狀態 '6'):
   EXEC GetSharedCustViewData 
       @inCustViewid = '[YourCustViewId]',
    	@inPublishId = '',
    	@inIsEnable = '6',
    	@AccountId = '[YourAccountId]';

參數說明:
@inCustViewId - 自訂View Id (NVARCHAR(36), 必填)
@inPublishId  - Publish_id (NVARCHAR(36), 可空, 預設為NULL). 
               狀態 '61' 和 '63' 時會使用。
@inIsEnable   - 執行狀態 (CHAR(2), 必填). 
               '6', '61', '63' = 權限處理; '11' = 刪除.
@inAccountid  - 執行者 AccountId (NVARCHAR(36), 必填)

版本歷程:
Jay         v1.0.0 (2025-05-02) - 初始版本 (依據原註解)，新增刪除分享後自訂View功能(Sx系列)
================================================================================
*/



CREATE OR ALTER       PROCEDURE [dbo].[GetSharedCustViewData]
    @inCustViewId NVARCHAR(36),  -- CustViewId
	@inPublishId NVARCHAR(36),
	--@inAccountId NVARCHAR(36),
	@inIsEnable CHAR(2),
	@inAccountid NVARCHAR(36)
AS
BEGIN
    DECLARE @ViewName NVARCHAR(100),
            @PublishId NVARCHAR(36),
            @Frequency INT,
            @ResColumnNm NVARCHAR(36),
            @PDay INT,
            @PTime INT,
            @Period INT,
            @VariableId NVARCHAR(36),
            @StartYM CHAR(6),
            @EndYM CHAR(6),
            @AccountId NVARCHAR(36),
			@DomainUser NVARCHAR(36),
			@DBName NVARCHAR(10),
			@IsShareDT NVARCHAR(20),
			@IsGrantTB CHAR(1),
			@ORIViewName NVARCHAR(100);

		IF @inIsEnable in ('6') 
			BEGIN
			-- 定義游標
			DECLARE CustomerViewCursor CURSOR FOR
			SELECT ViewName, PublishId, Frequency, ResColumnNm, PDay, PTime, Period, isnull(VariableId,'') AS VariableId, isnull(StartYM,'') AS StartYM, isnull(EndYM,'') AS EndYM, AccountId
			FROM [iTemp].[dbo].[tmpCustView_Publish]
			WHERE custviewid = @inCustViewId  and PublishId is not null and Enable = '3' and ModifyUser = @inAccountid-- 使用傳入的參數
			GROUP BY ViewName, PublishId, Frequency, ResColumnNm, PDay, PTime, Period, VariableId, StartYM, EndYM, AccountId;

			SELECT @ORIViewName = dbo.fnGetiDataCenterInfo('3', @inCustViewid,'1','');
			PRINT 'ORIViewName:'+@ORIViewName;

			-- 打開游標
			OPEN CustomerViewCursor;

			-- 循環讀取數據
			FETCH NEXT FROM CustomerViewCursor INTO @ViewName, @PublishId, @Frequency, @ResColumnNm, @PDay, @PTime, @Period, @VariableId, @StartYM, @EndYM, @AccountId;

			WHILE @@FETCH_STATUS = 0
			BEGIN
				-- 在這裡可以處理每一行數據，比如打印或插入到其他表中
				PRINT 'ViewName: ' + @ViewName + ', PublishId: ' + @PublishId + ', Frequency: ' + CAST(@Frequency AS NVARCHAR);
				SELECT @DomainUser = dbo.fnGetDomainUserByAccountId(@AccountId);
				SELECT @DBName = dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1','');
				SELECT @IsShareDT = ISNULL(dbo.fnGetiDataCenterInfo('10', @inCustViewid,'1',''),'N');
					EXEC [iOPEN].[dbo].[SetupUnifiedSecurity]
						@UserName = @DomainUser,
						@StartDate = @StartYM,
						@EndDate = @EndYM,
						@IsADUser = 1,
						@IsOwner = 0,
						@IsnonDate = 1,
						@CheckDate = @IsShareDT,
						@CustomView = 'S',
						@DBName = @DBName,
						@ViewName = @ViewName,
						@DisplayViewName = '',
						@Source = @inCustViewId,
						@inIsEnable = @inIsEnable;

				-- 繼續讀取下一行數據
				FETCH NEXT FROM CustomerViewCursor INTO @ViewName, @PublishId, @Frequency, @ResColumnNm, @PDay, @PTime, @Period, @VariableId, @StartYM, @EndYM, @AccountId;
			END;

			-- 關閉游標
			CLOSE CustomerViewCursor;
			DEALLOCATE CustomerViewCursor;
		END
		ELSE IF @inIsEnable in ('61') 
		BEGIN
			-- 定義游標
			DECLARE CustomerViewCursor CURSOR FOR
			SELECT ViewName, PublishId, Frequency, ResColumnNm, PDay, PTime, Period,  isnull(VariableId,'') AS VariableId, isnull(StartYM,'') AS StartYM, isnull(EndYM,'') AS EndYM, AccountId
			FROM [iTemp].[dbo].[tmpCustView_Publish]
			WHERE PublishId = @inPublishId and Enable = '3'-- 使用傳入的參數
			GROUP BY ViewName, PublishId, Frequency, ResColumnNm, PDay, PTime, Period, VariableId, StartYM, EndYM, AccountId;

			SELECT @ORIViewName = dbo.fnGetiDataCenterInfo('3', @inCustViewid,'1','');
			PRINT 'ORIViewName:'+@ORIViewName;

			-- 打開游標
			OPEN CustomerViewCursor;

			-- 循環讀取數據
			FETCH NEXT FROM CustomerViewCursor INTO @ViewName, @PublishId, @Frequency, @ResColumnNm, @PDay, @PTime, @Period, @VariableId, @StartYM, @EndYM, @AccountId;

			WHILE @@FETCH_STATUS = 0
			BEGIN
				-- 在這裡可以處理每一行數據，比如打印或插入到其他表中
				PRINT 'ViewName: ' + @ViewName + ', PublishId: ' + @PublishId + ', Frequency: ' + CAST(@Frequency AS NVARCHAR);
				SELECT @DomainUser = dbo.fnGetDomainUserByAccountId(@AccountId);
				SELECT @DBName = dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1','');
				SELECT @IsShareDT = ISNULL(dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1',''),'N');
					EXEC [iOPEN].[dbo].[SetupUnifiedSecurity]
						@UserName = @DomainUser,
						@StartDate = @StartYM,
						@EndDate = @EndYM,
						@IsADUser = 1,
						@IsOwner = 0,
						@IsnonDate = 1,
						@CheckDate = @IsShareDT,
						@CustomView = 'S',
						@DBName = @DBName,
						@ViewName = @ViewName,
						@DisplayViewName = '',
						@Source = @inCustViewId,
						@inIsEnable = @inIsEnable;

				-- 繼續讀取下一行數據
				FETCH NEXT FROM CustomerViewCursor INTO @ViewName, @PublishId, @Frequency, @ResColumnNm, @PDay, @PTime, @Period, @VariableId, @StartYM, @EndYM, @AccountId;
			END;

			-- 關閉游標
			CLOSE CustomerViewCursor;
			DEALLOCATE CustomerViewCursor;
			EXEC [iOPEN].[dbo].[SetupUnifiedSecurity]
				@UserName = @DomainUser,
				@StartDate = @StartYM,
				@EndDate = @EndYM,
				@IsADUser = 1,
				@IsOwner = 0,
				@IsnonDate = 1,
				@CheckDate = @IsShareDT,
				@CustomView = 'S',
				@DBName = @DBName,
				@ViewName = @ViewName,
				@DisplayViewName = '',
				@Source = @inPublishId,
				@inIsEnable = @inIsEnable;
		END
		ELSE IF @inIsEnable in ('63') 
		BEGIN
		    Print '63權限更新';
			/*-- 定義游標
			DECLARE CustomerViewCursor CURSOR FOR
			SELECT ViewName, PublishId, Frequency, ResColumnNm, PDay, PTime, Period, VariableId, StartYM, EndYM, AccountId
			FROM [iTemp].[dbo].[tmpCustView_Publish]
			WHERE PublishId = @inPublishId and Enable = '62'-- 使用傳入的參數
			GROUP BY ViewName, PublishId, Frequency, ResColumnNm, PDay, PTime, Period, VariableId, StartYM, EndYM, AccountId;

			SELECT @ORIViewName = dbo.fnGetiDataCenterInfo('3', @inCustViewid,'1','');
			PRINT 'ORIViewName:'+@ORIViewName;

			-- 打開游標
			OPEN CustomerViewCursor;

			-- 循環讀取數據
			FETCH NEXT FROM CustomerViewCursor INTO @ViewName, @PublishId, @Frequency, @ResColumnNm, @PDay, @PTime, @Period, @VariableId, @StartYM, @EndYM, @AccountId;

			WHILE @@FETCH_STATUS = 0
			BEGIN
				-- 在這裡可以處理每一行數據，比如打印或插入到其他表中
				PRINT 'ViewName: ' + @ViewName + ', PublishId: ' + @PublishId + ', Frequency: ' + CAST(@Frequency AS NVARCHAR);
				SELECT @DomainUser = dbo.fnGetDomainUserByAccountId(@AccountId);
				SELECT @DBName = dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1','');
				SELECT @IsShareDT = ISNULL(dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1',''),'N');
					EXEC [iOPEN].[dbo].[SetupUnifiedSecurity]
						@UserName = @DomainUser,
						@StartDate = @StartYM,
						@EndDate = @EndYM,
						@IsADUser = 1,
						@IsOwner = 0,
						@IsnonDate = 1,
						@CheckDate = @IsShareDT,
						@CustomView = 'S',
						@DBName = @DBName,
						@ViewName = @ViewName,
						@DisplayViewName = '',
						@Source = @inCustViewId,
						@inIsEnable = @inIsEnable;

				-- 繼續讀取下一行數據
				FETCH NEXT FROM CustomerViewCursor INTO @ViewName, @PublishId, @Frequency, @ResColumnNm, @PDay, @PTime, @Period, @VariableId, @StartYM, @EndYM, @AccountId;
			END;

			-- 關閉游標
			CLOSE CustomerViewCursor;
			DEALLOCATE CustomerViewCursor;
			EXEC [iOPEN].[dbo].[SetupUnifiedSecurity]
				@UserName = @DomainUser,
				@StartDate = @StartYM,
				@EndDate = @EndYM,
				@IsADUser = 1,
				@IsOwner = 0,
				@IsnonDate = 1,
				@CheckDate = @IsShareDT,
				@CustomView = 'S',
				@DBName = @DBName,
				@ViewName = @ViewName,
				@DisplayViewName = '',
				@Source = @inPublishId,
				@inIsEnable = @inIsEnable;*/
		END
		ELSE IF @inIsEnable in ('11') 
		BEGIN
		    --2025/05/02 Jay新增刪除分享後自訂View功能(Sx系列)
		    Print '11:刪除自訂View';
			SELECT @ViewName = dbo.fnGetiDataCenterInfo('3', @inCustViewid,'1','');
			PRINT 'ORIViewName:'+@ORIViewName;
			PRINT 'ViewName: ' + @ViewName + ', PublishId: ' + @PublishId + ', Frequency: ' + CAST(@Frequency AS NVARCHAR);
			SELECT @DomainUser = dbo.fnGetDomainUserByAccountId(@inAccountid);
			SELECT @DBName = dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1','');
			SELECT @IsShareDT = ISNULL(dbo.fnGetiDataCenterInfo('9', @inCustViewid,'1',''),'N');
			print 'DomainUser:'+@DomainUser+', DBName:'+@DBName+', IsShareDT:'+@IsShareDT;
				EXEC [iOPEN].[dbo].[SetupUnifiedSecurity]
					@UserName = @DomainUser,
					@StartDate = '',
					@EndDate = '',
					@IsADUser = 1,
					@IsOwner = 0,
					@IsnonDate = 1,
					@CheckDate = @IsShareDT,
					@CustomView = 'S',
					@DBName = @DBName,
					@ViewName = @ViewName,
					@DisplayViewName = '',
					@Source = @inCustViewId,
					@inIsEnable = @inIsEnable;
		END
END;
GO


