USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[sp_GetViewFieldsAndFilters_SQL]    Script Date: 2025/12/7 上午 10:56:00 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




























































/*
=============================================================================
  描述：[進行數據平台各功能資料回寫至Temp，並讓平台功能處理後，更新至正式資料，並傳回SQL提供平台刷新資料]
  
  版本記錄：
  ------------------------------------------------------------------------------
  版本   日期          作者             描述
  1.0    2024-11-22    Jay Hsu        初始版本
  1.1    2024-12-13    Jay Hsu        調整保存會蓋掉別名問題
  1.2    2025-01-17    Vic Wang       1.調整 fn_ConvertToOpenQuery_TEST 判斷條件，增加暫時排除 isEnable <> 74
								      2.@IsEnable = '1' 時，增加 AccountId，解決分享時刪到同時開啟的其他使用者的資料
  1.3    2025-01-22    Weiping Chung  新加入條件,必須該使用人可以使用的CustView								  
  1.4    2025-02-03    Jay Hsu        1.將WP 2025/01/22改的條件更新到此條件資訊		
  1.5    2025-02-14    Jay Hsu        1.加入Dinstinct功能，資料位置為tbwksitem，相關有影響到此table的作業都加上IsDistinct欄位
  1.5.1  2025-04-01    Jay Hsu        移除SHAREDT條件判斷
  1.5.2  2025-04-09    Jay Hsu        移除另存showfield=1的判別,解決另存時將隱藏及where條件的欄位未記錄
  1.5.3  2025-05-02    Jay Hsu        新增刪除分享後自訂View功能(Sx系列)
  1.5.4  2025-05-23    Jay Hsu        新增派送優化delivery
  1.6    2025-05-29    Jay Hsu        新增Onwer一對多機制(table調整:tbcustview->vcustview)
  1.7    2025-08-14    Jay Hsu        加入Agent代理機制(代理、同群組分享)
    
  
  參數說明：
  ------------------------------------------------------------------------------
  @CustViewId varchar(100)   自訂View Id       必填
  @ViewName   Varchar(100)   ViewName          選填              
  @IsEnable   Char(2)        資料處理狀態      必填              
  @AccountId  Varchar(100)   平台登入使用者Id  必填              
  @ViewNo     Varchar(100)   自訂View別名      選填              
  @QueryQty   Varchar(20)    資料筆數          選填              選填，預設值=100
  
  返回值：
  ------------------------------------------------------------------------------
  返回型態：SQL語法
  返回說明：[提供數據平台進行資料刷新使用]
  
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
  EXEC sp_GetViewFieldsAndFilters_SQL 
    @CustViewid = 'FC4AA5EA-91EE-4B68-B42E-4109A6B654CE',
	@ViewName = '',
	@IsEnable = '2',
	@AccountId = 'CEA7EEAD-C1D8-452E-97BE-DB7F3D1D2E6A', 
	@ViewNo ='',
	@QueryQty ='100';
  
  注意事項：
  ------------------------------------------------------------------------------
  1. [重要注意事項1]
  2. [效能考量說明]
  3. [業務邏輯特殊處理]
	 1.1 [Enable]-處理狀態
		1.使用
		11.刪除自訂
		2.正式
		3.暫存 -> 若按保存會把3改為正式版本
		4.上一版本
		5.另存
		6.分享(temp2tb) 
		  61.刪除分享 
		  62.批次分享查詢
		  63.批次分享更新
		7.派送(temp2tb) 
		  71.刪除派送 
		  72.派送查詢 
		  73.大批分享更新
		  74.資料派送
		8.匯出 (From Temp) 
		9.開發保留
=============================================================================
*/
CREATE OR ALTER                           PROCEDURE [dbo].[sp_GetViewFieldsAndFilters_SQL]
    @CustViewid VARCHAR(100),
    @ViewName VARCHAR(100),
	@IsEnable CHAR(2),
	@Accountid VARCHAR(100),
	@ViewNo VARCHAR(100),
	@QueryQty VARCHAR(20) = '100' --值為-1則是帶出所有筆數
	/*enable-SP 參數 
	自訂View:
	  1:(把temp 3刪掉,將1新增成3) ,
	儲存:
	  3:將正式-前一版4刪除,將1改成4,再將temp 3新增到正式為1,
	另存:
	  5:將temp 3新增至正式但調整新ID,新layer,新layerno
	自訂View修改確認(查暫存表):
	  2:自訂View暫存作業*/
AS
BEGIN
	DECLARE @SelectFields VARCHAR(MAX);
	DECLARE @OldSelectFields VARCHAR(MAX);
	DECLARE @GroupFields VARCHAR(MAX);
	DECLARE @WhereFields VARCHAR(MAX);
	DECLARE @CombinedQuery VARCHAR(MAX);
	DECLARE @CombinedQueryCount NCHAR(20);
	DECLARE @CreateViewQuery VARCHAR(MAX);
	DECLARE @DBName VARCHAR(10);
	DECLARE @Layer VARCHAR(10);
	DECLARE @LayerNo VARCHAR(10);
	DECLARE @OwnerAccountId VARCHAR(36);
	DECLARE @CustViewLayer VARCHAR(10);
	DECLARE @ShareCustViewLayer VARCHAR(10);
	DECLARE @SelectTable VARCHAR(100) = '[iTemp].[dbo].[tmpCustView_DataAccess]';
	--2025/01/22 Weiping加入@WhereModifyUser
	DECLARE @WhereModifyUser VARCHAR(100) = '[iTemp].[dbo].[tmpCustView_DataAccess]';
	DECLARE @NewID VARCHAR(36);
	DECLARE @RunEnable VARCHAR(10);
	DECLARE @DomainUser VARCHAR(20);
	DECLARE @OldViewName VARCHAR(100);
	DECLARE @ParentViewName VARCHAR(100);
	DECLARE @SQL NVARCHAR(MAX);
	DECLARE @OutputValue NVARCHAR(MAX);
	DECLARE @WebFunCode VARCHAR(10);
	DECLARE @SQLQueryQty VARCHAR(30);
	DECLARE @PublishId VARCHAR(36);
	DECLARE @Shared_FLG INT;
	DECLARE @IsShareDT VARCHAR(20);
	DECLARE @IsPublished CHAR(2);
    DECLARE @UpdatePublishId NVARCHAR(36);
	DECLARE @UpdateCustViewId NVARCHAR(36);
	DECLARE @FnLayer NVARCHAR(3);
	DECLARE @SharePublishWhere NVARCHAR(MAX);
	DECLARE @GetParentId NVARCHAR(36);
	DECLARE @GetIsDistinct NVARCHAR(10); -- 執行workspace時 enable為1時抓取
	DECLARE @IsDistinct NVARCHAR(10);  --從前端temp資料抓取現在狀況
	DECLARE @GetWksItemKindId NVARCHAR(36);
    DECLARE @StartTime DATETIME;
    DECLARE @EndTime DATETIME;
	DECLARE @TimeDiff INT;
	DECLARE @AgentAccountId VARCHAR(36);
	DECLARE @CustOwnerId VARCHAR(36);
    
    SET NOCOUNT ON;

    -- 如果 @ViewName 為空，則使用 SELECT 賦值
    IF LEN(@CustViewid) <> 0
    BEGIN
		   Print '111';
        --SELECT @ViewName = dbo.fnCustViewName(@CustViewid);
		--SELECT @ViewName = viewname FROM [dbo].[tbCustView] where Id = @CustViewid and Enable = '1';

			--抓出現有ViewName及上一層ViewName
             /*SELECT @ViewName = A.ViewName, @ParentViewName = isnull(B.ViewName,A.ViewName) FROM [dbo].[tbCustView] A 
			 LEFT JOIN [dbo].[tbCustView] B
			 ON A.ParentId = B.Id
			 WHERE A.id = @CustViewid;*/

			 --抓出現有ViewName及Source ViewName
             SELECT 
			 @ViewName = A.ViewName, @ParentViewName = isnull(B.ViewName,A.ViewName) 
			 FROM [dbo].[vCustView] A 
			 LEFT JOIN [dbo].[vCustView] B
			 --ON A.MasterId = B.ParentId
			 ON A.ParentId = B.Id AND a.Enable = b.Enable
			 WHERE A.id = @CustViewid and A.Enable = '1'
			 --2025/01/22 WEIPING CHUNG 新加入條件,必須該使用人可以使用的CustView
			 --2025/07/01 Jay Hsu 加入代理機制(tbWksItem->vWksItem)
			 AND A.id IN (SELECT DISTINCT CustViewId FROM vWksItem WHERE AccountId =@Accountid AND CustViewId = @CustViewid AND ENABLE = '1')
			 GROUP BY A.ViewName,B.ViewName;
    END
		   Print '222';
    --PRINT '開始執行...'
    --PRINT '變數 @ViewName = ' + CAST(@ViewName AS VARCHAR)
	--PRINT '變數 @ViewName = ' + CAST(@ViewName AS VARCHAR)
    -- 其他程式碼
    --PRINT '執行到步驟 2'
	--加入代理人處理
	SELECT @CustOwnerId = COALESCE(OwnerId, AccountId)
	FROM itemp.dbo.tmpCustView_DataAccess
	WHERE CustViewId = @CustViewid 
		AND enable = '3'
		AND ModifyUser = @Accountid
	GROUP BY AccountId, OwnerId;

	--SELECT @CustViewLayer = dbo.fnCustViewLayer(@CustViewid);
	select @CustViewLayer =Max(a.LayerNo)+1 from [dbo].[vCustView] A
	inner join 
	(select MasterId from [dbo].[vCustView]
	--where id = @CustViewid) B
	--2025/02/03 Jay將WP 2025/01/22改的條件更新到此條件資訊
	--2025/07/01 Jay Hsu 加入代理機制(tbWksItem->vWksItem)
	where id = (SELECT DISTINCT CustViewId FROM vWksItem WHERE AccountId =@CustOwnerId/*@Accountid*/ AND CustViewId = @CustViewid AND ENABLE = '1')) B
	ON A.MasterId = B.MasterId and Enable = '1' AND A.Layer in ('1','2')
	--2025/01/22 WEIPING CHUNG 新加入條件,必須該使用人可以使用的CustView
	--where A.id IN (SELECT DISTINCT CustViewId FROM tbWksItem WHERE AccountId =@Accountid AND CustViewId = @CustViewid AND ENABLE = '1')
	;

	Print '333'
	--Share CustView
	select @ShareCustViewLayer =Max(a.LayerNo)+1 from [dbo].[vCustView] A
	inner join 
	(select MasterId from [dbo].[vCustView]
	--2025/02/03 Jay將WP 2025/01/22改的條件更新到此條件資訊
	--2025/07/01 調整tbCustView->vCustView及tbWksItem->vWksItem
	where id = (SELECT DISTINCT CustViewId FROM vWksItem WHERE AccountId =@CustOwnerId/*@Accountid*/ AND CustViewId = @CustViewid AND ENABLE = '1')) B
	ON A.MasterId = B.MasterId and Enable = '1' AND A.Layer in ('1','3')
	--2025/01/22 WEIPING CHUNG 新加入條件,必須該使用人可以使用的CustView
	--where A.id IN (SELECT DISTINCT CustViewId FROM tbWksItem WHERE AccountId =@Accountid AND CustViewId = @CustViewid AND ENABLE = '1')
	;
	--GetTableOwnerAccountId
	SELECT @OwnerAccountId = dbo.fnGetiDataCenterInfo('6', @CustViewid,'1',@CustOwnerId/*@Accountid*/);
Print '444'
	--SELECT @CombinedQuery AS OutputValue;
    --PRINT '變數 @CustViewLayer = ' + CAST(@CustViewLayer AS VARCHAR)
	--PRINT '變數 @ShareCustViewLayer = ' + CAST(@ShareCustViewLayer AS VARCHAR)
	--PRINT '變數 @OwnerAccountId = ' + CAST(@OwnerAccountId AS VARCHAR)
    -- 其他程式碼
    --PRINT '執行到步驟 3'

	--vCustView_DataAccess資料處理
	IF @IsEnable = '1'
	BEGIN
	   SELECT @GetIsDistinct = dbo.fnGetiDataCenterInfo('17', @CustViewid, '',@Accountid);
	   --SET @GetIsDistinct = '0';
	   --2025/1/21 Vic Where @IsEnable = '1' 時，增加 AccountId，解決分享時刪到同時開啟的其他使用者的資料
	   DELETE from [iTemp].[dbo].[tmpCustView_DataAccess]
	   WHERE ViewName = @ViewName AND Enable in ('3','0')
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid
	   ;

	   DELETE from [iTemp].[dbo].[tmpCustView_Publish]
	   WHERE ViewName = @ViewName AND Enable in ('3','0')
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid
	   ;
	   --分享用S
	    SELECT @PublishId = Id 
		FROM [iDataCenter].[dbo].[tbPublish] 	   
		WHERE CustViewId = @CustViewid
		--2025/01/22 WEIPING CHUNG 新加入條件,必須該使用人可以使用的CustView
		--2025/07/01 Jay Hsu 加入代理機制(tbWksItem->vWksItem)
		and CustViewId IN (SELECT DISTINCT CustViewId FROM vWksItem WHERE AccountId =@Accountid AND CustViewId = @CustViewid AND ENABLE = '1')
		group by Id;

	   DELETE from [iTemp].[dbo].[tmpPublish]
	   where CustViewId = @CustViewid
	   AND Enable in ('3','0')
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid
	   ;


	   DELETE from [iTemp].[dbo].[tmpPublishPeriod]
	   where PublishId = @PublishId
	   AND Enable in ('3','0')
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	   
	   ;
	   DELETE from [iTemp].[dbo].[tmpWksItem]
	   where CustViewId = @CustViewid and Len(PublishId)>0
	   AND Enable in ('3','0')
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	   
	   ;
	   --分享用E
	   print '1:555';
    SET @StartTime = GETDATE()
	   INSERT INTO [iTemp].[dbo].[tmpCustView_DataAccess](
        [CustViewId],
		[ParentId],
        [ResId],
        [ResSchemaId],
        [CustViewSchemaId],
        [Organize],
        [EmpNo],
        [Layer],
        [LayerNo],
        [DbName],
        [ViewName],
		[ViewNo],
        [FieldName],
        [FieldType],
        [NickName],
        [ShowField],
        [Summarize],
        [SortKind],
        [SortKey],
        [Seq],
		--[IsDistinct],
        [IsTimeVar],
        [FilterGroup],
        [Operator],
        [Val],
		[FilterUser],
		[Enable],
		[AccountId],
		[OwnerId],
		[ModifyUser],
        [ModifyTime]
		)
		-- 當前層數據
		SELECT [CustViewId]
			  ,[ParentId]
			  ,[ResId]
			  ,[ResSchemaId]
			  ,[CustViewSchemaId]
			  ,[Organize]
			  ,[EmpNo]
			  ,[Layer]
			  ,[LayerNo]
			  ,[DbName]
			  ,[ViewName]
			  ,[ViewNo]
			  ,[FieldName]
			  ,[FieldType]
			  ,[NickName]
			  ,[ShowField]
			  ,[Summarize]
			  ,[SortKind]
			  ,[SortKey]
			  ,[Seq]
			  --,@GetIsDistinct
			  ,[IsTimeVar]
			  ,[FilterGroup]
			  ,[Operator]
			  ,[Val]
			  ,[FilterUser]
			  ,'3' AS [Enable]
			  ,[AccountId]
			  ,[OwnerId]
			  ,@Accountid
			  ,GETDATE()--[ModifyTime] 
		FROM vCustView_DataAccess
		WHERE CustViewId = @CustViewid
		AND Enable = @IsEnable
		
		and CustViewId IN (SELECT DISTINCT CustViewId FROM vWksItem WHERE AccountId =@Accountid AND CustViewId = @CustViewid AND ENABLE = '1')
		-- 修正代理機制條件
		and (
			-- 第一優先：直接匹配
			(Accountid = @Accountid)
			OR
			-- 第二優先：只有在沒有直接匹配時才執行
			(Accountid <> @Accountid 
				AND OwnerId IS NULL 
				AND NOT EXISTS (
					SELECT 1 FROM vCustView_DataAccess vcd_inner 
					WHERE vcd_inner.CustViewId = @CustViewid
					AND vcd_inner.Enable = '1'
					AND vcd_inner.Accountid = @Accountid
				)
				--AND Accountid NOT IN (SELECT AccountId FROM vWksItem t2 WHERE t2.Accountid = '525121DA-5789-43DE-BA28-1A93C03FE9D3' and CustViewId = '212DA30E-FF52-4AE3-B5F7-9BFE24E9E793' AND ENABLE = '1'))
				AND Accountid NOT IN (SELECT AccountId FROM vWksItem t2 WHERE t2.Accountid = @Accountid and CustViewId = @CustViewid AND ENABLE = '1')
				)
		)
		/*WHERE CustViewId = @CustViewid
		AND Enable = @IsEnable
		--2025/01/22 WEIPING CHUNG 新加入條件,必須該使用人可以使用的CustView
		--2025/07/01 Jay Hsu 加入代理機制(tbWksItem->vWksItem)
		and CustViewId IN (SELECT DISTINCT CustViewId FROM vWksItem WHERE AccountId =@Accountid AND CustViewId = @CustViewid AND ENABLE = '1')
		--Agent調整
		and ((Accountid = @Accountid)  -- 第一優先
         OR 
       (Accountid <> @Accountid AND OwnerId IS NULL AND 
        Accountid IN (SELECT AccountId FROM vWksItem t2 WHERE t2.Accountid = @Accountid and CustViewId = @CustViewid AND ENABLE = '1')))*/
		--and ((Accountid = @Accountid) or (Accountid <> @Accountid and OwnerId is null))
		--Agent
		UNION

		-- 上層數據
		--需再確認會不會卡住
		SELECT 
			c.[CustViewId]
			,c.[ParentId]
			,c.[ResId]
			,v.[ResSchemaId]
			,v.[CustViewSchemaId]
			,c.[Organize]
			,c.[EmpNo]
			,c.[Layer]
			,c.[LayerNo]
			,v.[DbName]
			,c.[ViewName]
			,c.[ViewNo]
			,v.[FieldName]
			,v.[FieldType]
			,v.[NickName]
			,'0' as ShowField
			,v.[Summarize]
			,v.[SortKind]
			,v.[SortKey]
			,v.[Seq]
			--,@GetIsDistinct
			,v.[IsTimeVar]
			,v.[FilterGroup]
			,v.[Operator]
			,v.[Val]
			,v.[FilterUser]
			,'3' AS [Enable]
			,c.AccountId
			,c.OwnerId
			,@Accountid
			,GETDATE()--v.[ModifyTime]
		FROM vCustView_DataAccess v
		CROSS JOIN (
			-- 獲取當層的固定值
			SELECT TOP 1 
				CustViewId, ParentId, ResId, 
				Organize, EmpNo, Layer, LayerNo,ViewName, ViewNo, AccountId, OwnerId
			FROM vCustView_DataAccess
			WHERE CustViewId = @CustViewid 
			AND Enable = @IsEnable
		) c
		JOIN (
			-- 獲取當層的 ParentId
			SELECT DISTINCT ParentId 
			FROM vCustView_DataAccess
			WHERE CustViewId = @CustViewid 
			AND Enable = @IsEnable
			AND ParentId IS NOT NULL
		) p ON v.CustViewId = p.ParentId
		WHERE v.Enable = @IsEnable AND ((c.AccountId = @Accountid and c.Layer = '2') or (c.AccountId = @Accountid and c.Layer = '3'))
		AND v.FieldName NOT IN (
			-- 排除當層已有的 fieldname
			SELECT FieldName 
			FROM vCustView_DataAccess
			WHERE CustViewId = @CustViewid 
			AND Enable = @IsEnable 
			/*1.5.1  2025-04-01    Jay Hsu        移除SHAREDT條件判斷
			AND FieldName <> 'ShareDT'*/

		)
		;

    SET @EndTime = GETDATE()
	SET @TimeDiff = DATEDIFF(millisecond, @StartTime, @EndTime)
    PRINT 'Execution Time: ' + CAST(@TimeDiff AS VARCHAR(20)) + ' milliseconds'
		print '1:完成temp_dataaccess';

		update [iTemp].[dbo].[tmpCustView_DataAccess]
		set IsDistinct = @GetIsDistinct
		where CustViewId = @CustViewid 
		  and ModifyUser = @Accountid;
	   /*SELECT [CustViewId]
	  ,[ParentId]
      ,[ResId]
      ,[ResSchemaId]
      ,[CustViewSchemaId]
      ,[Organize]
      ,[EmpNo]
      ,[Layer]
      ,[LayerNo]
      ,[DbName]
      ,[ViewName]
      ,[FieldName]
      ,[FieldType]
      ,[NickName]
      ,[ShowField]
      ,[Summarize]
      ,[SortKind]
      ,[SortKey]
      ,[Seq]
      ,[IsTimeVar]
      ,[FilterGroup]
      ,[Operator]
      ,[Val]
      ,'3'
	  ,[AccountId]
	  ,@Accountid
      ,[ModifyTime] 
	  FROM vCustView_DataAccess
	   WHERE CustViewId = @CustViewid AND Enable = @IsEnable;*/
	INSERT INTO [iTemp].[dbo].[tmpCustView_Publish](
	   [Kind]
      ,[OwnerId]
	  ,[CustViewOwnerId]
      ,[OwnerEmpNo]
      ,[OwnerEmpName]
      ,[CustViewId]
      ,[ParentId]
      ,[MasterId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewName]
      ,[ViewNo]
      ,[PublishId]
      ,[PublishPeriodId]
      ,[WksItemId]
      ,[PublishUsersId]
      ,[GroupNm]
      ,[Frequency]
      ,[NotifyStatus]
      ,[StartDT]
      ,[EndDT]
	  ,[IsDistinct]
      ,[ResSchemaId]
      ,[ResColumnNm]
      ,[PDay]
      ,[PTime]
      ,[Notify]
      ,[Period]
      ,[VariableId]
      ,[StartYM]
      ,[EndYM]
      ,[AccountId]
      ,[AccountKind]
      ,[EmpNo]
      ,[EmpName]
      ,[EmpEmail]
      ,[DeptCode]
      ,[DeptName]
      ,[FirstNickNm]
      ,[SecontNickNm]
      ,[FileId]
      ,[YearOffset]
      ,[Effective]
	  ,[Delivery]
      ,[Ememo]
      ,[WksNodeId]
      ,[WksItemKindId]
      ,[PublishWhere]
      ,[Enable]
      ,[ModifyUser]
      ,[ModifyTime]
      ,[CreateUser]
      ,[CreateTime]
	)SELECT 
	       [Kind]
		  ,[OwnerId]
		  ,[CustViewOwnerId]
		  ,[OwnerEmpNo]
		  ,[OwnerEmpName]
		  ,[CustViewId]
		  ,[ParentId]
		  ,[MasterId]
		  ,[Layer]
		  ,[LayerNo]
		  ,[ViewName]
		  ,[ViewNo]
		  ,[PublishId]
		  ,[PublishPeriodId]
		  ,[wksItemId]
		  ,[publishUsersId]
		  ,[GroupNm]
		  ,[Frequency]
		  ,[NotifyStatus]
		  ,[StartDT]
		  ,[EndDT]
		  ,@GetIsDistinct
		  ,[ResSchemaId]
		  ,[ResColumnNm]
		  ,[PDay]
		  ,[PTime]
		  ,[Notify]
		  ,[Period]
		  ,[VariableId]
		  ,[StartYM]
		  ,[EndYM]
		  ,[AccountId]
		  ,[AccountKind]
		  ,[EmpNo]
		  ,[EmpName]
		  ,[EmpEmail]
		  ,[DeptCode]
		  ,[DeptName]
		  ,[FirstNickNm]
		  ,[SecontNickNm]
		  ,[FileId]
		  ,[YearOffset]
		  ,[Effective]
		  ,[Delivery]
		  ,[Ememo]
		  ,[WksNodeId]
		  ,[WksItemKindId]
		  ,[PublishWhere]
		  ,'3'
		  ,@Accountid
		  ,GETDATE()
		  ,[CreateUser]
		  ,[CreateTime]
		   FROM [iDataCenter].[dbo].[vCustView_Publish]
		   where CustViewId = @CustViewid 
		   AND Enable = @IsEnable 
		   --Agent
		   AND OwnerId = @Accountid
		   ;
		   

	INSERT INTO [iTemp].[dbo].[tmpPublish](
		   [Id]
		  ,[CustViewId]
		  ,[FileId]
		  ,[Kind]
		  ,[Name]
		  ,[Frequency]
		  ,[Period]
		  ,[Notify]
		  ,[NotifyStatus]
		  ,[PDay]
		  ,[PTime]
		  ,[StartDT]
		  ,[EndDT]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime])
		   SELECT  
		   [PublishId]
		  ,[CustViewId]
		  ,[FileId]
		  ,[Kind]
		  ,[GroupNm]
		  ,[Frequency]
		  ,[Period]
		  ,[Notify]
		  ,[NotifyStatus]
		  ,[PDay]
		  ,[PTime]
		  ,[StartDT]
		  ,[EndDT]
		  ,'3'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@Accountid
		  ,GETDATE()
		   FROM [iTemp].[dbo].[tmpCustView_Publish]
		   where CustViewId = @CustViewid and len(publishid) > 0
		   AND Enable = @IsEnable AND AccountId = @Accountid;

		   INSERT INTO [iTemp].[dbo].[tmpPublishPeriod](
		   [Id]
		  ,[PublishId]
		  ,[VariableId]
		  ,[ResSchemaId]
		  ,[ResColumnNm]
		  ,[StartYM]
		  ,[EndYM]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime])
		   SELECT 
		   newid()
		  ,[PublishId]
		  ,[VariableId]
		  ,[ResSchemaId]
		  ,[ResColumnNm]
		  ,[StartYM]
		  ,[EndYM]
		  ,'3'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@Accountid
		  ,GETDATE()
		   FROM [iTemp].[dbo].[tmpCustView_Publish]
		   where CustViewId = @CustViewid 
		   --PublishId = @PublishId 
		   AND Enable = @IsEnable AND AccountId = @Accountid;

		   INSERT INTO [iTemp].[dbo].[tmpWksItem](
		   [Id]
		  ,[CustViewId]
		  ,[WksNodeId]
		  ,[WksItemKindId]
		  ,[PublishId]
		  ,[StartDT]
		  ,[Effective]
		  ,[AccountId]
		  ,[Ememo]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime])
		   SELECT 
		   newid()
		  ,[CustViewId]
		  ,[WksNodeId]
		  ,[WksItemKindId]
		  ,[PublishId]
		  ,[StartDT]
		  ,[Effective]
		  ,[AccountId]
		  ,[Ememo]
		  ,'3'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,GETDATE()
		  FROM [iTemp].[dbo].[tmpCustView_Publish]
		   where CustViewId = @CustViewid
		   AND Enable = @IsEnable and len(PublishId)>0 AND AccountId = @Accountid;
	   /*INSERT INTO [iTemp].[dbo].[tmpPublish](
	   [Id]
      ,[CustViewId]
      ,[FileId]
      ,[Kind]
      ,[Name]
      ,[Frequency]
      ,[Period]
      ,[Notify]
      ,[NotifyStatus]
      ,[PDay]
      ,[PTime]
      ,[StartDT]
      ,[EndDT]
      ,[Enable]
      ,[CreateUser]
      ,[CreateTime]
      ,[ModifyUser]
      ,[ModifyTime])
	   SELECT  
	   [Id]
      ,[CustViewId]
      ,[FileId]
      ,[Kind]
      ,[Name]
      ,[Frequency]
      ,[Period]
      ,[Notify]
      ,[NotifyStatus]
      ,[PDay]
      ,[PTime]
      ,[StartDT]
      ,[EndDT]
      ,'3'
      ,[CreateUser]
      ,[CreateTime]
      ,[ModifyUser]
      ,GETDATE()
	   FROM [iDataCenter].[dbo].[tbPublish]
	   where CustViewId = @CustViewid 
	   AND Enable = @IsEnable;

	   INSERT INTO [iTemp].[dbo].[tmpPublishPeriod](
	   [Id]
      ,[PublishId]
      ,[VariableId]
      ,[ResSchemaId]
      ,[ResColumnNm]
      ,[StartYM]
      ,[EndYM]
      ,[Enable]
      ,[CreateUser]
      ,[CreateTime]
      ,[ModifyUser]
      ,[ModifyTime])
	   SELECT 
	   [Id]
      ,[PublishId]
      ,[VariableId]
      ,[ResSchemaId]
      ,[ResColumnNm]
      ,[StartYM]
      ,[EndYM]
      ,'3'
      ,[CreateUser]
      ,[CreateTime]
      ,[ModifyUser]
      ,GETDATE()
	   FROM [iDataCenter].[dbo].[tbPublishPeriod]
	   where PublishId = @PublishId 
	   AND Enable = @IsEnable;

	   INSERT INTO [iTemp].[dbo].[tmpWksItem](
	   [Id]
      ,[CustViewId]
      ,[WksNodeId]
      ,[WksItemKindId]
      ,[PublishId]
      ,[StartDT]
      ,[Effective]
      ,[AccountId]
      ,[Ememo]
      ,[Enable]
      ,[CreateUser]
      ,[CreateTime]
      ,[ModifyUser]
      ,[ModifyTime])
	   SELECT 
	   [Id]
      ,[CustViewId]
      ,[WksNodeId]
      ,[WksItemKindId]
      ,[PublishId]
      ,[StartDT]
      ,[Effective]
      ,[AccountId]
      ,[Ememo]
      ,'3'
      ,[CreateUser]
      ,[CreateTime]
      ,[ModifyUser]
      ,GETDATE()
	  FROM [iDataCenter].[dbo].[tbWksItem]
	   where CustViewId = @CustViewid
	   AND Enable = @IsEnable and len(PublishId)>0;
	   */
	END
	ELSE IF @IsEnable = '11'  --自訂View刪除
	BEGIN
		SELECT @IsPublished = dbo.fnGetiDataCenterInfo('11', @CustViewid,'1','');
		IF LEN(@IsPublished) = 0
		BEGIN 
			Set @IsPublished = 'N';
		END
		
       --Select @Shared_FLG = count(*) from tbPublish
	   --where Enable = '1' and CustViewId = @CustViewid;
	   IF @IsPublished = 'N' 
	   BEGIN
		   DECLARE PublishIDCursor CURSOR FOR
		   SELECT id FROM [dbo].[tbPublish]
		   WHERE CustViewId = @CustViewId AND Enable in ('1')
		   GROUP BY Id;
	   
		   OPEN PublishIDCursor;

		   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;

		   WHILE @@FETCH_STATUS = 0
		   BEGIN 
			
			   Update [dbo].[tbPublishPeriod]
			   SET Enable = '4'
			   where PublishId = @UpdatePublishId
				 and Enable = '1';

			   Update [dbo].[tbPublishUsers]
			   SET Enable = '4'
			   where PublishId = @UpdatePublishId
				 and Enable = '1';

			   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;
		   END
			-- 關閉CURSOR
			CLOSE PublishIDCursor;
			DEALLOCATE PublishIDCursor;

			--2025/05/02 Jay新增刪除分享後自訂View功能(Sx系列)
			exec [dbo].[GetSharedCustViewData] @inCustViewId = @CustViewid, @inPublishId = @UpdatePublishId, @inIsEnable = @IsEnable, @inAccountId = @Accountid;

		   DELETE from [iTemp].[dbo].[tmpCustView_DataAccess]
		   WHERE ViewName = @ViewName AND Enable = '3';

		   UPDATE [iDataCenter].[dbo].[tbCustView]
		   SET ENABLE = '4'
		   WHERE id = @CustViewid AND Enable = '1';

		   UPDATE [iDataCenter].[dbo].[tbCustViewSchema]
		   SET ENABLE = '4'
		   WHERE CustViewid = @CustViewid AND Enable = '1';

		   UPDATE [iDataCenter].[dbo].[tbCustViewFilter]
		   SET ENABLE = '4'
		   WHERE CustViewid = @CustViewid AND Enable = '1';

		   UPDATE [iDataCenter].[dbo].[tbPublish]
		   SET ENABLE = '4'
		   WHERE CustViewid = @CustViewid AND Enable = '1';

		   UPDATE [iDataCenter].[dbo].[tbWksItem]
		   SET ENABLE = '4'
		   WHERE CustViewid = @CustViewid AND Enable = '1';


	   END
	END
	ELSE IF @IsEnable = '3' --儲存
	BEGIN
	   SELECT @NewID = NEWID();

	   Print '執行儲存';

		--加入代理人處理
		SELECT @CustOwnerId = COALESCE(OwnerId, AccountId)
		FROM itemp.dbo.tmpCustView_DataAccess
		WHERE CustViewId = @CustViewid 
		  AND enable = '3'
		  AND ModifyUser = @Accountid
		GROUP BY AccountId, OwnerId;

		-- 檢查是否有找到資料
		IF @@ROWCOUNT > 1
		BEGIN
			RAISERROR('查詢返回多筆資料，請確認條件是否正確', 16, 1);
			RETURN;
		END

		/*
		IF @CustOwnerId IS NOT NULL
		BEGIN
			SET @Accountid = @CustOwnerId;
		END
		*/

	   SELECT @GetWksItemKindId = dbo.fnGetiDataCenterInfo('18', @CustViewid,'',@CustOwnerId);
	   
	   --DELETE 上一版(tbCustView,tbCustViewSchema,tbCustViewFilter)
	   DELETE from [dbo].[tbCustView]
	   WHERE id = @CustViewId AND Enable = '4';

	   DELETE from [dbo].[tbCustViewSchema]
	   WHERE CustViewId = @CustViewId AND Enable = '4';

	   DELETE from [dbo].[tbCustViewFilter]
	   WHERE CustViewId = @CustViewId AND Enable = '4';

	   DELETE from [dbo].[tbWksItem]
	   WHERE CustViewId = @CustViewId AND Enable = '4';

	   --2025/01/02 新增分享派送資料處理
	   DECLARE PublishIDCursor CURSOR FOR
	   SELECT id FROM [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable in ('4')
	   GROUP BY Id;
	   
	   OPEN PublishIDCursor;

	   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;

	   WHILE @@FETCH_STATUS = 0
	   BEGIN 
		   /*DELETE from [dbo].[tbPublishPeriod]
		   WHERE PublishId = @CustViewId AND Enable = '4';
		   DELETE from [dbo].[tbPublishUsers]
		   WHERE PublishId = @CustViewId AND Enable = '4';			*/
		   DELETE [dbo].[tbPublishPeriod]
		   where PublishId = @UpdatePublishId
			 and Enable IN ('4');

		   DELETE [dbo].[tbPublishUsers]
		   where PublishId = @UpdatePublishId
			 and Enable IN ('4');

		   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;
	   END
		-- 關閉游標
		CLOSE PublishIDCursor;
		DEALLOCATE PublishIDCursor;
		--2025/01/02 End

	   DELETE from [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable = '4';


	   --將原本版本改為1->4(變成上一版)
	   Update [dbo].[tbCustView]
	   SET Enable = '4'
	   where id = @CustViewid 
	     and Enable = '1';

	   Update [dbo].[tbCustViewSchema]
	   SET Enable = '4' 
	   where CustViewId = @CustViewid
	     and Enable = '1';

	   Update [dbo].[tbCustViewFilter]
	   SET Enable = '4'
	   where CustViewId = @CustViewid
	     and Enable = '1';

	   Update [dbo].[tbWksItem]
	   SET Enable = '4'
	   where CustViewId = @CustViewid
	     and Enable = '1';

	   --2025/01/02 新增分享派送資料處理
	   DECLARE PublishIDCursor CURSOR FOR
	   SELECT id FROM [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable in ('1')
	   GROUP BY Id;
	   
	   OPEN PublishIDCursor;

	   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;

	   WHILE @@FETCH_STATUS = 0
	   BEGIN 
		   Update [dbo].[tbPublishPeriod]
		   SET Enable = '4'
		   where PublishId = @UpdatePublishId
			 and Enable = '1';

		   Update [dbo].[tbPublishUsers]
		   SET Enable = '4'
		   where PublishId = @UpdatePublishId
			 and Enable = '1';
		   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;
	   END
		-- 關閉游標
		CLOSE PublishIDCursor;
		DEALLOCATE PublishIDCursor;

	   Update [dbo].[tbPublish]
	   SET Enable = '4'
	   where CustViewId = @CustViewid
	     and Enable = '1';

	   --2025/01/02 End

	   --將temp 3新增到正式為1
	   --[dbo].[tbCustView]
	   insert into [dbo].[tbCustView](
	   [Id]
      ,[ParentId]
      ,[MasterId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewNo]
      ,[ViewName]
      ,[AccountId]
      ,[Enable]
      ,[ModifyUser]
      ,[ModifyTime]
	  )
	  SELECT 
	  [CustViewId]
      ,[ParentId]
      ,[ResId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewNo]
      ,[ViewName]
      ,@CustOwnerId
      ,'1'
      ,@Accountid
      ,GETDATE()
	  FROM [iTemp].[dbo].[tmpCustView_DataAccess]
	  WHERE CustViewId = @CustViewId and Enable = '3'
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid	  
	  GROUP BY [CustViewId],[ParentId],[ResId],[Layer],[LayerNo],[ViewNo],[ViewName];

	  --[dbo].[tbCustViewSchema]
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
      ,[ModifyUser]
      ,[ModifyTime]
	  )
	  SELECT 
	   NewID()
      ,[CustViewId]
      ,[ResSchemaId]
      ,[FieldName]
      ,[ShowField]
      ,[NickName]
      ,[Summarize]
      ,[SortKind]
      ,[SortKey]
      ,[Seq]
      ,'1'
      ,@Accountid
      ,GETDATE()
	  FROM [iTemp].[dbo].[tmpCustView_DataAccess]
	  WHERE CustViewId = @CustViewId and Enable = '3' 
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	  
	  GROUP BY [CustViewId],[ResSchemaId],[FieldName],[ShowField],[NickName],[Summarize],[SortKind],[SortKey],[Seq];

	  --[dbo].[tbCustViewFilter]
	   insert into [dbo].[tbCustViewFilter](
	   [Id]
      ,[CustViewId]
      ,[ResSchemaId]
      ,[Operator]
      ,[Val]
      ,[FilterGroup]
	  ,[Enable]
	  ,[CreateUser]
      ,[ModifyUser]
      ,[ModifyTime]
	  )
	  SELECT 
	   NewID()
      ,[CustViewId]
      ,[ResSchemaId]
      ,[Operator]
      ,[Val]
      ,[FilterGroup]
	  ,'1'
	  ,[FilterUser]
      ,@Accountid
      ,GETDATE()
	  FROM [iTemp].[dbo].[tmpCustView_DataAccess]
	  WHERE CustViewId = @CustViewId and Enable = '3' AND Val IS NOT NULL
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid	  
	  GROUP BY [CustViewId],[ResSchemaId],[Operator],[Val],[FilterGroup],[FilterUser];

	Insert INTO [iDataCenter].[dbo].[tbWksItem](
		   [Id]
		  ,[CustViewId]
		  ,[WksNodeId]
		  ,[WksItemKindId]
		  ,[StartDT]
		  ,[Effective]
		  ,[IsDistinct]
		  ,[AccountId]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT Newid()
		  ,[CustViewId]
		  ,'24159CDB-714A-4BE9-9BBD-36863D7D2935'
		  ,@GetWksItemKindId
		  ,''
		  ,''
		  ,[IsDistinct]
		  ,@CustOwnerId		  --,@AccountId
		  ,'1'
		  ,@CustOwnerId       --,@AccountId
		  ,GetDATE()
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_DataAccess]
	  where [CustViewId] = @CustViewId
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid and FieldName <> 'ShareDT'	  
	  GROUP BY CustViewId,Layer,IsDistinct;

	--2025/01/02 新增分享派送資料處理
	Insert INTO [iDataCenter].[dbo].[tbPublish](
			[Id]
		  ,[CustViewId]
		  ,[FileId]
		  ,[Kind]
		  ,[Name]
		  ,[Frequency]
		  ,[Period]
		  ,[Notify]
		  ,[NotifyStatus]
		  ,[PDay]
		  ,[PTime]
		  ,[StartDT]
		  ,[EndDT]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT distinct PublishId--@PublishId
		  ,CustViewid
		  ,FileId
		  ,Kind
		  ,GroupNm
		  ,Frequency
		  ,Period
		  ,Notify
		  ,NotifyStatus
		  ,PDay
		  ,PTime
		  ,StartDT
		  ,EndDT
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where [CustViewId] = @CustViewId and Enable = '3' and PublishId is not null
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid	  
	   ;

	Insert INTO [iDataCenter].[dbo].[tbPublishPeriod](
			[Id]
		  ,[PublishId]
		  ,[VariableId]
		  ,[ResSchemaId]
		  ,[ResColumnNm]
		  ,[StartYM]
		  ,[EndYM]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT NewId()--Newid()
		  ,PublishId--@PublishId
		  ,VariableId
		  ,ResSchemaId
		  ,ResColumnNm
		  ,StartYM
		  ,EndYM
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where CustViewId= @CustViewId and Enable = '3' 
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	  
	  GROUP BY PublishId, VariableId, ResSchemaId, ResColumnNm, StartYM, EndYM, CreateUser, CreateTime;

	Insert INTO [iDataCenter].[dbo].[tbWksItem](
		   [Id]
		  ,[CustViewId]
		  ,[WksNodeId]
		  ,[WksItemKindId]
		  ,[PublishId]
		  --,[StartDT]
		  ,[Effective]
		  ,[IsDistinct]
		  ,[AccountId]
		  ,[Ememo]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT newId()--Newid()
		  ,CustViewId
		  ,'24159CDB-714A-4BE9-9BBD-36863D7D2935'
		  --,[WksItemKindId]
		  ,'3'
		  ,[PublishId]
		  --,[StartDT]
		  ,[Effective]
		  ,[IsDistinct]
		  ,[AccountId]
		  ,[Ememo]
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where [CustViewId] = @CustViewId and Enable = '3' and len(PublishId)>0
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid	  
	  GROUP BY CustViewId, WksItemKindId, PublishId, IsDistinct, Effective, AccountId, Ememo, CreateUser, CreateTime;

	Insert INTO [iDataCenter].[dbo].[tbPublishUsers](
		[Id]
      ,[PublishId]
      ,[AccountId]
      ,[Kind]
      --,[OTP]
	  ,[Delivery]
      ,[Enable]
      ,[CreateUser]
      ,[CreateTime]
      ,[ModifyUser]
      ,[ModifyTime]
	)
	SELECT newid()
      ,[PublishId]
      ,[AccountId]
      ,[Kind]
	  ,[Delivery]
      --,[OTP]
      ,'1'
      ,[CreateUser]
      ,[CreateTime]
      ,@Accountid
      ,GETDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where [CustViewId] = @CustViewId and Enable = '3' and len(PublishId)>0
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	  
	  GROUP BY PublishId, AccountId, Kind, Delivery, Enable, CreateUser, CreateTime;

	  exec [dbo].[UpdatetbPublishWhere] @CustViewId = @CustViewid;
	  --2025/01/02 End

	END
	ELSE IF @IsEnable = '5' -- 將temp 3新增至正式但調整新ID,新layer,新layerno
	BEGIN
		print '另存作業';
		--加入代理人處理
		SELECT @CustOwnerId = ISNULL(
			(SELECT COALESCE(OwnerId, AccountId)
			 FROM itemp.dbo.tmpCustView_DataAccess
			 WHERE CustViewId = @CustViewid
			   AND enable = '3'
			   AND ModifyUser = @Accountid
			   AND COALESCE(AccountId,OwnerId) = @Accountid  -- 新增這個條件
			 GROUP BY AccountId, OwnerId), 
			@Accountid
		);

		/*
		SELECT @CustOwnerId = COALESCE(OwnerId, AccountId)
		FROM itemp.dbo.tmpCustView_DataAccess
		WHERE CustViewId = @CustViewid 
		  AND enable = '3'
		  AND ModifyUser = @Accountid
		GROUP BY AccountId, OwnerId;*/

	   SELECT @NewID = NEWID();
	   insert into [dbo].[tbCustView](
	   [Id]
      ,[ParentId]
      ,[MasterId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewNo]
      ,[ViewName]
      ,[AccountId]
      ,[Enable]
      ,[ModifyUser]
      ,[ModifyTime]
	  )
	  SELECT 
	   @NewID
      ,CASE WHEN Layer = '1' THEN @CustViewid  --取消繼承，都接原本的媽媽
	        WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ = @CustOwnerId/*@Accountid*/ THEN ParentId
			WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ <> @CustOwnerId/*@Accountid*/ THEN CustViewId
			WHEN Layer = '3' THEN ParentId ELSE ParentId END
      ,[ResId]
      ,CASE WHEN Layer = '1' THEN '2' 
	        WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ = @CustOwnerId/*@Accountid*/ THEN '2' 
			WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ <> @CustOwnerId/*@Accountid*/ THEN '3' 
	        WHEN Layer = '3' THEN '3' ELSE Layer END AS Layer -- 將 Layer 1:Owner, 2:自訂View, 3:被分享的自訂
      ,CASE WHEN Layer = '1' THEN @CustViewLayer 
	        WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ = @CustOwnerId/*@Accountid*/ THEN @CustViewLayer 
	        WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ <> @CustOwnerId/*@Accountid*/ THEN @ShareCustViewLayer
	        WHEN Layer = '3' THEN @ShareCustViewLayer END AS LayerNo
      ,@ViewNo
	  ,CASE WHEN Layer = '1' THEN CASE WHEN @CustViewLayer = '0' THEN [ViewName]+'_C1' ELSE @ViewName+'_C'+@CustViewLayer END
	        WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ = @CustOwnerId/*@Accountid*/ THEN LEFT([ViewName], LEN([ViewName]) - PATINDEX('%[0-9][^0-9]%', REVERSE([ViewName])))+@CustViewLayer
			WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ <> @CustOwnerId/*@Accountid*/ AND @ShareCustViewLayer = '1' THEN [ViewName]+'_S1' 
			--WHEN Layer = '2' AND AccountId <> @CustOwnerId/*@Accountid*/ AND @ShareCustViewLayer <> '1' THEN LEFT([ViewName], LEN([ViewName]) - PATINDEX('%[0-9][^0-9]%', REVERSE([ViewName])))+@ShareCustViewLayer
			WHEN Layer = '2' AND COALESCE(OwnerId, AccountId)/*AccountId*/ <> @CustOwnerId/*@Accountid*/ AND @ShareCustViewLayer <> '1' THEN [ViewName]+'_S'+@ShareCustViewLayer
			--WHEN Layer = '2' THEN SUBSTRING([ViewName], 1, LEN([ViewName]) - 1)+@CustViewLayer
			WHEN Layer = '3' THEN LEFT([ViewName], LEN([ViewName]) - PATINDEX('%[0-9][^0-9]%', REVERSE([ViewName])))+@ShareCustViewLayer ELSE [ViewName]+'_S'+ CAST(CAST([LayerNo] AS INT) + 1 AS VARCHAR(10)) END AS ViewName
      ,@CustOwnerId--@Accountid
      ,'1'
      ,@Accountid
      ,GETDATE() 
	  FROM [iTemp].[dbo].[tmpCustView_DataAccess]
	  WHERE CustViewId = @CustViewId and Enable = '3'
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	  
	  GROUP BY [CustViewId],[ParentId],[ResId],[Layer],[LayerNo],[ViewNo],[ViewName],[AccountId],[OwnerId];

	  --[dbo].[tbCustViewSchema]
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
      ,[ModifyUser]
      ,[ModifyTime]
	  )
	  SELECT 
	   NewID()
      ,@NewID
      ,[ResSchemaId]
      ,[FieldName]
      ,[ShowField]
      ,[NickName]
      ,[Summarize]
      ,[SortKind]
      ,[SortKey]
      ,[Seq]
      ,'1'
      ,@CustOwnerId-- @Accountid
      ,GETDATE()
	  FROM [iTemp].[dbo].[tmpCustView_DataAccess]
	  WHERE CustViewId = @CustViewId and Enable = '3' /*and ShowField = '1'*/
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	  
	  GROUP BY [ResSchemaId],[FieldName],[ShowField],[NickName],[Summarize],[SortKind],[SortKey],[Seq];

	  --[dbo].[tbCustViewFilter]
	   insert into [dbo].[tbCustViewFilter](
	   [Id]
      ,[CustViewId]
      ,[ResSchemaId]
      ,[Operator]
      ,[Val]
      ,[FilterGroup]
	  ,[Enable]
	  ,[CreateUser]
      ,[ModifyUser]
      ,[ModifyTime]
	  )
	  SELECT 
	   NewID()
      ,@NewID
      ,[ResSchemaId]
      ,[Operator]
      ,[Val]
      ,[FilterGroup]
	  ,'1'
	  ,[FilterUser]
      ,@Accountid
      ,GETDATE()
	  FROM [iTemp].[dbo].[tmpCustView_DataAccess]
	  WHERE CustViewId = @CustViewId and Enable = '3' AND Val IS NOT NULL
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	  
	  GROUP BY [ResSchemaId],[Operator],[Val],[FilterGroup],[FilterUser];

	Insert INTO [iDataCenter].[dbo].[tbWksItem](
		   [Id]
		  ,[CustViewId]
		  ,[WksNodeId]
		  ,[WksItemKindId]
		  ,[StartDT]
		  ,[Effective]
		  ,[IsDistinct]
		  ,[AccountId]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT Newid()
		  ,@NewId
		  ,'24159CDB-714A-4BE9-9BBD-36863D7D2935'
		  --,'2'
		  ,CASE WHEN Layer = '1' THEN '2'
		        WHEN Layer = '2' AND AccountId = @Accountid THEN '2'
		        WHEN Layer = '2' AND AccountId <> @Accountid THEN '4'
				WHEN Layer = '3' THEN '4' END 
		  ,''
		  ,''
		  ,IsDistinct
		  ,@CustOwnerId--@AccountId
		  ,'1'
		  ,@AccountId
		  ,GetDATE()
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_DataAccess]
	  where [CustViewId] = @CustViewId
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid AND FieldName <> 'ShareDT'
	  GROUP BY CustViewId,Layer,AccountId,IsDistinct;

	  SELECT @fnLayer = dbo.fnGetiDataCenterInfo('12', @CustViewid,'1','');

	  --若有代理人資料請保留
		DECLARE @NewSaveCustviewid NVARCHAR(36);
		DECLARE @NewSaveAccountId NVARCHAR(36); 

		-- 保存 @NewId
		SET @NewSaveCustviewid = @NewId;

		-- 先查詢並取得 AccountId 保存到變數中
		SELECT TOP 1 @NewSaveAccountId = [AccountId]
		FROM [iDataCenter].[dbo].[tbCustViewAgent]
		WHERE GETDATE() BETWEEN StartTime AND EndTime 
			AND Enable = '1' 
			AND Kind = '1' 
			AND CustViewId = @custviewid;

		-- 如果有查到資料才執行 INSERT
		IF @NewSaveAccountId IS NOT NULL
		BEGIN
			-- 新增代理人資料
			INSERT INTO [iDataCenter].[dbo].[tbCustViewAgent]
			(
				[AgentId],
				[CustViewId],
				[AccountId],
				[StartTime], 
				[EndTime],
				[Kind],
				[Memo],
				[SysRemark],
				[Enable],
				[CreateUser], 
				[CreateTime], 
				[ModifyUser], 
				[ModifyTime]    
			)
			SELECT 
				NEWID(),
				@NewSaveCustviewid,
				[AccountId],
				[StartTime],
				[EndTime], 
				[Kind],
				[Memo],
				[SysRemark],
				[Enable],
				@AccountId,        -- 創建者（原本的參數）
				GETDATE(),         -- 創建時間
				@AccountId,        -- 修改者（原本的參數）
				GETDATE()          -- 更新時間
			FROM [iDataCenter].[dbo].[tbCustViewAgent]
			WHERE GETDATE() BETWEEN StartTime AND EndTime 
				AND Enable = '1' 
				AND Kind = '1' 
				AND CustViewId = @custviewid;
        
			PRINT '代理人資料已成功插入';
		END
		ELSE
		BEGIN
			PRINT '未找到符合代理人條件的資料';
		END

		--另存時一並將分享及派送拷貝一份
		/*IF (@fnLayer = '2' AND @OwnerAccountId = @Accountid) OR @fnLayer = '3'
		BEGIN	   
		    DECLARE @newPublishId NVARCHAR(36);
			DECLARE tmpPublishIDCursor CURSOR FOR
			SELECT PublishId FROM [iTemp].[dbo].[tmpCustView_Publish]
			WHERE CustViewId = @CustViewId AND Enable in ('3')
			--2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
			and ModifyUser =@Accountid	  
			GROUP BY PublishId;

			OPEN tmpPublishIDCursor;

			FETCH NEXT FROM tmpPublishIDCursor INTO @UpdatePublishId;
			
			SET @newPublishId = NEWID();

			WHILE @@FETCH_STATUS = 0
			BEGIN 
				Insert INTO [iDataCenter].[dbo].[tbPublish](
						[Id]
					  ,[CustViewId]
					  ,[FileId]
					  ,[Kind]
					  ,[Name]
					  ,[Frequency]
					  ,[Period]
					  ,[Notify]
					  ,[NotifyStatus]
					  ,[PDay]
					  ,[PTime]
					  ,[YearOffset]
					  ,[StartDT]
					  ,[EndDT]
					  ,[PublishWhere]
					  ,[Enable]
					  ,[CreateUser]
					  ,[CreateTime]
					  ,[ModifyUser]
					  ,[ModifyTime]
				)
				SELECT @newPublishId
					  ,@NewId
					  ,FileId
					  ,Kind
					  ,GroupNm
					  ,Frequency
					  ,Period
					  ,Notify
					  ,NotifyStatus
					  ,PDay
					  ,PTime
					  ,YearOffset
					  ,StartDT
					  ,EndDT
					  ,PublishWhere
					  ,'1'
					  ,@Accountid
					  ,GETDATE()
					  ,@AccountId
					  ,GetDATE()
				  FROM [iTemp].[dbo].[tmpCustView_Publish]
				  where [CustViewId] = @CustViewId and Enable = '3'
				  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
				  and ModifyUser =@Accountid	  
				  ;			

				Insert INTO [iDataCenter].[dbo].[tbPublishPeriod](
						[Id]
					  ,[PublishId]
					  ,[VariableId]
					  ,[ResSchemaId]
					  ,[ResColumnNm]
					  ,[StartYM]
					  ,[EndYM]
					  ,[Enable]
					  ,[CreateUser]
					  ,[CreateTime]
					  ,[ModifyUser]
					  ,[ModifyTime]
				)
				SELECT Newid()
					  ,@newPublishId
					  ,VariableId
					  ,ResSchemaId
					  ,ResColumnNm
					  ,StartYM
					  ,EndYM
					  ,'1'
					  ,@Accountid
					  ,GETDATE()
					  ,@AccountId
					  ,GetDATE()
				  FROM [iTemp].[dbo].[tmpCustView_Publish]
				  where [CustViewId] = @CustViewId and Enable = '3'
				  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
					and ModifyUser =@Accountid	  
					;

				Insert INTO [iDataCenter].[dbo].[tbPublishUsers](
						[Id]
					  ,[PublishId]
					  ,[AccountId]
					  ,[Kind]
					  ,[Enable]
					  ,[CreateUser]
					  ,[CreateTime]
					  ,[ModifyUser]
					  ,[ModifyTime]
				)
				SELECT Newid()
					  ,@newPublishId --這要給新的
					  ,AccountId
					  ,Kind
					  ,'1'
					  ,@Accountid
					  ,GETDATE()
					  ,@AccountId
					  ,GetDATE()
				  FROM [iTemp].[dbo].[tmpCustView_Publish]
				  where [CustViewId] = @CustViewId and Enable = '3'
				  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
				  and ModifyUser =@Accountid	  
				  ;

				FETCH NEXT FROM tmpPublishIDCursor INTO @UpdatePublishId;
			END
			-- 關閉游標
			CLOSE tmpPublishIDCursor;
			DEALLOCATE tmpPublishIDCursor;
		END*/

	END
	ELSE IF @IsEnable = '6' -- 將temp 3新增至正式但調整新ID,新layer,新layerno(分享專用)
	BEGIN
	   --DELETE 上一版(tbCustView,tbCustViewSchema,tbCustViewFilter)
	   DELETE from [dbo].[tbWksItem]
	   WHERE CustViewId = @CustViewId AND Enable in ('4','0') and len(PublishId) > 0 ;
	   DECLARE PublishIDCursor CURSOR FOR
	   SELECT id FROM [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable in ('4','0')
	   GROUP BY Id;
	   
	   OPEN PublishIDCursor;

	   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;

	   WHILE @@FETCH_STATUS = 0
	   BEGIN 
			
		   DELETE [dbo].[tbPublishPeriod]
		   where PublishId = @UpdatePublishId
			 and Enable IN ('0','4');
		   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;
	   END
		-- 關閉游標
		CLOSE PublishIDCursor;
		DEALLOCATE PublishIDCursor;

	   DELETE from [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable in ('4','0');
	   DELETE from [dbo].[tbPublishPeriod]
	   WHERE PublishId = @PublishId AND Enable in ('4','0');

	   --將原本版本改為1->4(變成上一版)
	   Update [dbo].[tbWksItem]
	   SET Enable = '4'
	   where CustViewId = @CustViewid and Len(PublishId) > 0
	     and Enable = '1';

	   DECLARE PublishIDCursor CURSOR FOR
	   SELECT id FROM [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable = '1';
	   
	   OPEN PublishIDCursor;

	   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;

	   WHILE @@FETCH_STATUS = 0
	   BEGIN 
			
		   Update [dbo].[tbPublishPeriod]
		   SET Enable = '4', ModifyTime = GETDATE()
		   where PublishId = @UpdatePublishId
			 and Enable = '1';
		   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;
	   END
		-- 關閉游標
		CLOSE PublishIDCursor;
		DEALLOCATE PublishIDCursor;
	    SELECT @PublishId = Id 
		FROM [iTemp].[dbo].[tmpPublish] 	   
		WHERE CustViewId = @CustViewid AND Enable = '3'
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	    and ModifyUser =@Accountid	  
		group by Id;

	   Update [dbo].[tbPublish]
	   SET Enable = '4'
	   WHERE CustViewId = @CustViewId AND Enable = '1';



	Insert INTO [iDataCenter].[dbo].[tbPublish](
			[Id]
		  ,[CustViewId]
		  ,[FileId]
		  ,[Kind]
		  ,[Name]
		  ,[Frequency]
		  ,[Period]
		  ,[Notify]
		  ,[NotifyStatus]
		  ,[PDay]
		  ,[PTime]
		  ,[StartDT]
		  ,[EndDT]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT distinct PublishId--@PublishId
		  ,CustViewid
		  ,FileId
		  ,Kind
		  ,GroupNm
		  ,Frequency
		  ,Period
		  ,Notify
		  ,NotifyStatus
		  ,PDay
		  ,PTime
		  ,StartDT
		  ,EndDT
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where [CustViewId] = @CustViewId and Enable = '3' and PublishId is not null
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid	  ;

	Insert INTO [iDataCenter].[dbo].[tbPublishPeriod](
			[Id]
		  ,[PublishId]
		  ,[VariableId]
		  ,[ResSchemaId]
		  ,[ResColumnNm]
		  ,[StartYM]
		  ,[EndYM]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT NewId()--Newid()
		  ,PublishId--@PublishId
		  ,VariableId
		  ,ResSchemaId
		  ,ResColumnNm
		  ,StartYM
		  ,EndYM
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where CustViewId= @CustViewId and Enable = '3' 
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid	  
	  GROUP BY PublishId, VariableId, ResSchemaId, ResColumnNm, StartYM, EndYM, CreateUser, CreateTime;

	Insert INTO [iDataCenter].[dbo].[tbWksItem](
		   [Id]
		  ,[CustViewId]
		  ,[WksNodeId]
		  ,[WksItemKindId]
		  ,[PublishId]
		  --,[StartDT]
		  ,[IsDistinct]
		  ,[Effective]
		  ,[AccountId]
		  ,[Ememo]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT newId()--Newid()
		  ,CustViewId
		  ,'24159CDB-714A-4BE9-9BBD-36863D7D2935'
		  --,[WksItemKindId]
		  ,'3'
		  ,[PublishId]
		  --,[StartDT]
		  ,[IsDistinct]
		  ,[Effective]
		  ,[AccountId]
		  ,[Ememo]
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where [CustViewId] = @CustViewId and Enable = '3' and len(PublishId)>0
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid	  
	  GROUP BY CustViewId, WksItemKindId, PublishId, IsDistinct, Effective, AccountId, Ememo, CreateUser, CreateTime;

	  exec [dbo].[UpdatetbPublishWhere] @CustViewId = @CustViewid;
	END
	ELSE IF @IsEnable = '61' --分享刪除
	BEGIN
		DECLARE tmpPublishCursor CURSOR FOR
		SELECT CustViewId, PublishId FROM [iTemp].[dbo].[tmpCustView_Publish]
		WHERE PublishId = @CustViewid AND Enable in ('3')
		GROUP BY CustViewId, PublishId;

		Print 'CustViewId:'+@CustViewId;
	   
		OPEN tmpPublishCursor;

		FETCH NEXT FROM tmpPublishCursor INTO @UpdateCustViewId, @UpdatePublishId;

		WHILE @@FETCH_STATUS = 0
		BEGIN 
				print 'AccountId:'+@AccountId;
				print 'UpdateCustViewId:'+@UpdateCustViewId;
				print 'UpdatePublishId:'+@UpdatePublishId;
			DELETE from [dbo].[tbPublish]
			where CustViewId = @CustViewid AND Id = @UpdatePublishId
			AND Enable in ('4');

			DELETE from [dbo].[tbPublishPeriod]
			where PublishId = @UpdatePublishId
			AND Enable in ('4');

			DELETE from [dbo].[tbWksItem]
			where PublishId = @UpdatePublishId
			AND Enable in ('4');

			Update [dbo].[tbPublish]
			SET Enable = '4', ModifyTime = GETDATE()
			where CustViewId = @UpdateCustViewId AND Id = @UpdatePublishId
				and Enable IN ('1');
			
			Update [dbo].[tbPublishPeriod]
			SET Enable = '4', ModifyTime = GETDATE()
			where PublishId = @UpdatePublishId 
				and Enable IN ('1');

			Update [dbo].[tbWksItem]
			SET Enable = '4', ModifyTime = GETDATE()
			where PublishId = @UpdatePublishId 
				and Enable IN ('1');

			exec [dbo].[GetSharedCustViewData] @inCustViewId = @UpdateCustViewId, @inPublishId = @UpdatePublishId, @inIsEnable = @IsEnable, @inAccountId = @Accountid;

			FETCH NEXT FROM tmpPublishCursor INTO @UpdateCustViewId, @UpdatePublishId;
		END
		-- 關閉游標
		CLOSE tmpPublishCursor;
		DEALLOCATE tmpPublishCursor;	

		--需另外處理(待改)
		/*UPDATE [iDataCenter].[dbo].[tbPublishPeriod]
		SET Enable = '4'
		where PublishId = @CustViewid
		AND Enable = '1';*/
		UPDATE [iDataCenter].[dbo].[tbWksItem]
		SET Enable = '4'
		where CustViewId = @CustViewid AND Enable = '1';				
	END
	ELSE IF @IsEnable = '62' --大批分享查詢
	BEGIN
	   DELETE from [iTemp].[dbo].[tmpCustView_Publish]
	   where OwnerId = @Accountid
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid	
	   AND Enable in ('62','0');

	   INSERT [iTemp].[dbo].[tmpCustView_Publish] 
	   SELECT [Kind]
      ,[OwnerId]
	  ,[CustViewOwnerId]
      ,[OwnerEmpNo]
      ,[OwnerEmpName]
      ,[CustViewId]
      ,[ParentId]
      ,[MasterId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewName]
      ,[ViewNo]
      ,[PublishId]
      ,[PublishPeriodId]
      ,[wksItemId]
      ,[publishUsersId]
      ,[GroupNm]
      ,[Frequency]
      ,[NotifyStatus]
      ,[StartDT]
      ,[EndDT]
	  ,0--20250214調整
      ,[ResSchemaId]
      ,[ResColumnNm]
      ,[PDay]
      ,[PTime]
      ,[Notify]
      ,[Period]
      ,[VariableId]
      ,[StartYM]
      ,[EndYM]
      ,[AccountId]
      ,[AccountKind]
      ,[EmpNo]
      ,[EmpName]
      ,[EmpEmail]
      ,[DeptCode]
      ,[DeptName]
	  ,[ReviewUnit]
      ,[FirstNickNm]
      ,[SecontNickNm]
      ,[FileId]
      ,[YearOffset]
      ,[Effective]
	  ,[Delivery]
      ,[Ememo]
      ,[WksNodeId]
      ,[WksItemKindId]
      ,[PublishWhere]
      ,'62'
      --,[ModifyUser]
	  ,@Accountid as [ModifyUser]
      ,GETDATE()
      ,[CreateUser]
      ,[CreateTime]
	   FROM iDataCenter.dbo.vCustView_Publish
	   WHERE enable = '1' and OwnerId = @Accountid and Kind = '1';

	END
	ELSE IF @IsEnable = '63' --大批分享查詢
	BEGIN
	   DECLARE tmpPublishCursor CURSOR FOR
	   SELECT CustViewId, PublishId FROM [iTemp].[dbo].[tmpCustView_Publish]
	   WHERE OwnerId = @Accountid AND Enable in ('62')
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid
	   GROUP BY CustViewId, PublishId;
	   
	   OPEN tmpPublishCursor;

	   FETCH NEXT FROM tmpPublishCursor INTO @UpdateCustViewId, @UpdatePublishId;

	   WHILE @@FETCH_STATUS = 0
	   BEGIN 
			   print 'AccountId:'+@AccountId;
			   print 'UpdateCustViewId:'+@UpdateCustViewId;
			   print 'UpdatePublishId:'+@UpdatePublishId;

			Update [dbo].[tbPublish]
			SET Enable = '4', ModifyTime = GETDATE()
			where CustViewId = @UpdateCustViewId AND Id = @UpdatePublishId
				and Enable IN ('1');
			
			Update [dbo].[tbPublishPeriod]
			SET Enable = '4', ModifyTime = GETDATE()
			where PublishId = @UpdatePublishId 
				and Enable IN ('1');
			
			INSERT 	[dbo].[tbPublish]
			SELECT 
				PublishId
			  ,[CustViewId]
			  ,[FileId]
			  ,[Kind]
			  ,GroupNm
			  ,[Frequency]
			  ,[Period]
			  ,[Notify]
			  ,[NotifyStatus]
			  ,[PDay]
			  ,[PTime]
			  ,[YearOffset]
			  ,[StartDT]
			  ,[EndDT]
			  ,[PublishWhere]
			  ,'1'
			  ,[CreateUser]
			  ,[CreateTime]
			  ,@Accountid
			  ,GetDATE()
			from iTemp.dbo.tmpCustView_Publish
			WHERE CustViewId = @UpdateCustViewId and PublishId = @UpdatePublishId and Enable = '62'
		    --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
			and ModifyUser =@Accountid
			GROUP BY PublishId, CustViewId, FileId, Kind, GroupNm, Frequency, Period, Notify, NotifyStatus, PDay, PTime, YearOffset, StartDT, EndDT, PublishWhere, CreateUser, CreateTime;
			INSERT [dbo].[tbPublishPeriod]
			SELECT 
				Newid()
			  ,[PublishId]
			  ,[VariableId]
			  ,[ResSchemaId]
			  ,[ResColumnNm]
			  ,[StartYM]
			  ,[EndYM]
			  ,'1'
			  ,[CreateUser]
			  ,[CreateTime]
			  ,@Accountid
			  ,GETDATE()
			from iTemp.dbo.tmpCustView_Publish
			WHERE PublishId = @UpdatePublishId and Enable = '62'
		    --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
			and ModifyUser =@Accountid
			GROUP BY PublishId, VariableId, ResSchemaId, ResColumnNm, StartYM, EndYM, CreateUser, CreateTime;

			FETCH NEXT FROM tmpPublishCursor INTO @UpdateCustViewId, @UpdatePublishId;
	   END
		-- 關閉游標
		CLOSE tmpPublishCursor;
		DEALLOCATE tmpPublishCursor;	   
	   /*DELETE from [iTemp].[dbo].[tmpCustView_Publish]
	   where OwnerId = @Accountid
	   AND Enable in ('62','0');

	   INSERT [iTemp].[dbo].[tmpCustView_Publish] 
	   SELECT [Kind]
      ,[OwnerId]
      ,[OwnerEmpNo]
      ,[OwnerEmpName]
      ,[CustViewId]
      ,[ParentId]
      ,[MasterId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewName]
      ,[ViewNo]
      ,[PublishId]
      ,[PublishPeriodId]
      ,[wksItemId]
      ,[publishUsersId]
      ,[GroupNm]
      ,[Frequency]
      ,[NotifyStatus]
      ,[StartDT]
      ,[EndDT]
      ,[ResSchemaId]
      ,[ResColumnNm]
      ,[PDay]
      ,[PTime]
      ,[Notify]
      ,[Period]
      ,[VariableId]
      ,[StartYM]
      ,[EndYM]
      ,[AccountId]
      ,[AccountKind]
      ,[EmpNo]
      ,[EmpName]
      ,[EmpEmail]
      ,[DeptCode]
      ,[DeptName]
      ,[FirstNickNm]
      ,[SecontNickNm]
      ,[FileId]
      ,[YearOffset]
      ,[Effective]
      ,[Ememo]
      ,[WksNodeId]
      ,[WksItemKindId]
      ,[PublishWhere]
      ,'62'
      ,[ModifyUser]
      ,[ModifyTime]
      ,[CreateUser]
      ,[CreateTime]
	   FROM iDataCenter.dbo.vCustView_Publish
	   WHERE enable = '1' and OwnerId = @Accountid;*/
	   --Insert iDataCenter.dbo.tbpublish
	   Print 'Enable:63';
	END
	ELSE IF @IsEnable = '7' -- 將temp 3新增至正式但調整新ID,新layer,新layerno(派送專用)
	BEGIN
	   --DELETE 上一版(tbCustView,tbCustViewSchema,tbCustViewFilter)
	   DECLARE PublishIDCursor CURSOR FOR
	   SELECT id FROM [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable in ('4','0')
	   GROUP BY Id;
	   
	   OPEN PublishIDCursor;

	   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;

	   WHILE @@FETCH_STATUS = 0
	   BEGIN 
		   DELETE from [dbo].[tbPublishUsers]
		   WHERE PublishId = @UpdatePublishId AND Enable in ('4','0') and len(PublishId) > 0 ;			

		   DELETE [dbo].[tbPublishPeriod]
		   where PublishId = @UpdatePublishId
			 and Enable IN ('0','4');
		   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;
	   END
		-- 關閉游標
		CLOSE PublishIDCursor;
		DEALLOCATE PublishIDCursor;

	   DELETE from [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable in ('4','0');

	   --將原本版本改為1->4(變成上一版)

	   DECLARE PublishIDCursor CURSOR FOR
	   SELECT id FROM [dbo].[tbPublish]
	   WHERE CustViewId = @CustViewId AND Enable = '1';
	   
	   OPEN PublishIDCursor;

	   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;

	   WHILE @@FETCH_STATUS = 0
	   BEGIN 
		   Update [dbo].[tbPublishPeriod]
		   SET Enable = '4', ModifyTime = GETDATE()
		   where PublishId = @UpdatePublishId
			 and Enable = '1';

		   Update [dbo].[tbPublishUsers]
		   SET Enable = '4', ModifyTime = GETDATE()
		   where PublishId = @UpdatePublishId
			 and Enable = '1';
		   FETCH NEXT FROM PublishIDCursor INTO @UpdatePublishId;
	   END
		-- 關閉游標
		CLOSE PublishIDCursor;
		DEALLOCATE PublishIDCursor;

	   Update [dbo].[tbPublish]
	   SET Enable = '4'
	   WHERE CustViewId = @CustViewId AND Enable = '1';


	    print 'enable:7'

	Insert INTO [iDataCenter].[dbo].[tbPublish](
			[Id]
		  ,[CustViewId]
		  ,[FileId]
		  ,[Kind]
		  ,[Name]
		  ,[Frequency]
		  ,[Period]
		  ,[Notify]
		  ,[NotifyStatus]
		  ,[PDay]
		  ,[PTime]
		  ,[StartDT]
		  ,[EndDT]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT distinct PublishId--@PublishId
		  ,CustViewid
		  ,FileId
		  ,Kind
		  ,GroupNm
		  ,Frequency
		  ,Period
		  ,Notify
		  ,NotifyStatus
		  ,PDay
		  ,PTime
		  ,StartDT
		  ,EndDT
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where [CustViewId] = @CustViewId and Enable = '3' and PublishId is not null
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid
	  ;
	Insert INTO [iDataCenter].[dbo].[tbPublishPeriod](
			[Id]
		  ,[PublishId]
		  ,[VariableId]
		  ,[ResSchemaId]
		  ,[ResColumnNm]
		  ,[StartYM]
		  ,[EndYM]
		  ,[Enable]
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
	)
	SELECT NewId()--Newid()
		  ,PublishId--@PublishId
		  ,VariableId
		  ,ResSchemaId
		  ,ResColumnNm
		  ,StartYM
		  ,EndYM
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,@AccountId
		  ,GetDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where CustViewId= @CustViewId and Enable = '3' 
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid
	  GROUP BY PublishId, VariableId, ResSchemaId, ResColumnNm, StartYM, EndYM, CreateUser, CreateTime;

	Insert INTO [iDataCenter].[dbo].[tbPublishUsers](
		[Id]
      ,[PublishId]
      ,[AccountId]
      ,[Kind]
      --,[OTP]
	  ,[Delivery]
      ,[Enable]
      ,[CreateUser]
      ,[CreateTime]
      ,[ModifyUser]
      ,[ModifyTime]
	)
	SELECT newid()
      ,[PublishId]
      ,[AccountId]
      ,[Kind]
      --,[OTP]
	  ,[Delivery]
      ,'1'
      ,[CreateUser]
      ,[CreateTime]
      ,@Accountid
      ,GETDATE()
	  FROM [iTemp].[dbo].[tmpCustView_Publish]
	  where [CustViewId] = @CustViewId and Enable = '3' and len(PublishId)>0
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  and ModifyUser =@Accountid
	  GROUP BY PublishId, AccountId, Kind, Delivery, Enable, CreateUser, CreateTime;

	  exec [dbo].[UpdatetbPublishWhere] @CustViewId = @CustViewid;
	END
	ELSE IF @IsEnable = '71' --派送刪除
	BEGIN
		Print 'Enable : 71';
		DECLARE tmpPublishCursor CURSOR FOR
		SELECT CustViewId, PublishId FROM [iTemp].[dbo].[tmpCustView_Publish]
		WHERE PublishId = @CustViewid AND Enable in ('3')
		--2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
		and ModifyUser =@Accountid
		GROUP BY CustViewId, PublishId;
	   
		OPEN tmpPublishCursor;

		FETCH NEXT FROM tmpPublishCursor INTO @UpdateCustViewId, @UpdatePublishId;

		WHILE @@FETCH_STATUS = 0
		BEGIN 
				print 'AccountId:'+@AccountId;
				print 'UpdateCustViewId:'+@UpdateCustViewId;
				print 'UpdatePublishId:'+@UpdatePublishId;
			DELETE from [dbo].[tbPublish]
			where CustViewId = @CustViewid AND Id = @UpdatePublishId
			AND Enable in ('4');

			DELETE from [dbo].[tbPublishPeriod]
			where PublishId = @UpdatePublishId
			AND Enable in ('4');

			DELETE from [dbo].[tbPublishUsers]
			where PublishId = @UpdatePublishId
			AND Enable in ('4');

			Update [dbo].[tbPublish]
			SET Enable = '4', ModifyTime = GETDATE()
			where CustViewId = @UpdateCustViewId AND Id = @UpdatePublishId
				and Enable IN ('1');
			
			Update [dbo].[tbPublishPeriod]
			SET Enable = '4', ModifyTime = GETDATE()
			where PublishId = @UpdatePublishId 
				and Enable IN ('1');

			Update [dbo].[tbPublishUsers]
			SET Enable = '4', ModifyTime = GETDATE()
			where PublishId = @UpdatePublishId 
				and Enable IN ('1');

			--exec [dbo].[GetSharedCustViewData] @inCustViewId = @UpdateCustViewId, @inPublishId = @UpdatePublishId, @inIsEnable = @IsEnable;

			FETCH NEXT FROM tmpPublishCursor INTO @UpdateCustViewId, @UpdatePublishId;
		END
		-- 關閉游標
		CLOSE tmpPublishCursor;
		DEALLOCATE tmpPublishCursor;	

		--需另外處理(待改)
		/*UPDATE [iDataCenter].[dbo].[tbPublishPeriod]
		SET Enable = '4'
		where PublishId = @CustViewid
		AND Enable = '1';*/
	END
	ELSE IF @IsEnable = '72' --大批分享查詢
	BEGIN
	   DELETE from [iTemp].[dbo].[tmpCustView_Publish]
	   where OwnerId = @Accountid
	   --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid
	   AND Enable in ('72','0');

	   INSERT [iTemp].[dbo].[tmpCustView_Publish] 
	   SELECT [Kind]
      ,[OwnerId]
	  ,[CustViewOwnerId]
      ,[OwnerEmpNo]
      ,[OwnerEmpName]
      ,[CustViewId]
      ,[ParentId]
      ,[MasterId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewName]
      ,[ViewNo]
      ,[PublishId]
      ,[PublishPeriodId]
      ,[wksItemId]
      ,[publishUsersId]
      ,[GroupNm]
      ,[Frequency]
      ,[NotifyStatus]
      ,[StartDT]
      ,[EndDT]
	  ,0--20250214調整
      ,[ResSchemaId]
      ,[ResColumnNm]
      ,[PDay]
      ,[PTime]
      ,[Notify]
      ,[Period]
      ,[VariableId]
      ,[StartYM]
      ,[EndYM]
      ,[AccountId]
      ,[AccountKind]
      ,[EmpNo]
      ,[EmpName]
      ,[EmpEmail]
      ,[DeptCode]
      ,[DeptName]
	  ,[ReviewUnit]
      ,[FirstNickNm]
      ,[SecontNickNm]
      ,[FileId]
      ,[YearOffset]
      ,[Effective]
	  ,[Delivery]
      ,[Ememo]
      ,[WksNodeId]
      ,[WksItemKindId]
      ,[PublishWhere]
      ,'72'
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
      --,[ModifyUser]
	  ,@Accountid as [ModifyUser]
      ,GETDATE()
      ,[CreateUser]
      ,[CreateTime]
	   FROM iDataCenter.dbo.vCustView_Publish
	   WHERE enable = '1' and OwnerId = @Accountid and Kind = '2';


	END
	ELSE IF @IsEnable = '73' --大批分享查詢
	BEGIN
	   DECLARE tmpPublishCursor CURSOR FOR
	   SELECT CustViewId, PublishId FROM [iTemp].[dbo].[tmpCustView_Publish]
	   WHERE OwnerId = @Accountid AND Enable in ('72')
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	   and ModifyUser =@Accountid
	   GROUP BY CustViewId, PublishId;
	   
	   OPEN tmpPublishCursor;

	   FETCH NEXT FROM tmpPublishCursor INTO @UpdateCustViewId, @UpdatePublishId;

	   WHILE @@FETCH_STATUS = 0
	   BEGIN 
			   print 'AccountId:'+@AccountId;
			   print 'UpdateCustViewId:'+@UpdateCustViewId;
			   print 'UpdatePublishId:'+@UpdatePublishId;
			Update [dbo].[tbPublish]
			SET Enable = '4', ModifyTime = GETDATE()
			where CustViewId = @UpdateCustViewId AND Id = @UpdatePublishId
				and Enable IN ('1');

			Update [dbo].[tbPublishPeriod]
			SET Enable = '4', ModifyTime = GETDATE()
			where PublishId = @UpdatePublishId 
				and Enable IN ('1');

			Update [dbo].[tbPublishUsers]
			SET Enable = '4', ModifyTime = GETDATE()
			where PublishId = @UpdatePublishId 
				and Enable IN ('1');
			
			INSERT 	[dbo].[tbPublish]
			SELECT 
				PublishId
			  ,[CustViewId]
			  ,[FileId]
			  ,[Kind]
			  ,GroupNm
			  ,[Frequency]
			  ,[Period]
			  ,[Notify]
			  ,[NotifyStatus]
			  ,[PDay]
			  ,[PTime]
			  ,[YearOffset]
			  ,[StartDT]
			  ,[EndDT]
			  ,[PublishWhere]
			  ,'1'
			  ,[CreateUser]
			  ,[CreateTime]
			  ,@Accountid
			  ,GetDATE()
			from iTemp.dbo.tmpCustView_Publish
			WHERE CustViewId = @UpdateCustViewId and PublishId = @UpdatePublishId and Enable = '72' and Kind = '2'
		    --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	        and ModifyUser =@Accountid
			GROUP BY PublishId, CustViewId, FileId, Kind, GroupNm, Frequency, Period, Notify, NotifyStatus, PDay, PTime, YearOffset, StartDT, EndDT, PublishWhere, CreateUser, CreateTime;

			INSERT [dbo].[tbPublishPeriod]
			SELECT 
				Newid()
			  ,[PublishId]
			  ,[VariableId]
			  ,[ResSchemaId]
			  ,[ResColumnNm]
			  ,[StartYM]
			  ,[EndYM]
			  ,'1'
			  ,[CreateUser]
			  ,[CreateTime]
			  ,@Accountid
			  ,GETDATE()
			from iTemp.dbo.tmpCustView_Publish
			WHERE PublishId = @UpdatePublishId and Enable = '72' and Kind = '2'
			--2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	        and ModifyUser =@Accountid
			GROUP BY PublishId, VariableId, ResSchemaId, ResColumnNm, StartYM, EndYM, CreateUser, CreateTime;

			INSERT [dbo].[tbPublishUsers](
				Id
			  ,[PublishId]
			  ,[AccountId]
			  ,[Kind]
			  --,[OTP]
			  ,[Delivery]
			  ,[Enable]
			  ,[CreateUser]
			  ,[CreateTime]
			  ,[ModifyUser]
			  ,[ModifyTime]
			)
			SELECT 
			   NewId()
			  ,[PublishId]
			  ,[AccountId]
			  ,[Kind]
			  --,[OTP]
			  ,[Delivery]
			  ,'1'
			  ,[CreateUser]
			  ,[CreateTime]
			  ,@Accountid
			  ,GETDATE()
			from iTemp.dbo.tmpCustView_Publish
			WHERE PublishId = @UpdatePublishId and Enable = '72'
			--2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	        and ModifyUser =@Accountid
			GROUP BY PublishId, AccountId, Kind, Delivery, CreateUser, CreateTime;

			FETCH NEXT FROM tmpPublishCursor INTO @UpdateCustViewId, @UpdatePublishId;
	   END
		-- 關閉游標
		CLOSE tmpPublishCursor;
		DEALLOCATE tmpPublishCursor;	   
	   /*DELETE from [iTemp].[dbo].[tmpCustView_Publish]
	   where OwnerId = @Accountid
	   AND Enable in ('62','0');

	   INSERT [iTemp].[dbo].[tmpCustView_Publish] 
	   SELECT [Kind]
      ,[OwnerId]
      ,[OwnerEmpNo]
      ,[OwnerEmpName]
      ,[CustViewId]
      ,[ParentId]
      ,[MasterId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewName]
      ,[ViewNo]
      ,[PublishId]
      ,[PublishPeriodId]
      ,[wksItemId]
      ,[publishUsersId]
      ,[GroupNm]
      ,[Frequency]
      ,[NotifyStatus]
      ,[StartDT]
      ,[EndDT]
      ,[ResSchemaId]
      ,[ResColumnNm]
      ,[PDay]
      ,[PTime]
      ,[Notify]
      ,[Period]
      ,[VariableId]
      ,[StartYM]
      ,[EndYM]
      ,[AccountId]
      ,[AccountKind]
      ,[EmpNo]
      ,[EmpName]
      ,[EmpEmail]
      ,[DeptCode]
      ,[DeptName]
      ,[FirstNickNm]
      ,[SecontNickNm]
      ,[FileId]
      ,[YearOffset]
      ,[Effective]
      ,[Ememo]
      ,[WksNodeId]
      ,[WksItemKindId]
      ,[PublishWhere]
      ,'62'
      ,[ModifyUser]
      ,[ModifyTime]
      ,[CreateUser]
      ,[CreateTime]
	   FROM iDataCenter.dbo.vCustView_Publish
	   WHERE enable = '1' and OwnerId = @Accountid;*/
	   --Insert iDataCenter.dbo.tbpublish
	   Print 'Enable:73';
	END
	ELSE IF @IsEnable = '74' --派送作業
	BEGIN
	   Print 'Enable:74';
	   DELETE from [iTemp].[dbo].[tmpCustView_Publish]
	   where OwnerId = @Accountid
		--2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	    and ModifyUser =@Accountid
	   AND Enable in ('74','0');

	   INSERT [iTemp].[dbo].[tmpCustView_Publish] 
	   SELECT [Kind]
      ,[OwnerId]
	  --,CustViewOwnerId
      ,[OwnerEmpNo]
      ,[OwnerEmpName]
      ,[CustViewId]
      ,[ParentId]
      ,[MasterId]
      ,[Layer]
      ,[LayerNo]
      ,[ViewName]
      ,[ViewNo]
      ,[PublishId]
      ,[PublishPeriodId]
      ,[wksItemId]
      ,[publishUsersId]
      ,[GroupNm]
      ,[Frequency]
      ,[NotifyStatus]
      ,[StartDT]
      ,[EndDT]
	  ,0--20250214調整
      ,[ResSchemaId]
      ,[ResColumnNm]
      ,[PDay]
      ,[PTime]
      ,[Notify]
      ,[Period]
      ,[VariableId]
      ,[StartYM]
      ,[EndYM]
      ,[AccountId]
      ,[AccountKind]
      ,[EmpNo]
      ,[EmpName]
      ,[EmpEmail]
      ,[DeptCode]
      ,[DeptName]
	  ,[ReviewUnit]
      ,[FirstNickNm]
      ,[SecontNickNm]
      ,[FileId]
      ,[YearOffset]
      ,[Effective]
	  ,[Delivery]
      ,[Ememo]
      ,[WksNodeId]
      ,[WksItemKindId]
      ,[PublishWhere]
      ,'74'
	  --2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
	  --,[ModifyUser]
      ,@Accountid as [ModifyUser]
      ,GETDATE()
      ,[CreateUser]
      ,[CreateTime]
	  ,CustViewOwnerId
	   FROM iDataCenter.dbo.vCustView_Publish
	   WHERE enable = '1' and CustViewId = @CustViewid and Kind = '2';
	   Print 'Enable:74 End';
	   
	END

    -- 根據 @IsEnable 決定使用的表格名稱
    SET @SelectTable = CASE @IsEnable
        WHEN '1' THEN '[iTemp].[dbo].[tmpCustView_DataAccess]'
        WHEN '2' THEN '[iTemp].[dbo].[tmpCustView_DataAccess]'
        WHEN '3' THEN '[iTemp].[dbo].[tmpCustView_DataAccess]'
        WHEN '4' THEN '[dbo].[vCustView]'    
        WHEN '5' THEN '[iTemp].[dbo].[tmpCustView_DataAccess]'     
		WHEN '6' THEN '[iTemp].[dbo].[tmpCustView_DataAccess]'    
		WHEN '7' THEN '[iTemp].[dbo].[tmpCustView_DataAccess]'    
		WHEN '74' THEN '[dbo].[vCustView_DataAccess]'    
		WHEN '8' THEN '[iTemp].[dbo].[tmpCustView_DataAccess]'  -- 匯出使用
        WHEN '9' THEN '[dbo].[tbCustViewSchema]'     
        ELSE '[dbo].[tbCustViewSchema]'
    END;
	--2025/01/22 Weiping 新增
    SET @WhereModifyUser = CASE @IsEnable
        WHEN '1' THEN ' and ModifyUser = '+ QUOTENAME(@Accountid, '''') + ' '
        WHEN '2' THEN ' and ModifyUser = '+ QUOTENAME(@Accountid, '''') + ' '
        WHEN '3' THEN ' and ModifyUser = '+ QUOTENAME(@Accountid, '''') + ' '
        WHEN '4' THEN '  '    
        WHEN '5' THEN ' and ModifyUser = '+ QUOTENAME(@Accountid, '''') + ' '
		WHEN '6' THEN ' and ModifyUser = '+ QUOTENAME(@Accountid, '''') + ' '
		WHEN '7' THEN ' and ModifyUser = '+ QUOTENAME(@Accountid, '''') + ' '
		WHEN '74' THEN '  '   
		WHEN '8' THEN ' and ModifyUser = '+ QUOTENAME(@Accountid, '''') + ' '  -- 匯出使用
        WHEN '9' THEN '  '   
        ELSE '  '   
    END;
	print '@WhereModifyUser:'+@WhereModifyUser
	--執行Enable的情境
    SET @RunEnable = CASE @IsEnable
        WHEN '1' THEN '3'
		WHEN '11' THEN '1'
        WHEN '2' THEN '3'
        WHEN '3' THEN '3'
        WHEN '4' THEN '[dbo].[vCustView]'    
        WHEN '5' THEN '3'     
		WHEN '6' THEN '3'
		WHEN '61' THEN '1'
		WHEN '62' THEN '1'
		WHEN '63' THEN '1'
		WHEN '7' THEN '3'
		WHEN '72' THEN '1'
		WHEN '73' THEN '1'
		WHEN '74' THEN '1'
		WHEN '8' THEN '3'
        WHEN '9' THEN '[dbo].[tbCustViewSchema]'     
        ELSE '[dbo].[tbCustViewSchema]'
    END;

	IF @IsEnable not in ('62','63','72','73','11','61','71')
	BEGIN
		-- 使用動態 SQL 來獲取 DBName
			SET @SQL = N'
			SELECT @DBNameOut = DbName, @LayerOut = Layer, @LayerNoOut = LayerNo
			FROM ' + @SelectTable + '
			WHERE ViewName = @ViewNameParam
			'+ @WhereModifyUser +'
			GROUP BY DBNAME, Layer, LayerNo';

			PRINT 'SQL:' + @SQL;

		DECLARE @ParmDefinition NVARCHAR(500) = N'@ViewNameParam VARCHAR(100), @DBNameOut VARCHAR(10) OUTPUT, @LayerOut VARCHAR(10) OUTPUT, @LayerNoOut VARCHAR(10) OUTPUT';
		EXEC sp_executesql @SQL, @ParmDefinition, 
			@ViewNameParam = @ViewName,
			@DBNameOut = @DBName OUTPUT,
			@LayerOut = @Layer OUTPUT,
			@LayerNoOut = @LayerNo OUTPUT

			print 'CustViewid:'+@CustViewid;

			SELECT @DBName = dbo.fnGetiDataCenterInfo('9', @CustViewid,'1','');


		PRINT 'SelectTable:' + @SelectTable;
		PRINT 'ViewName:'+@ViewName;
		PRINT 'DBName:'+@DBName;

		--select @DBName = DbName from vCustView_DataAccess where ViewName = @ViewName GROUP BY DBNAME;


		BEGIN TRY
		    print 'select start';
			-- Get Select and Group Fields
			/*SELECT @SelectFields = STRING_AGG(CONVERT(varchar(20), CASE WHEN Summarize = 1 THEN fieldname
																		WHEN Summarize = 2 THEN 'SUM('+FieldName+')' 
																		WHEN Summarize = 3 THEN 'AVG('+FieldName+')' 
																		WHEN Summarize = 4 THEN 'MIN('+FieldName+')'
																		WHEN Summarize = 5 THEN 'MAX('+FieldName+')' 
																		WHEN Summarize = 6 THEN 'COUNT('+FieldName+')' END), ',') WITHIN GROUP (ORDER BY seq),
				   @GroupFields = STRING_AGG(CONVERT(varchar(20), 
										CASE WHEN Summarize = 1 THEN FieldName END), ',') WITHIN GROUP (ORDER BY seq)
			FROM vCustView_DataAccess
			WHERE ViewName = @ViewName and Enable = @IsEnable
			AND ShowField = '1';*/
			-- Get Select and Group Fields
			SET @SQL = N'
			SELECT @SelectFieldsOut = STRING_AGG(CONVERT(varchar(40), 
				CASE 
					WHEN ShowField = 1 AND Summarize = 0 THEN FieldName
					WHEN Summarize = 1 THEN fieldname
					WHEN Summarize = 2 THEN ''SUM(''+FieldName+'') AS ''+ FieldName 
					WHEN Summarize = 3 THEN ''AVG(''+FieldName+'') AS ''+ FieldName
					WHEN Summarize = 4 THEN ''MIN(''+FieldName+'') AS ''+ FieldName
					WHEN Summarize = 5 THEN ''MAX(''+FieldName+'') AS ''+ FieldName
					WHEN Summarize = 6 THEN ''COUNT(''+FieldName+'') AS ''+ FieldName
				END), '','') WITHIN GROUP (ORDER BY seq),
				@GroupFieldsOut = STRING_AGG(CONVERT(varchar(40), 
					CASE WHEN Summarize = 1 THEN FieldName END), '','') 
					WITHIN GROUP (ORDER BY seq)
			FROM ' + @SelectTable + '
			WHERE ViewName = @ViewNameParam 
			AND Enable = @IsEnableParam
			'+ @WhereModifyUser +'
			AND ShowField = ''1''';

			SET @ParmDefinition = N'@ViewNameParam VARCHAR(100), @IsEnableParam CHAR(1), 
				@SelectFieldsOut VARCHAR(MAX) OUTPUT, @GroupFieldsOut VARCHAR(MAX) OUTPUT';
			print 'SelectFields_(1):'+@SelectFields;
			print 'SelectFields_(2)';
			EXEC sp_executesql @SQL, @ParmDefinition,
				@ViewNameParam = @ViewName,
				@IsEnableParam = @RunEnable,
				@SelectFieldsOut = @SelectFields OUTPUT,
				@GroupFieldsOut = @GroupFields OUTPUT;

			print 'SelectFields_(3):'+@SelectFields;
			
			-- Get Where Fields
		
			if @IsEnable = 1 
			BEGIN
				SELECT @WhereFields = STRING_AGG(A.where_Field, ' or ')
				FROM (
					SELECT '('+STRING_AGG(CONVERT(varchar(100), A.fieldname+ CASE WHEN A.Operator = 1 THEN '=''' 
																				WHEN A.Operator = 2 THEN '<''' 
																				WHEN A.Operator = 3 THEN '>'''
																				WHEN A.Operator = 4 THEN '<='''
																				WHEN A.Operator = 5 THEN '>='''
																				WHEN A.Operator = 6 THEN '<>'''
																				 WHEN A.Operator = 7 THEN ' Like '''
																					 WHEN A.Operator = 8 THEN ' Not Like '''
																					 WHEN A.Operator = 9 THEN ' is null'
																					 WHEN A.Operator = 10 THEN ' is not null' END +
																				CASE WHEN A.Operator IN (7,8) THEN '%' + A.Val + '%'
																					 WHEN A.Operator IN (9,10) THEN ''
																					 ELSE CASE WHEN Left(A.Val,1) = '@' THEN B.Var_DT_Filter_Format_Start ELSE A.Val END END + 
																				CASE WHEN A.Operator IN (1,2,3,4,5,6,7,8) THEN ''''
																					 ELSE '' END
																			), ' and ') WITHIN GROUP (ORDER BY A.seq)+')' AS where_Field
					FROM [iTemp].[dbo].[tmpCustView_DataAccess] A
					LEFT JOIN
					(    select Var_DT_Filter_Format_Start, Var_DT_Filter_Format_End, Var_DT_Nm, Var_DT_Desc, Var_DT_Open, Enable
					  FROM [iDataCenter].[dbo].[iPJT_FinWeb_UI_VAR_DT_LIST]
					  where enable = '1' and Var_DT_Open = '1'
					  union all
					  select Var_Value_Field as Var_DT_Filter_Format_Start, Var_Value_Field as Var_DT_Filter_Format_End, Var_Nm as Var_DT_Nm, Var_DT_Desc, Var_Mon_End_Colsing AS Var_DT_Open, Enable
					  from [iDataCenter].[dbo].[iPJT_FinWeb_UI_VAR_ALL_LIST]
					  where Var_Type = 'Version' and Var_Mon_End_Colsing = '1') B
					--[iDataCenter].[dbo].[iPJT_FinWeb_UI_VAR_DT_LIST] B
					ON CASE WHEN LEFT(A.Val,1) = '@' THEN SUBSTRING(A.Val,2,len(A.Val)-1) ELSE A.VAL END = B.Var_DT_Nm AND B.Var_DT_Open = '1'
					WHERE A.ViewName = @ViewName and A.Enable = @RunEnable
					--2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
					and A.ModifyUser =@Accountid
					AND A.FilterGroup IS NOT NULL
					GROUP BY A.FilterGroup
				) A;
			END
			ELSE IF @IsEnable = 3 or @IsEnable = 2 or @IsEnable = 8 or @IsEnable = 5 
			BEGIN
			    --2025/05/12 調整用變式進行資料串聯處理
				SELECT @WhereFields = STRING_AGG(A.where_Field, ' or ')
				FROM (
					SELECT '('+STRING_AGG(CONVERT(varchar(100), 
						A.fieldname + 
						CASE WHEN A.Operator = 1 THEN '=''' 
							 WHEN A.Operator = 2 THEN '<''' 
							 WHEN A.Operator = 3 THEN '>'''
							 WHEN A.Operator = 4 THEN '<='''
							 WHEN A.Operator = 5 THEN '>='''
							 WHEN A.Operator = 6 THEN '<>'''
							 WHEN A.Operator = 7 THEN ' Like '''
							 WHEN A.Operator = 8 THEN ' Not Like '''
							 WHEN A.Operator = 9 THEN ' is null'
							 WHEN A.Operator = 10 THEN ' is not null' END +
						CASE WHEN A.Operator IN (7,8) THEN '%' + A.Val + '%'
							 WHEN A.Operator IN (9,10) THEN ''
							 ELSE CASE WHEN Left(A.Val,1) = '@' THEN B.Var_DT_Filter_Format_Start ELSE A.Val END END + 
						CASE WHEN A.Operator IN (1,2,3,4,5,6,7,8) THEN ''''
							 ELSE '' END
						), ' and ') WITHIN GROUP (ORDER BY A.seq)+')' AS where_Field
					FROM [iTemp].[dbo].[tmpCustView_DataAccess] A
					LEFT JOIN 
					(    select Var_DT_Filter_Format_Start, Var_DT_Filter_Format_End, Var_DT_Nm, Var_DT_Desc, Var_DT_Open, Enable
					  FROM [iDataCenter].[dbo].[iPJT_FinWeb_UI_VAR_DT_LIST]
					  where enable = '1' and Var_DT_Open = '1'
					  union all
					  select Var_Value_Field as Var_DT_Filter_Format_Start, Var_Value_Field as Var_DT_Filter_Format_End, Var_Nm as Var_DT_Nm, Var_DT_Desc, Var_Mon_End_Colsing AS Var_DT_Open, Enable
					  from [iDataCenter].[dbo].[iPJT_FinWeb_UI_VAR_ALL_LIST]
					  where Var_Type = 'Version' and Var_Mon_End_Colsing = '1') B
					--[iDataCenter].[dbo].[iPJT_FinWeb_UI_VAR_DT_LIST] B
					ON CASE WHEN LEFT(A.Val,1) = '@' THEN SUBSTRING(A.Val,2,len(A.Val)-1) ELSE A.VAL END = B.Var_DT_Nm AND B.Var_DT_Open = '1'
					WHERE ViewName = @ViewName 
					AND A.Enable = @RunEnable
					AND A.FilterGroup IS NOT NULL
					--2025/01/22 WEIPING CHUNG 新加入條件,modifyuser須卡AccountID
					and A.ModifyUser =@Accountid
					GROUP BY A.FilterGroup
				) A;
			END
			print 'SelectFields:'+@SelectFields;
			print 'WhereFields:'+@WhereFields;
			SELECT @SelectFields = dbo.RemoveDuplicateColumns(@SelectFields);

			SELECT @IsDistinct = CAST(IsDistinct AS VARCHAR(10)) from [iTemp].[dbo].[tmpCustView_DataAccess]
			WHERE ViewName = @ViewName
			  AND Enable = @RunEnable
			  AND ModifyUser = @Accountid AND FieldName <> 'ShareDT'
			  GROUP BY IsDistinct;

            print 'IsDistinct:'+@IsDistinct;

			SET @OldViewName = @ViewName;

			-- Handle NULL values
			IF @QueryQty = '-1' 
			BEGIN 
				SET @OldSelectFields  = @SelectFields;
				if @IsDistinct = '0' 
				BEGIN
				   SET @SelectFields = ' Count(*) ';
				END
				SET @SQLQueryQty = '';
				print 'OldSelectFields:'+@OldSelectFields
			END
			ELSE
			BEGIN
				SET @SelectFields = ISNULL(@SelectFields, '*');
				IF @IsEnable = '8' or @IsEnable = '72' or @IsEnable = '73' or @IsEnable = '74' or @IsEnable = '62' or @IsEnable = '63' or @IsEnable = '11' or @IsEnable = '61'
				BEGIN
					SET @SQLQueryQty = ' ';
				END
				ELSE
				BEGIN
				    if @IsDistinct = '0' 
					BEGIN
					   SET @SQLQueryQty = ' top '+@QueryQty+' ';
					END
					ELSE
					BEGIN
					   SET @SQLQueryQty = ' ';
					END
				END
			END
			print 'SQLQueryQty:'+@SQLQueryQty;
			print 'OwnerAccountId:'+@OwnerAccountId;
			print 'AccountId:'+@AccountId;
			print 'ShareCustViewLayer:'+@ShareCustViewLayer;
			print 'oldViewName:'+@ViewName;
			print 'Layer:'+@Layer;
			print 'IsEnable:'+@IsEnable;
			SET @GroupFields = ISNULL(@GroupFields, '');
			SET @WhereFields = ISNULL(@WhereFields, '');
		/*--加入代理人處理
		SELECT @CustOwnerId = COALESCE(OwnerId, AccountId)
		FROM itemp.dbo.tmpCustView_DataAccess
		WHERE CustViewId = @CustViewid 
		  AND enable = '3'
		  AND ModifyUser = @Accountid
		GROUP BY AccountId, OwnerId;*/
		--加入代理人處理
		SELECT @CustOwnerId = ISNULL(
			(SELECT COALESCE(OwnerId, AccountId)
			 FROM itemp.dbo.tmpCustView_DataAccess
			 WHERE CustViewId = @CustViewid
			   AND enable = '3'
			   AND ModifyUser = @Accountid
			   AND COALESCE(AccountId,OwnerId) = @Accountid  -- 新增這個條件
			 GROUP BY AccountId, OwnerId), 
			@Accountid
		);
			SET @ViewName = CASE WHEN @Layer = '1' and @isEnable = '5' THEN 
									  CASE WHEN @CustViewLayer = '0' THEN @ViewName+'_C1' ELSE @ViewName+'_C'+@CustViewLayer END  
								 --WHEN @Layer = '2' and @IsEnable = '5' THEN SUBSTRING(@ViewName, 1, LEN(@ViewName) - 1)+@CustViewLayer
								 WHEN @Layer = '2' and @IsEnable = '5' AND @CustOwnerId/*@Accountid*/ = @OwnerAccountId THEN LEFT(@ViewName, LEN(@ViewName) - PATINDEX('%[0-9][^0-9]%', REVERSE(@ViewName)))+@CustViewLayer
								 WHEN @Layer = '2' and @IsEnable = '5' AND @CustOwnerId/*@Accountid*/ <> @OwnerAccountId AND @ShareCustViewLayer = '1' THEN @ViewName+'_S1'  
								 WHEN @Layer = '2' and @IsEnable = '5' AND @CustOwnerId/*@Accountid*/ <> @OwnerAccountId AND @ShareCustViewLayer <> '1' THEN @ViewName+'_S'+@ShareCustViewLayer
								 --WHEN @Layer = '2' and @IsEnable = '5' AND @Accountid <> @OwnerAccountId AND @ShareCustViewLayer <> '1' THEN LEFT(@ViewName, LEN(@ViewName) - PATINDEX('%[0-9][^0-9]%', REVERSE(@ViewName)))+@ShareCustViewLayer
								 WHEN @Layer = '3' and @IsEnable = '5' THEN LEFT(@ViewName, LEN(@ViewName) - PATINDEX('%[0-9][^0-9]%', REVERSE(@ViewName)))+@ShareCustViewLayer
								 ELSE @ViewName END;
			print 'ViewName:' + @ViewName;
			print 'ParentViewName:' + @ParentViewName;
			print 'CustViewId:' + @CustViewId;
			print 'CustOwnerId:' + @CustOwnerId;
			-- Create combined query
			/*if @QueryQty = '-1'
			BEGIN 
				@SQLQueryQty = ' top '+@QueryQty;
			END*/
			SELECT @GetWksItemKindId = dbo.fnGetiDataCenterInfo('18', @CustViewid,'',@CustOwnerId);
			print 'GetWksItemKindId:'+@GetWksItemKindId
			print 'GroupFields:'+@GroupFields

			IF /*LEN(@GroupFields) > 0 and*/ @GetWksItemKindId <= '2' and @IsEnable <> '74'
			BEGIN
				SET @CombinedQuery = 'SELECT ' + @SQLQueryQty + ' ' + @SelectFields + ' FROM ' + @DBName + '.' + @ParentViewName + CHAR(13) + CHAR(10);
			END
			ELSE
			BEGIN
				IF @IsEnable = '74'
				BEGIN
					SET @CombinedQuery = 'SELECT ' + @SQLQueryQty + ' ' + @SelectFields + ' FROM ' + @DBName + '.' + @ViewName + CHAR(13) + CHAR(10);
				END
				ELSE 
				BEGIN
				    --20250924 調整不讀上層View
					--SET @CombinedQuery = 'SELECT ' + @SQLQueryQty + ' ' + @SelectFields + ' FROM ' + @DBName + '.' + @ParentViewName + CHAR(13) + CHAR(10);
					SET @CombinedQuery = 'SELECT ' + @SQLQueryQty + ' ' + @SelectFields + ' FROM ' + @DBName + '.' + @ViewName + CHAR(13) + CHAR(10);
				END
			END
			IF (@Layer = '2' AND @Accountid <> @OwnerAccountId and @IsEnable = '5')
			BEGIN
				SET @CreateViewQuery = 'SELECT ' + @SelectFields + ' FROM ' + @DBName + '.' + @OldViewName + CHAR(13) + CHAR(10);
			END
			ELSE
			BEGIN
				SET @CreateViewQuery = 'SELECT ' + @SelectFields + ' FROM ' + @DBName + '.' + @ParentViewName + CHAR(13) + CHAR(10);
			END

			Print 'DBName:'+@DBName;
			Print 'Layer:'+@Layer;

			IF @Layer = '2' AND @Accountid <> @OwnerAccountId
			BEGIN
				SELECT @SharePublishWhere = dbo.fnGetiDataCenterInfo('13', @CustViewid,@RunEnable,@Accountid);
			END
			ELSE IF @Layer = '3' 
			BEGIN
				SELECT @GetParentId = dbo.fnGetiDataCenterInfo('14', @CustViewid,'1','');
				Print 'GetParentId:'+@GetParentId;
				SELECT @SharePublishWhere = dbo.fnGetiDataCenterInfo('13', @GetParentId,@RunEnable,@Accountid);
			END
			ELSE IF @IsEnable = '74' 
			BEGIN
				SELECT @SharePublishWhere = dbo.fnGetiDataCenterInfo('13', @CustViewid,'3',@Accountid);
			END
        
			-- Add WHERE clause if @WhereFields is not empty
			IF LEN(@WhereFields) > 0
			BEGIN
				IF LEN(@SharePublishWhere)>0 
				BEGIN
					SET @CombinedQuery = @CombinedQuery + ' WHERE ' + @WhereFields + REPLACE(@SharePublishWhere,'WHERE', 'AND') +CHAR(13) + CHAR(10);
					SET @CreateViewQuery = @CreateViewQuery + ' WHERE ' + @WhereFields + CHAR(13) + CHAR(10);
				END
				ELSE
				BEGIN
					SET @CombinedQuery = @CombinedQuery + ' WHERE ' + @WhereFields +CHAR(13) + CHAR(10);
					SET @CreateViewQuery = @CreateViewQuery + ' WHERE ' + @WhereFields + CHAR(13) + CHAR(10);
				END
			END
			ELSE 
			BEGIN
				IF LEN(@SharePublishWhere)>0 
				BEGIN
					SET @CombinedQuery = @CombinedQuery + @SharePublishWhere + CHAR(13) + CHAR(10);
				END
			END

			print 'SharePublishWhere:'+@SharePublishWhere;

			-- Add GROUP BY clause if @GroupFields is not empty
			IF LEN(@GroupFields) > 0
			   BEGIN
				SET @CombinedQuery = @CombinedQuery + ' GROUP BY ' + @GroupFields + CHAR(13) + CHAR(10);
				SET @CreateViewQuery = @CreateViewQuery + ' GROUP BY ' + @GroupFields + ' ; '+CHAR(13) + CHAR(10);
			   END
			print 'CreateViewQuery:'+@CreateViewQuery;
			print 'CombinedQuery:'+@CombinedQuery;

		IF @isEnable in ('5','3')  -- 儲存或另存重新給權限
		BEGIN
			--加入代理人處理
  			SELECT @CustOwnerId = ISNULL(
				(SELECT COALESCE(OwnerId,AccountId)
				 FROM itemp.dbo.tmpCustView_DataAccess
				 WHERE CustViewId = @CustViewid
				   AND enable = '3'
				   AND ModifyUser = @Accountid
				   AND COALESCE(AccountId, OwnerId) = @Accountid  -- 新增這個條件
				 GROUP BY AccountId, OwnerId), 
				@Accountid
			);
			/*SELECT @CustOwnerId = COALESCE(OwnerId, AccountId)
			FROM itemp.dbo.tmpCustView_DataAccess
			WHERE CustViewId = @CustViewid 
			  AND enable = '3'
			  AND ModifyUser = @Accountid
			GROUP BY AccountId, OwnerId;*/
			SELECT @DomainUser = dbo.fnGetDomainUserByAccountId(@AccountId);
			--SELECT @DomainUser = dbo.fnGetDomainUserByAccountId(@CustOwnerId/*@AccountId*/);
			SELECT @IsShareDT = dbo.fnGetiDataCenterInfo('10', @CustViewid,'1','');
			IF len(@IsShareDT) = 0
			BEGIN
				SET @IsShareDT = 'N';
			END

			EXEC [iOPEN].[dbo].[SetupUnifiedSecurity]
				@UserName = @DomainUser,
				@StartDate = '',
				@EndDate = '',
				@IsADUser = 1,
				@IsOwner = 1,
				@IsnonDate = 1,
				@CheckDate = @IsShareDT,
				@CustomView = 'C',
				@DBName = @DBName,
				@ViewName = @CreateViewQuery,
				@DisplayViewName = @ViewName,
				@Source = '',
				@inIsEnable = @IsEnable;
		   if @NewSaveAccountId is not null
		   BEGIN
			   EXEC iopen.dbo.sp_ManageCustViewAccess 
				   @CustViewId = @NewSaveCustviewid, 
				   @AccountId = @NewSaveAccountId, 
				   @Kind = 1, 
				   @Oper = 1;
		   END
		END
		ELSE IF @IsEnable = '6' -- 建立分享資訊
		BEGIN
			exec [dbo].[GetSharedCustViewData] @inCustViewId = @CustViewid, @inPublishId = '', @inIsEnable = @IsEnable, @inAccountId = @Accountid;
		END
		/*ELSE IF @IsEnable = '61'
		BEGIN
		    Print 'Exec [dbo].[GetSharedCustViewData] :';
			exec [dbo].[GetSharedCustViewData] @CustViewId = @CustViewid, @PublishId = '', @IsEnable = @IsEnable;
		END*/

		END TRY
		BEGIN CATCH
			DECLARE @ErrorMessage NVARCHAR(4000);
			DECLARE @ErrorSeverity INT;
			DECLARE @ErrorState INT;

			SELECT 
				@ErrorMessage = ERROR_MESSAGE(),
				@ErrorSeverity = ERROR_SEVERITY(),
				@ErrorState = ERROR_STATE();

			RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
		END CATCH
		SET @OutputValue = @CombinedQuery;
		print '@OutputValue:'+@OutputValue;
		SET @WebFunCode = @isEnable;
    END

	if @IsEnable NOT IN ('63','62','61','71','72','73')
	BEGIN
	EXEC [iLog].[dbo].[WEBUI_UseCustView_Ins] @CustViewId,@AccountId, @WebFunCode
	END

	IF @QueryQty = '-1' and @Layer <> '3'
	BEGIN
	    print 'GroupFields:'+@GroupFields;
		print 'QsourceCombinedQuery:'+@CombinedQuery;
		if @GetWksItemKindId <= '2'
		BEGIN
		   SELECT @CombinedQuery = [dbo].[FN_CONVERT_TO_OPENQUERY](@CombinedQuery,@GroupFields,@OldSelectFields,@Layer,@IsDistinct);
		END 
		print 'QnewCombinedQuery:'+@CombinedQuery;
	END
	ELSE IF @Layer <> '3' and @isEnable <> '74' --@ParentViewName = 'IPJT_IMART_CUSTPNL_GRP'
	BEGIN
	   print 'sourceCombinedQuery:'+@CombinedQuery;
	   print 'isDistinct:'+@isDistinct;
	   if @GetWksItemKindId <= '2'
	   BEGIN
		SELECT @CombinedQuery = [dbo].[FN_CONVERTTOOPENQUERY_TEST](@CombinedQuery,@DBName,@ParentViewName,@IsDistinct);
	   END
	   print 'newCombinedQuery:'+@CombinedQuery;
	END

	/*IF @IsPublished = 'Y' --Enable = 11,自訂刪除
	BEGIN
		SELECT 'YS' AS OutputValue;
	END
	ELSE
	BEGIN
		SET @CombinedQuery = REPLACE(@CombinedQuery, ',ShareDT', '');
		SELECT @CombinedQuery AS OutputValue;
		--SELECT @CombinedQuery AS OutputValue;
	END*/

	IF @IsPublished = 'Y' --Enable = 11,自訂刪除
	BEGIN
		SELECT 'YS' AS OutputValue;
	END
	ELSE
	BEGIN
	    if @QueryQty <> '-1'
		BEGIN
		   if @IsEnable not in ('8','74')
		   BEGIN
		      SELECT @CombinedQuery = QuerySQL_TopN,@CombinedQueryCount = QuerySQL_Count FROM [dbo].[fn_GenerateCustomViewSQL](@CustViewid, @QueryQty,@Accountid,@IsDistinct);
		   END
		   ELSE
		   BEGIN
		      SELECT @CombinedQuery = QuerySQL,@CombinedQueryCount = QuerySQL_Count FROM [dbo].[fn_GenerateCustomViewSQL](@CustViewid, @QueryQty,@Accountid,@IsDistinct);
		   END
		   SET @OutputValue = @CombinedQuery;
		   --SELECT * FROM [dbo].[fn_GenerateCustomViewSQL]('C47D90A7-8947-4D13-9671-C86F813AAC71',100);
		END
		ELSE
		BEGIN
	       SELECT @CombinedQuery = QuerySQL_Count FROM [dbo].[fn_GenerateCustomViewSQL](@CustViewid, @QueryQty,@Accountid,@IsDistinct);
		   SET @OutputValue = @CombinedQuery;
		END
		SET @CombinedQuery = REPLACE(@CombinedQuery, ',ShareDT', '');
		SELECT @CombinedQuery AS OutputValue;
		print 'newCombinedQueryend:'+@CombinedQuery;
	END
	--SELECT @CreateViewQuery AS OutputValue;
	--SELECT @ViewName AS OutputValue;
	EXEC [iLog].[dbo].[sp_GetViewFieldsAndFilters_SQL_WithLogging]
        @CustViewid, @ViewName, @IsEnable, @Accountid, @ViewNo, @QueryQty, @OutputValue
END;
GO


