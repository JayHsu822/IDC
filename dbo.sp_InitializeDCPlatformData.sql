USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[sp_InitializeDCPlatformData]    Script Date: 2025/12/7 上午 10:59:43 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




















CREATE OR ALTER   PROCEDURE [dbo].[sp_InitializeDCPlatformData]
    @TB_NM NVARCHAR(50)
AS
BEGIN
/*2025/01/20		Weiping 調整Share_DT判斷
*/
	DECLARE @OldResId NVARCHAR(36);
	DECLARE @PreResId NVARCHAR(36);
	DECLARE @NewRedId NVARCHAR(36);
	DECLARE @tbShareDT NVARCHAR(50);
	DECLARE @tbResno NVARCHAR(200);
	DECLARE @tbResName NVARCHAR(200);
	DECLARE @tbDBName NVARCHAR(200);
	DECLARE @tbEmpNo NVARCHAR(20);
	DECLARE @tbEmemo NVARCHAR(20);
	DECLARE @NewAccountId NVARCHAR(36);
	DECLARE @OldCustViewId NVARCHAR(36);
	DECLARE @PreCustViewId NVARCHAR(36);
	DECLARE @NewCustviewId NVARCHAR(36);
	DECLARE @C1NewCustviewId NVARCHAR(36);
    -- 宣告變數用於錯誤處理
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @StartTime DATETIME;
    DECLARE @EndTime DATETIME;
    
    SET @StartTime = GETDATE();
    
    BEGIN TRY
        -- 開始交易
        BEGIN TRANSACTION;
        
        SET NOCOUNT ON;

        -- 檢查資料表是否存在，如果存在則刪除
        IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'tbCustViewV') AND type in (N'U'))
        BEGIN
            -- 先清空現有資料
            TRUNCATE TABLE tbCustViewV;
            DROP TABLE tbCustViewV;
			Print 'DropTable';
        END

        -- 創建新資料表
        CREATE TABLE tbCustViewV (
            TD_TB_NM [nvarchar](50) NOT NULL,
            Master_TB_NM [nvarchar](50) NOT NULL,
            Parent_TB_NM [nvarchar](50) NOT NULL,
            TB_NM [nvarchar](50) NOT NULL,
            TB_Kind [int] NOT NULL,
            TB_Type [char](1) NOT NULL,
            TB_Layer [int] NOT NULL,
            TB_Enable [int] NOT NULL,
            [CreateUser] [nvarchar](36) NULL,
            [CreateTime] [datetime] NULL,
            [ModifyUser] [nvarchar](36) NULL,
            [ModifyTime] [datetime] NULL
        );

        Print 'Create:';
        -- 插入初始化資料
        -- OriTB
        INSERT INTO tbCustViewV
        SELECT 
            ResName as TD_TB_NM,
            ResNo as Master_TB_NM,
            ResNo as Parent_TB_NM,
            ResNo as TB_NM,
            Kind as TB_Kind,
            'O' as TB_Type,
            1 as TB_Layer,
            1 as TB_Enable,
            'Sys' as CreateUser,
            CURRENT_TIMESTAMP as [CreateTime],
            'Sys' as [ModifyUser],
            CURRENT_TIMESTAMP as [ModifyTime]
        FROM tbResInitTmp A
        WHERE A.ResName = @TB_NM;

        -- 自訂TB
        INSERT INTO tbCustViewV
        SELECT 
            ResName as TD_TB_NM,
            ResNo as Master_TB_NM,
            ResNo as Parent_TB_NM,
            ResNo+'_C1' as TB_NM,
            Kind as TB_Kind,
            'C' as TB_Type,
            2 as TB_Layer,
            1 as TB_Enable,
            'Sys' as CreateUser,
            CURRENT_TIMESTAMP as [CreateTime],
            'Sys' as [ModifyUser],
            CURRENT_TIMESTAMP as [ModifyTime]
        FROM tbResInitTmp A
        WHERE A.ResName = @TB_NM AND (A.Share_PJT IS NOT NULL OR A.Share_CO IS NOT NULL OR A.Share_FI IS NOT NULL OR A.Share_CI IS NOT NULL OR A.Share_GSI IS NOT NULL OR A.Share_ICM IS NOT NULL);
		print '建匯入資料:'+@TB_NM;
        -- 檢查資料表是否存在，如果存在則刪除
        IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'tbCustViewUser') AND type in (N'U'))
        BEGIN
            -- 先清空現有資料
            TRUNCATE TABLE tbCustViewUser;
            DROP TABLE tbCustViewUser;
        END
		CREATE TABLE tbCustViewUser (
		--資料表名稱
		TB_NM [nvarchar](50) NOT NULL,
		--TB使用人員
		AccountID [nvarchar](36) NOT NULL,
		--帳號類別 1:擁有者,2:被分享者
		AccountType  [int] NOT NULL,
		--TB使用人員是否啟用
		AccountEnable [int] NOT NULL,
		CustViewNm [nvarchar](20) NULL,
		--建立人員
		[CreateUser] [nvarchar](36) NULL,
		--建立時間
		[CreateTime] [datetime] NULL,
		--修改人員
		[ModifyUser] [nvarchar](36) NULL,
		--修改時間
		[ModifyTime] [datetime] NULL
		);

		--select * from [tbCustView]

		--select * from [tbRes]

		--新增擁有者
		insert into tbCustViewUser
			select ResNo as TB_NM,
			B.ID as AccountID,
			1   as AccountType,
			1     as AccountEnable,
			NULL,
			'Init' as CreateUser,
			CURRENT_TIMESTAMP as [CreateTime] ,
			'Sys' as [ModifyUser] ,
			CURRENT_TIMESTAMP as [ModifyTime]
			--,* 
			from tbResInitTmp a
			left join
			(select ID,upper(organize+'\'+empno) as domain_user from [dbo].[tbSysAccount] where organize is not null) b
			on upper(a.EmpNo) = b.domain_user
			WHERE a.ResName = @TB_NM
			;

			insert into tbCustViewUser
			select ResNo+'_C1' as TB_NM,
				B.ID as AccountID,
				2   as AccountType,
				1   as AccountEnable,
				CASE 
					WHEN s.EmpNo IN (SELECT upper(trim(value)) FROM STRING_SPLIT(t.Share_PJT, ',') WHERE value > '') THEN 'PJT'
					WHEN s.EmpNo IN (SELECT upper(trim(value)) FROM STRING_SPLIT(t.Share_CO, ',') WHERE value > '') THEN 'CO'
					WHEN s.EmpNo IN (SELECT upper(trim(value)) FROM STRING_SPLIT(t.Share_FI, ',') WHERE value > '') THEN 'FI'
					WHEN s.EmpNo IN (SELECT upper(trim(value)) FROM STRING_SPLIT(t.Share_CI, ',') WHERE value > '') THEN 'CI'
					WHEN s.EmpNo IN (SELECT upper(trim(value)) FROM STRING_SPLIT(t.Share_GSI, ',') WHERE value > '') THEN 'GSI'
					WHEN s.EmpNo IN (SELECT upper(trim(value)) FROM STRING_SPLIT(t.Share_ICM, ',') WHERE value > '') THEN 'ICM'
				END as CustViewNm,
				'Init' as CreateUser,
				CURRENT_TIMESTAMP as [CreateTime],
				'Sys' as [ModifyUser],
				CURRENT_TIMESTAMP as [ModifyTime]
			from tbResInitTmp t
			CROSS APPLY (
				SELECT upper(trim(value)) AS EmpNo
				FROM STRING_SPLIT(CONCAT(
					NULLIF(t.Share_PJT, ''), 
					CASE WHEN t.Share_PJT > '' AND t.Share_CO > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_CO, ''),
					CASE WHEN (t.Share_CO > '' OR t.Share_PJT > '') AND t.Share_FI > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_FI, ''),
					CASE WHEN (t.Share_CO > '' OR t.Share_PJT > '' OR t.Share_FI > '') AND t.Share_CI > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_CI, ''),
					CASE WHEN (t.Share_CO > '' OR t.Share_PJT > '' OR t.Share_FI > '' OR t.Share_CI > '') AND t.Share_GSI > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_GSI, ''),
					CASE WHEN (t.Share_CO > '' OR t.Share_PJT > '' OR t.Share_FI > '' OR t.Share_CI > '' OR t.Share_GSI > '') AND t.Share_ICM > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_ICM, '')
				), ',')
				WHERE value > ''
			) s
			left join
			(select ID, upper(empno) as domain_user 
			 from [dbo].[tbSysAccount] 
			 where organize is not null
			) b
			on s.EmpNo = b.domain_user
			where ResName = @TB_NM
			and B.ID is not null;

/*			--新增被分享者
			insert into tbCustViewUser
			select ResNo+'_C1' as TB_NM,
				B.ID as AccountID,
				2   as AccountType,
				1   as AccountEnable,
				'Sys' as CreateUser,
				CURRENT_TIMESTAMP as [CreateTime],
				'Sys' as [ModifyUser],
				CURRENT_TIMESTAMP as [ModifyTime]
			from tbResInitTmp t
			CROSS APPLY (
				SELECT upper(trim(value)) AS EmpNo
				FROM STRING_SPLIT(CONCAT(
					NULLIF(t.Share_PJT, ''), 
					CASE WHEN t.Share_PJT > '' AND t.Share_CO > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_CO, ''),
					CASE WHEN (t.Share_CO > '' OR t.Share_PJT > '') AND t.Share_FI > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_FI, ''),
					CASE WHEN (t.Share_CO > '' OR t.Share_PJT > '' OR t.Share_FI > '') AND t.Share_CI > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_CI, ''),
					CASE WHEN (t.Share_CO > '' OR t.Share_PJT > '' OR t.Share_FI > '' OR t.Share_CI > '') AND t.Share_GSI > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_GSI, ''),
					CASE WHEN (t.Share_CO > '' OR t.Share_PJT > '' OR t.Share_FI > '' OR t.Share_CI > '' OR t.Share_GSI > '') AND t.Share_ICM > '' THEN ',' ELSE '' END,
					NULLIF(t.Share_ICM, '')
				), ',')
				WHERE value > ''
			) s
			left join
			(select ID, upper(empno) as domain_user 
			 from [dbo].[tbSysAccount] 
			 where organize is not null
			) b
			on s.EmpNo = b.domain_user
			where ResName = @TB_NM
			and B.ID is not null;
*/
		select @OldResId = id FROM tbRes WHERE ResName = @TB_NM AND Enable = '2';
		select @PreResId = id FROM tbRes WHERE ResName = @TB_NM AND Enable = '4';

		DELETE from tbRes
		WHERE Enable = '4' AND ResName = @TB_NM;
		Print 'OldResId:'+@OldResId;
		update tbRes
		SET Enable = '4'
		WHERE ResName = @TB_NM AND Enable = '2';

		insert into tbRes
		select 
				a.[Id]
				,a.[Kind]
				,LEFT(a.[ResNo], CHARINDEX('_', a.[ResNo]) - 1)
				,a.[ResNo]
				,a.[ResName]
				,null ResSel
				,a.Security
				,B.ID as AccountID
				,upper(B.empno) as EmpNo
				,2 as Enable,
		'Init' as CreateUser,
		CURRENT_TIMESTAMP as [CreateTime] ,
		'Sys' as [ModifyUser] ,
		CURRENT_TIMESTAMP as [ModifyTime],
		null AS DQSetTime
		--,* 
		from tbResInitTmp a
		left join
		(select ID,upper(organize+'\'+empno) as domain_user,empno from [dbo].[tbSysAccount] where organize is not null and enable =1) b
		on upper(a.EmpNo) = b.domain_user
		WHERE a.ResName = @TB_NM;

		SELECT @NewRedId = id, @tbShareDT = ShareDT, @tbResNo = ResNo , @tbResName = ResName, @tbEmpNo = Empno, @tbEmemo = Ememo,@tbDBName = CASE 
        WHEN CHARINDEX('_', ResNo) > 0 
        THEN LEFT(ResNo, CHARINDEX('_', ResNo) - 1)
        ELSE ResNo END
		FROM tbResInitTmp WHERE ResName = @TB_NM; 

		print 'ShareDT:'+@tbShareDT;		

		print 'DELETE from tbResSchema';
		DELETE from tbResSchema
		WHERE resid = @PreResId and Enable = '4';

		print 'update tbResSchema';
		update tbResSchema
		SET enable = '4'
		WHERE resid = @OldResId; 
		--delete tbResSchema where enable=2;
		insert into tbResSchema
		SELECT newid() as ID
			  ,A.ID as [ResId]
			  ,B.ColumnName as [FieldName]
			  ,case when B.ColumnType in ('CV','CF') then 1  when B.ColumnType in ('I') then 2   when B.ColumnType in ('D') then 3  else 9 end  as [FieldType]
			  ,0 as [IsTimeVar]
			  ,null as [TimeFormat]
			,1 as [Inheritable]
			,null as [locked]
				, 2 as [Enable]
		,B.ColumnID as [Seq]
		,'Init' as CreateUser
		,CURRENT_TIMESTAMP as [CreateTime] 
		,'Sys' as [ModifyUser] 
		,CURRENT_TIMESTAMP as [ModifyTime]
		--,* 
		from 
		(select * from tbRes where Enable=2 AND ResName = @TB_NM) a
		left join
		(select * from iOpen.ipjt.columnsvx) b
		on upper(a.ResName) = upper(b.DatabaseName + '.' + b.TableName)
		where b.DatabaseName is not null;

		--建立平台相關資料(Enable:1)
		--1.tbRes
		DELETE FROM tbRes
		WHERE id = @OldResId AND Enable = '1';

		Insert into tbRes
		SELECT
			[Id]
		  ,[Kind]
		  ,[DbName]
		  ,[ResNo]
		  ,[ResName]
		  ,[ResSel]
		  ,[Security]
		  ,[AccountId]
		  ,[EmpNo]
		  ,'1'
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
		  ,[DQSetTime]
	  FROM [iDataCenter].[dbo].[tbRes]
	  WHERE id = @NewRedId and Enable = '2';

		DELETE FROM tbResSchema
		WHERE resid = @OldResId and enable = '1';
		Insert Into tbResSchema
		SELECT [Id]
			  ,[ResId]
			  ,[FieldName]
			  ,[FieldType]
			  ,[IsTimeVar]
			  ,[TimeFormat]
			  ,[Inheritable]
			  ,[Locked]
			  ,'1'
			  ,[Seq]
			  ,[CreateUser]
			  ,[CreateTime]
			  ,[ModifyUser]
			  ,[ModifyTime]
		  FROM [iDataCenter].[dbo].[tbResSchema]
		  WHERE ResId = @NewRedId and Enable = '2';
		  
		  IF len(@tbShareDT) > 0
		  BEGIN
		    delete from tbResSchema
			WHERE id = @NewRedId and seq = '99999';

			INSERT INTO tbResSchema (ResId,FieldName,FieldType,Locked,Seq,Enable,CreateUser,ModifyUser)
			SELECT id,'ShareDT',1,1,99999,1,'Init','Sys'
			FROM tbRes
			WHERE id = @NewRedId and enable = '1';
		  END
		    SELECT @NewAccountId = accountId FROM tbRes WHERE id = @NewRedId AND Enable = '1';

			SELECT @PreCustViewId = id FROM tbCustView where upper(ViewName)=upper(@tbResno) and Enable = '4';

			SELECT @OldCustViewId = id FROM tbCustView where upper(ViewName)=upper(@tbResno) and Enable = '2';
		  
			delete tbCustView where upper(ViewName)=upper(@tbResno) and enable ='4';
			Update tbCustView
			set enable = '4'
			where upper(ViewName)=upper(@tbResno) and enable ='1';

			--刪除原有資料
				-- 宣告變數
				DECLARE @DelCustViewId VARCHAR(36);

				--刪除權限資料
				  delete from [iOpen].[dbo].[UserDateRange_rls_unified]
                  where viewname like @tbResno+'%';

				  EXEC iopen.dbo.sp_RevokeCustViewAllSelectPermissions @ViewName = @tbResno;
				  /*
					DECLARE @SQL NVARCHAR(MAX) = ''
        
					SELECT @SQL = @SQL + 
						'REVOKE SELECT ON [' + STUFF(OBJECT_NAME(perm.major_id), CHARINDEX('_', OBJECT_NAME(perm.major_id)), 1, '.') + '] FROM [' + usr.name + '];' + CHAR(13)
					FROM sys.database_permissions perm
						INNER JOIN sys.database_principals usr 
						ON perm.grantee_principal_id = usr.principal_id
					WHERE 
						perm.class = 1
						AND perm.permission_name = 'SELECT'
						AND OBJECT_NAME(perm.major_id) like @tbResno+'%'

					IF LEN(@SQL) > 0
					BEGIN
						PRINT '執行的 SQL: ' + @SQL  -- 用於偵錯
						EXEC sp_executesql @SQL
						PRINT '已成功移除所有 SELECT 權限'
					END
					ELSE
					BEGIN
						PRINT '@tbResno:'+@tbResno	
						PRINT '執行的 SQL: ' + @SQL  -- 用於偵錯
						PRINT '沒有找到需要移除的權限'
					END*/

				-- 宣告游標
				IF CURSOR_STATUS('global','DelCustView_cursor') >= 0
				BEGIN
					CLOSE DelCustView_cursor
					DEALLOCATE DelCustView_cursor
				END
				DECLARE DelCustView_cursor CURSOR FOR 
				SELECT id
				FROM tbCustView
				WHERE upper(ViewName) like upper(@tbResno)+'%';

				-- 開啟游標
				OPEN DelCustView_cursor

				-- 擷取第一筆資料
				FETCH NEXT FROM DelCustView_cursor INTO @DelCustViewId;

				-- 使用 @@FETCH_STATUS 檢查是否還有資料
				WHILE @@FETCH_STATUS = 0
				BEGIN
					-- 處理資料
					delete from tbCustView WHERE id = @DelCustViewId and enable = '1';
					delete from tbWksItem WHERE id = @DelCustViewId and enable = '1';
					--子刪除
							IF CURSOR_STATUS('global','DelPublish_cursor') >= 0
							BEGIN
								CLOSE DelPublish_cursor
								DEALLOCATE DelPublish_cursor
							END
							DECLARE @DelPublishId NVARCHAR(36);
							-- 宣告游標
							DECLARE DelPublish_cursor CURSOR FOR 
							SELECT id
							FROM tbPublish
							WHERE CustViewId = @DelCustViewId;

							-- 開啟游標
							OPEN DelPublish_cursor

							-- 擷取第一筆資料
							FETCH NEXT FROM DelPublish_cursor INTO @DelPublishId;

							-- 使用 @@FETCH_STATUS 檢查是否還有資料
							WHILE @@FETCH_STATUS = 0
							BEGIN
								-- 處理資料
								delete from tbPublishPeriod WHERE PublishId = @DelPublishId;

    
								-- 擷取下一筆
								FETCH NEXT FROM DelPublish_cursor INTO @DelPublishId;
							END

							-- 關閉游標
							CLOSE DelPublish_cursor

							-- 釋放游標
							DEALLOCATE DelPublish_cursor
						--刪除結尾
					--子刪除結尾
					delete from tbPublish WHERE id = @DelCustViewId;
					-- 擷取下一筆
					FETCH NEXT FROM DelCustView_cursor INTO @DelCustViewId;
				END

				-- 關閉游標
				CLOSE DelCustView_cursor

				-- 釋放游標
				DEALLOCATE DelCustView_cursor
			--刪除結尾



			select @NewCustviewid = newid();

			print '11NewCustviewid:'+@NewCustviewid;

			insert into tbCustView
			select @NewCustviewid as ID,@NewRedId as ParentId, @NewRedid AS MasterId,1 as Layer ,0 as LayerNo,null as ViewNo
			,ResNo as ViewName,null as ViewJoin,null as ViewSel,null as ViewWhere,null as ViewGroup,null as ViewOrder,null as Dql,null as AiDql,1 as RLS,AccountId ,1 as Enable
			,
			'Init' as CreateUser,
			CURRENT_TIMESTAMP as [CreateTime] ,
			'Sys' as [ModifyUser] ,
			CURRENT_TIMESTAMP as [ModifyTime]
			--,*
			from tbRes where id = @NewRedId and enable = '1';

			DELETE from tbWksItem
			where CustViewId = @PreCustViewId ;

			Update tbWksItem
			set enable = '4'
			where id = @OldCustViewId and enable = '1';

			Insert INTO [iDataCenter].[dbo].[tbWksItem](
				   [Id]
				  ,[CustViewId]
				  ,[WksNodeId]
				  ,[WksItemKindId]
				  ,[StartDT]
				  ,[Effective]
				  ,[Ememo]
				  ,[AccountId]
				  ,[Enable]
				  ,[CreateUser]
				  ,[CreateTime]
				  ,[ModifyUser]
				  ,[ModifyTime]
			)
			SELECT Newid()
				  ,@NewCustviewId
				  ,'24159CDB-714A-4BE9-9BBD-36863D7D2935'
				  ,'1'
				  ,NULL AS [StartDT]
				  ,NULL AS [Effective]
				  ,@tbEmemo
				  ,@NewAccountId
				  ,'1'
				  ,'Init'
				  ,GetDATE()
				  ,'Sys'
				  ,GetDATE()
			  from tbRes where id = @NewRedId and enable = '1';
		
		DECLARE @RetbResName NVARCHAR(100);
		DECLARE @isShareDT NVARCHAR(50);
		SET @RetbResName = @tbDBName + '.' + REPLACE(@tbResName, '.', '_');
		print 'tbShareDT:'+@tbShareDT;
		IF len(@tbShareDT) > 0
		BEGIN 
		   SET @isShareDT = 'ShareDT'
		END
		ELSE
		BEGIN
			SET @isShareDT = 'N';
		END
		print 'tbEmpNo:'+@tbEmpNo;
		print 'isShareDT:'+@isShareDT;
		print 'tbDBName:'+@tbDBName;
		print 'RetbResName:'+@RetbResName;
		print 'tbResno:'+@tbResno;
		
		print '[iOpen].[dbo].[SetupUnifiedSecurity]-Start';
  		EXEC [iOpen].[dbo].[SetupUnifiedSecurity]
			@UserName = @tbEmpNo,
			@StartDate = '',
			@EndDate = '',
			@IsADUser = 1,
			@IsOwner = 1,
			@IsnonDate = 1,
			@CheckDate = @isShareDT,
			@CustomView ='',
			@DBName = @tbDBName,
			@ViewName = @RetbResName,
			--REPLACE(@tbResName, '.', '_'),
			@DisplayViewName = @tbResno,
			@Source = '',
			@inIsEnable = '1';
		print '[iOpen].[dbo].[SetupUnifiedSecurity]-End';
		DECLARE @isHaveCustView NVARCHAR(10);
		select @isHaveCustView = AccountType from tbCustViewUser WHERE AccountType = '2' group by AccountType;
		
	   --建立自訂View
	   Print 'isHaveCustView:'+@isHaveCustView;
	   IF @isHaveCustView = '2'
	   BEGIN
		   SELECT @C1NewCustviewId = NEWID();
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
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
		  )
		  SELECT 
		   @C1NewCustviewID
		  ,@NewCustviewId
		  ,[ResId]
		  ,'2'
		  ,'1'
		  ,''
		  ,ViewName+'_C1'
		  ,AccountId
		  ,'1'
		  ,'Init'
		  ,GETDATE()
		  ,'Sys'
		  ,GETDATE() 
		  FROM vCustView_DataAccess
		  WHERE CustViewId = @NewCustViewId and Enable = '1'
		  GROUP BY [CustViewId],[ParentId],[ResId],[Layer],[LayerNo],[ViewNo],[ViewName],[AccountId];

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
		  ,[CreateUser]
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
		  )
		  SELECT 
		   NewID()
		  ,@C1NewCustviewID
		  ,[ResSchemaId]
		  ,[FieldName]
		  ,[ShowField]
		  ,[NickName]
		  ,[Summarize]
		  ,[SortKind]
		  ,[SortKey]
		  ,[Seq]
		  ,'1'
		  ,'Init'
		  ,GETDATE()
		  ,'Sys'
		  ,GETDATE()
		  FROM vCustView_DataAccess
		  WHERE CustViewId = @NewCustViewId and Enable = '1' and ShowField = '1'
		  GROUP BY [ResSchemaId],[FieldName],[ShowField],[NickName],[Summarize],[SortKind],[SortKey],[Seq],[AccountId];

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
		  ,[CreateTime]
		  ,[ModifyUser]
		  ,[ModifyTime]
		  )
		  SELECT 
		   NewID()
		  ,@C1NewCustviewID
		  ,[ResSchemaId]
		  ,[Operator]
		  ,[Val]
		  ,[FilterGroup]
		  ,'1'
		  ,'Init'
		  ,GETDATE()
		  ,'Sys'
		  ,GETDATE()
		  FROM vCustView_DataAccess
		  WHERE CustViewId = @NewCustViewId and Enable = '1' AND Val IS NOT NULL
		  GROUP BY [ResSchemaId],[Operator],[Val],[FilterGroup],[FilterUser],[AccountId];

		Insert INTO [iDataCenter].[dbo].[tbWksItem](
			   [Id]
			  ,[CustViewId]
			  ,[WksNodeId]
			  ,[WksItemKindId]
			  ,[StartDT]
			  ,[Effective]
			  ,[Ememo]
			  ,[AccountId]
			  ,[Enable]
			  ,[CreateUser]
			  ,[CreateTime]
			  ,[ModifyUser]
			  ,[ModifyTime]
		)
		SELECT Newid()
			  ,@C1NewCustviewId
			  ,'24159CDB-714A-4BE9-9BBD-36863D7D2935'
			  --,'2'
			  ,'2'
			  ,''
			  ,''
			  ,@tbEmemo
			  ,AccountId
			  ,'1'
			  ,'Init'
			  ,GetDATE()
			  ,'Sys'
			  ,GetDATE()
		  FROM vCustView_DataAccess
		  where [CustViewId] = @NewCustViewId
		  GROUP BY CustViewId,Layer,AccountId;

			DECLARE @RetbResNo NVARCHAR(100);
			SET @RetbResNo = @tbResno+'_C1';
			Print 'RetbResNo:'+@RetbResNo;
			Print '[iOpen].[dbo].[SetupUnifiedSecurity]-Start';
  			EXEC [iOpen].[dbo].[SetupUnifiedSecurity]
				@UserName = @tbEmpNo,
				@StartDate = '',
				@EndDate = '',
				@IsADUser = 1,
				@IsOwner = 1,
				@IsnonDate = 1,
				@CheckDate = @isShareDT,
				@CustomView ='',
				@DBName = @tbDBName,
				@ViewName = @RetbResName,
				--REPLACE(@tbResName, '.', '_'),
				@DisplayViewName = @RetbResNo,
				@Source = '',
				@inIsEnable = '1';
			Print '[iOpen].[dbo].[SetupUnifiedSecurity]-end';
	END

--分享人員
	Print 'CustView:';
	DECLARE @CustViewAccountId NVARCHAR(36);
	DECLARE @CustViewGroup NVARCHAR(20);
	DECLARE @CustViewEmemo NVARCHAR(20);
	DECLARE @CustViewEmpNo NVARCHAR(36);
	DECLARE @CustViewPublishId NVARCHAR(36);
	DECLARE @IsCustView NVARCHAR(10);

	select @IsCustView = TB_Type from [dbo].[tbCustViewV]
	where TB_Type = 'C';

	print 'IsCustView:'+@IsCustView;

	IF @IsCustView = 'C' 
	BEGIN
		IF CURSOR_STATUS('global','CustViewGroup_Curr') >= 0
		BEGIN
			CLOSE CustViewGroup_Curr
			DEALLOCATE CustViewGroup_Curr
		END

		DECLARE CustViewGroup_Curr CURSOR FOR 
		SELECT DISTINCT CustViewNm AS CustViewNm FROM [dbo].[tbCustViewUser] where AccountType = '2';

		-- 開啟游標
		OPEN CustViewGroup_Curr

		-- 擷取第一筆資料
		FETCH NEXT FROM CustViewGroup_Curr INTO @CustViewGroup;

		print 'newGroup:'+@CustViewGroup;

		-- 使用 @@FETCH_STATUS 檢查是否還有資料
		WHILE @@FETCH_STATUS = 0
		BEGIN

	    Print 'Start CustView!';
	    Select @CustViewPublishId = newid();
		INSERT INTO [iDataCenter].[dbo].[tbPublish](
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
			,[Enable]
			,[CreateUser]
			,[CreateTime]
			,[ModifyUser]
			,[ModifyTime]
		)
		VALUES (
			@CustViewPublishId    -- @PublishId
			,@C1NewCustviewID     -- CustViewId
			,NULL                  -- FileId
			,'1'                   -- Kind
			,@CustViewGroup            -- Name (原為 GroupNm)
			,'3'                   -- Frequency
			,'0'                   -- Period
			,'3'                   -- Notify
			,NULL                  -- NotifyStatus
			,NULL                  -- PDay
			,NULL                  -- PTime
			,'-1'                  -- YearOffset
			,'0'                   -- StartDT
			,'0'                   -- EndDT
			,'1'                   -- Enable
			,'Init'                 -- CreateUser
			,GETDATE()            -- CreateTime
			,'Sys'                -- ModifyUser
			,GETDATE()            -- ModifyTime
		);
		print 'tbPublish';
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
		)VALUES (
		       NewId()--Newid()
			  ,@CustViewPublishId--@PublishId
			  ,NULL
			  ,NULL
			  ,NULL
			  ,NULL
			  ,NULL
			  ,'1'
			  ,'Init'
			  ,GetDATE()
			  ,'Sys'
			  ,GetDATE()
		);
		print 'tbPublishPeriod';
		IF CURSOR_STATUS('global','CustView_Curr') >= 0
		BEGIN
			CLOSE CustView_Curr
			DEALLOCATE CustView_Curr
		END

		--2025/01/27 刪除分享更新Trigger會發送mail清單
		DELETE FROM tbFiles
		WHERE PublishId = @CustViewPublishId;

		DECLARE CustView_Curr CURSOR FOR 
		SELECT a.AccountID, b.Organize+'\'+b.EmpNo AS CustViewEmp FROM [dbo].[tbCustViewUser] a
		LEFT JOIN [dbo].[tbSysAccount] b
		ON a.AccountID = b.Id --AND b.Enable = '1'
		where AccountType = '2' AND CustViewNm = @CustViewGroup;

		-- 開啟游標
		OPEN CustView_Curr

		-- 擷取第一筆資料
		FETCH NEXT FROM CustView_Curr INTO @CustViewAccountId,@CustViewEmpNo;

		print 'newaccountid:'+@CustViewAccountId+',newemeo:'+@CustViewGroup+',CustViewEmpNo:'+@CustViewEmpNo;

		-- 使用 @@FETCH_STATUS 檢查是否還有資料
		WHILE @@FETCH_STATUS = 0
			BEGIN
			-- 處理資料
			Insert INTO [iDataCenter].[dbo].[tbWksItem](
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
				  ,[ModifyTime]
			)
			VALUES( newId()--Newid()
				  ,@C1NewCustviewId
				  ,'24159CDB-714A-4BE9-9BBD-36863D7D2935'
				  --,[WksItemKindId]
				  ,'3'
				  ,@CustViewPublishId
				  ,NULL
				  ,NULL
				  ,@CustViewAccountId
				  ,@tbEmemo
				  ,'1'
				  ,'Init'
				  ,GETDATE()
				  ,'Sys'
				  ,GetDATE()
				  );
				print '分享者:'+@CustViewEmpNo;

  				EXEC [iOpen].[dbo].[SetupUnifiedSecurity]
					@UserName = @CustViewEmpNo,
					@StartDate = '',
					@EndDate = '',
					@IsADUser = 1,
					@IsOwner = 0,
					@IsnonDate = 1,
					@CheckDate = @isShareDT,
					@CustomView ='S',
					@DBName = @tbDBName,
					@ViewName = @RetbResNo,
					--REPLACE(@tbResName, '.', '_'),
					@DisplayViewName = '',
					@Source = @C1NewCustviewId,
					@inIsEnable = '6';

				-- 擷取下一筆
				FETCH NEXT FROM CustView_Curr INTO @CustViewAccountId,@CustViewEmpNo;
			END

		-- 關閉游標
		CLOSE CustView_Curr

		-- 釋放游標
		DEALLOCATE CustView_Curr
		FETCH NEXT FROM CustViewGroup_Curr INTO @CustViewGroup;
		END
		
			-- 關閉游標
		CLOSE CustViewGroup_Curr

		-- 釋放游標
		DEALLOCATE CustViewGroup_Curr
	END
			
        -- 提交交易
        COMMIT TRANSACTION;
        
        SET @EndTime = GETDATE();

        -- 回傳成功訊息
        SELECT 
            '資料平台初始化完成' AS Result,
            '成功' AS Status,
            @StartTime AS StartTime,
            @EndTime AS EndTime,
            DATEDIFF(SECOND, @StartTime, @EndTime) AS ExecutionTimeSeconds;
            
    END TRY
    BEGIN CATCH
        -- 發生錯誤時回滾交易
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        -- 獲取錯誤資訊
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState = ERROR_STATE();
        
        -- 記錄錯誤日誌
        INSERT INTO ilog.dbo.ErrorLog (
            ErrorMessage,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorDateTime
        )
        VALUES (
            @ErrorMessage,
            @ErrorSeverity,
            @ErrorState,
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            GETDATE()
        );

        -- 回傳錯誤訊息
        SELECT 
            '資料平台初始化失敗' AS Result,
            '失敗' AS Status,
            @ErrorMessage AS ErrorMessage,
            @StartTime AS StartTime,
            GETDATE() AS EndTime;
            
        -- 拋出錯誤給呼叫程序
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;
GO


