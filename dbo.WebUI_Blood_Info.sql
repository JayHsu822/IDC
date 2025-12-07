USE [iDataCenter]
GO

/****** Object:  StoredProcedure [dbo].[WebUI_Blood_Info]    Script Date: 2025/12/7 上午 11:05:33 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



































/*
=============================================================================
  描述：[血緣圖功能:1.產生血緣圖, 2.血緣圖資料匯出, 3.血緣圖分享清單]
  
  版本記錄：
  ------------------------------------------------------------------------------
  版本   日期          作者        描述
  1.0    2024-12-10    Jay Hsu     初始版本
  1.1    2024-12-13    Jay Hsu     修改血緣圖分享資訊產出方式
  1.11   2025-02-27    Jay Hsu     功能2,3加入tbcustview enable = '1'及tbpublish kind = '1'
  1.2    2025-05-29    Jay Hsu     新增Owner一對多機制(table:tbcustview->vcustview)
  
  參數說明：
  ------------------------------------------------------------------------------
  @CustViewId NVARCHAR(36)   自訂View Id       必填
  @AccountId  NVARCHAR(36)   平台登入使用者Id  必填
  @Kind       NVARCHAR(2)    血緣圖功能        必填，預設值=1:血緣圖 2:血緣圖資料匯出  3:分享清單          
  
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
  EXEC WebUI_Blood_Info 
    @CustViewid = 'FC4AA5EA-91EE-4B68-B42E-4109A6B654CE',
	@AccountId = '4C337C6E-29F9-460D-982C-D770D24EC385',
	@Kind = '1';
  
  注意事項：
  ------------------------------------------------------------------------------
  1. [重要注意事項1]
  2. [效能考量說明]
  3. [業務邏輯特殊處理]
=============================================================================
*/
CREATE OR ALTER               PROCEDURE [dbo].[WebUI_Blood_Info]
    @CustViewId NVARCHAR(36),
    @AccountId NVARCHAR(36),
	@Kind NVARCHAR(2) -- 1.血緣圖, 2.血緣圖資料匯出, 3.分享清單
AS
BEGIN
    DECLARE @MasterId VARCHAR(MAX);
	DECLARE @OwnAccountId VARCHAR(36);
    SET NOCOUNT ON;

	EXEC [iLog].[dbo].[sp_WebUI_Blood_Info_WithLogging]
            @CustViewid, @AccountId, @Kind

	select @MasterId = [dbo].[fnGetiDataCenterInfo]('8',@CustViewId,'1','');

	select @OwnAccountId = cv.AccountId
	FROM [dbo].[tbCustViewAgent] cva
	INNER JOIN tbCustView cv
	ON cva.CustViewId = cv.id and cv.enable = '1'
	WHERE cva.AccountId = @AccountId and cva.CustViewId = @CustViewId;

	print 'OwnAccountId:'+@OwnAccountId;
	print 'AccountId:'+@AccountId;

	if @Kind = '1'
	BEGIN
		WITH RecursiveCTE AS (
			-- 取得根節點(Layer = 1的資料)
			SELECT 
				Id,
				ParentId,
				MasterId,
				Layer,
				LayerNo,
				ViewNo,
				ViewName,
				AccountId,
				enable,
				CAST(ViewName AS VARCHAR(MAX)) AS Hierarchy,
				CAST(LayerNo AS VARCHAR(10)) AS LayerPath
			FROM tbCustView
			WHERE Layer = 1 and Enable = '1'
			UNION ALL
			-- 遞迴取得子節點
			SELECT 
				t.Id,
				t.ParentId,
				t.MasterId,
				t.Layer,
				t.LayerNo,
				t.ViewNo,
				t.ViewName,
				t.AccountId,
				t.enable,
				CAST(r.Hierarchy + ' -> ' + t.ViewName AS VARCHAR(MAX)),
				CAST(r.LayerPath + '.' + CAST(t.LayerNo AS VARCHAR(10)) AS VARCHAR(10))
			FROM tbCustView t
			INNER JOIN RecursiveCTE r ON t.ParentId = r.Id
			--WHERE t.OwnerId is null
		),
		BaseData AS (
			SELECT 
				c.Id,
				c.ParentId,
				tr.Kind,
				c.ViewName,
				c.ViewNo,
				c.Layer,
				t.Id AS OwnAccountId,
				t.SecontNickNm AS OwnSecontNickNm,
				t.EmpNo,
				t.Notes AS E_EmpName,
				/*CONCAT(
				UPPER(LEFT(SUBSTRING(t.EmpEmail, 1, CHARINDEX('_', t.EmpEmail) - 1), 1)),
				LOWER(SUBSTRING(t.EmpEmail, 2, CHARINDEX('_', t.EmpEmail) - 2)),
				' ',
				UPPER(SUBSTRING(t.EmpEmail, CHARINDEX('_', t.EmpEmail) + 1, 1)),
				LOWER(SUBSTRING(t.EmpEmail, CHARINDEX('_', t.EmpEmail) + 2, CHARINDEX('@', t.EmpEmail) - CHARINDEX('_', t.EmpEmail) - 2))) AS E_EmpName,*/
				t.EmpName AS EmpName,
				t.Ext,
				t.EmpEmail,
				CASE WHEN c.AccountId = ISNULL(@OwnAccountId, @AccountId) THEN '1'
					 WHEN w.AccountId = ISNULL(@OwnAccountId, @AccountId) THEN '1' ELSE '0' END AS IsEnable,
				isnull(p.Name,'NA') AS GroupName,
				st.ReviewUnit,
				st.Notes AS User_EmpName,
				/*CONCAT(
				UPPER(LEFT(SUBSTRING(st.EmpEmail, 1, CHARINDEX('_', st.EmpEmail) - 1), 1)),
				LOWER(SUBSTRING(st.EmpEmail, 2, CHARINDEX('_', st.EmpEmail) - 2)),
				' ',
				UPPER(SUBSTRING(st.EmpEmail, CHARINDEX('_', st.EmpEmail) + 1, 1)),
				LOWER(SUBSTRING(st.EmpEmail, CHARINDEX('_', st.EmpEmail) + 2, CHARINDEX('@', st.EmpEmail) - CHARINDEX('_', st.EmpEmail) - 2))) AS User_EmpName,*/
				--st.EmpName AS User_EmpName,
				WTR.MAX_REVISE_DT
			FROM RecursiveCTE c
			LEFT JOIN tbSysAccount t ON c.AccountId = t.Id
			LEFT JOIN tbPublish p ON c.Id = p.CustViewId and p.Enable = '1' and p.Kind = '1'
			LEFT JOIN tbWksItem w ON p.Id = w.PublishId and w.Enable = '1'
			--LEFT JOIN [dbo].[tbCustViewAgent] cva on 
			LEFT JOIN tbSysAccount st ON w.AccountId = st.Id
			LEFT JOIN tbRes tr ON c.MasterId = tr.Id AND tr.Enable = '1'
			LEFT JOIN iPJT_FinWeb_UI_TB_REVISE_DT WTR ON tr.ResNo = WTR.TB_NM
			WHERE c.MasterId = @MasterId 
			AND c.enable = '1' 
		),
		UserGroups AS (
			SELECT
				Id,
				ParentId,
				Kind,
				OwnAccountId,
				OwnSecontNickNm,
				EmpNo,
				EmpName,
				E_EmpName,
				Ext,
				EmpEmail,
				ViewName,
				ViewNo,
				Layer,
				MAX(IsEnable) AS IsEnable,
				GroupName,
				ReviewUnit,
				MAX_REVISE_DT,
				STRING_AGG(User_EmpName, '/') WITHIN GROUP (ORDER BY User_EmpName) AS GroupedUsers,
				ROW_NUMBER() OVER (PARTITION BY Id ORDER BY ReviewUnit) as ReviewUnitOrder
			FROM BaseData
			WHERE GroupName IS NOT NULL
			GROUP BY Id, ParentId, kind, OwnAccountId, OwnSecontNickNm, EmpNo, EmpName, E_EmpName, Ext, EmpEmail, ViewName, ViewNo, Layer, GroupName, ReviewUnit, MAX_REVISE_DT
		),
		GroupedData AS (
			SELECT DISTINCT 
				Id,
				ParentId,
				kind,
				OwnAccountId,
				OwnSecontNickNm,
				EmpNo,
				EmpName,
				E_EmpName,
				Ext,
				EmpEmail,
				ViewName,
				ViewNo,
				Layer,
				MAX(IsEnable) AS IsEnable,
				GroupName,
				MAX_REVISE_DT,
				STRING_AGG(
					ReviewUnit + '|' + GroupedUsers,
					'#'
				) WITHIN GROUP (ORDER BY ReviewUnitOrder) AS FormattedUsers
			FROM UserGroups
			GROUP BY Id, ParentId, kind, OwnAccountId, OwnSecontNickNm, EmpNo, EmpName, E_EmpName, Ext, EmpEmail, ViewName, ViewNo, Layer,  GroupName, MAX_REVISE_DT
		)
		SELECT 
			G.id,
			G.ParentId,
			CASE WHEN G.kind = '2' THEN 'DATA' ELSE 'MD' END AS TableType,
			G.OwnAccountId,
			CASE WHEN @OwnAccountId IS NOT NULL AND @AccountId IS NOT NULL THEN @AccountId ELSE G.OwnAccountId END AS AgentId,
			G.OwnSecontNickNm AS Owner_Dept,
			G.EmpNo AS Owner_No,
			G.E_EmpName AS E_Owner_Name,
			G.EmpName AS Owner_Name,
			G.Ext AS Owner_Ext,
			G.EmpEmail AS Owner_EmpEmail,
			G.ViewName+CASE WHEN Len(G.ViewNo)>0 THEN '_'+G.ViewNo ELSE '' END AS Table_Name,
			G.ViewNo AS Alias,
			G.Layer,
			G.IsEnable,
			G.GroupName,
			CASE WHEN G.Layer = '3' THEN U.ReviewUnit+'|'+STRING_AGG(U.Notes, '/') WITHIN GROUP (ORDER BY B.AccountId)  ELSE G.FormattedUsers END AS Shared_Users,
			FORMAT(G.MAX_REVISE_DT, 'yyyy/MM/dd HH:mm') AS Update_DT
		FROM GroupedData G
		LEFT JOIN [dbo].[tbCustViewAgent] A
		ON G.id = A.CustViewId AND CASE WHEN @OwnAccountId IS NOT NULL THEN @AccountId ELSE G.OwnAccountId END = A.AccountId AND A.StartTime >= GetDate() AND A.EndTime <= GetDate()
		LEFT JOIN [dbo].[tbCustViewAgent] B
		ON G.id = B.CustViewId AND B.Kind = '2' AND GETDATE() BETWEEN B.StartTime AND B.EndTime
		LEFT JOIN [dbo].[tbSysAccount] U
		ON B.AccountId = U.id AND U.Enable = '1'
		GROUP BY G.id, G.ParentId, G.kind, G.OwnAccountId, G.OwnSecontNickNm, G.EmpNo, G.EmpName, G.E_EmpName, G.Ext, G.EmpEmail, G.ViewName, G.ViewNo, G.Layer,  G.GroupName, G.IsEnable, G.FormattedUsers, G.MAX_REVISE_DT, A.AccountId, U.ReviewUnit
		ORDER BY ViewName, IsEnable DESC;
	END
	ELSE IF @Kind='2'
	BEGIN
		WITH RecursiveCTE AS (
			-- 取得根節點(Layer = 1的資料)
			SELECT 
				Id,
				ParentId,
				MasterId,
				Layer,
				LayerNo,
				ViewNo,
				ViewName,
				AccountId,
				enable,
				CAST(ViewName AS VARCHAR(MAX)) AS Hierarchy,
				CAST(LayerNo AS VARCHAR(10)) AS LayerPath
			FROM tbCustView
			WHERE Layer = 1 and enable = '1'
			UNION ALL
			-- 遞迴取得子節點
			SELECT 
				t.Id,
				t.ParentId,
				t.MasterId,
				t.Layer,
				t.LayerNo,
				t.ViewNo,
				t.ViewName,
				t.AccountId,
				t.enable,
				CAST(r.Hierarchy + ' -> ' + t.ViewName AS VARCHAR(MAX)),
				CAST(r.LayerPath + '.' + CAST(t.LayerNo AS VARCHAR(10)) AS VARCHAR(10))
			FROM tbCustView t
			INNER JOIN RecursiveCTE r ON t.ParentId = r.Id
			--WHERE t.OwnerId is null
		)
		SELECT 
			--c.Id,
			--c.ParentId,
			--c.Layer,
			CASE WHEN c.Layer = '1' THEN 'Source'
				 WHEN c.Layer = '2' THEN 'Owner Custom'
				 WHEN c.Layer = '3' THEN 'Custom from shared' END Table_Type,
			c.ViewName+CASE WHEN Len(c.ViewNo)>0 THEN '_'+c.ViewNo ELSE '' END AS Table_Name,
			--ISNULL(c.ViewNo,'') AS Alias,
			--t.DeptCode AS Owner_Dept,
			t.ReviewUnit AS Owner_RviewUnit,
			t.DeptCode AS Owner_Dept,
			t.EmpNo AS Owner_No,
			t.Notes AS Owner_Name,
			/*CONCAT(
			UPPER(LEFT(SUBSTRING(t.EmpEmail, 1, CHARINDEX('_', t.EmpEmail) - 1), 1)),
			LOWER(SUBSTRING(t.EmpEmail, 2, CHARINDEX('_', t.EmpEmail) - 2)),
			' ',
			UPPER(SUBSTRING(t.EmpEmail, CHARINDEX('_', t.EmpEmail) + 1, 1)),
			LOWER(SUBSTRING(t.EmpEmail, CHARINDEX('_', t.EmpEmail) + 2, CHARINDEX('@', t.EmpEmail) - CHARINDEX('_', t.EmpEmail) - 2))) AS Owner_Name,*/
			--t.EmpName as Owner_Name,
			ISNULL(p.Name,'') AS Shared_Group,
			--st.DeptCode as User_Dept,
			ISNULL(st.ReviewUnit,'') as Shared_ReviewUnit,
			ISNULL(st.DeptCode,'') as Shared_Dept,
			st.EmpNo As Shared_No,
			st.Notes AS Shared_Name
			/*CONCAT(
			UPPER(LEFT(SUBSTRING(st.EmpEmail, 1, CHARINDEX('_', st.EmpEmail) - 1), 1)),
			LOWER(SUBSTRING(st.EmpEmail, 2, CHARINDEX('_', st.EmpEmail) - 2)),
			' ',
			UPPER(SUBSTRING(st.EmpEmail, CHARINDEX('_', st.EmpEmail) + 1, 1)),
			LOWER(SUBSTRING(st.EmpEmail, CHARINDEX('_', st.EmpEmail) + 2, CHARINDEX('@', st.EmpEmail) - CHARINDEX('_', st.EmpEmail) - 2))) as Shared_Name*/
			--ISNULL(st.EmpName,'') as Shared_Name--,
			-- Layer 分類欄位
			/*CASE 
				WHEN c.Layer = 1 THEN CONCAT(c.ViewName, '(', c.ViewNo, ')')
				ELSE NULL 
			END AS Source,
			CASE 
				WHEN c.Layer = 2 THEN CONCAT(c.ViewName, '(', c.ViewNo, ')')
				ELSE NULL 
			END AS Custom,
			CASE 
				WHEN c.Layer = 3 THEN CONCAT(c.ViewName, '(', c.ViewNo, ')')
				ELSE NULL 
			END AS ShareCustom*/
		FROM RecursiveCTE c
		LEFT JOIN tbSysAccount t 
			ON c.AccountId = t.Id
		LEFT JOIN tbPublish p 
			ON c.Id = p.CustViewId and p.Enable = '1' and p.kind = '1'
		LEFT JOIN vWksItem w 
			ON p.Id = w.PublishId and w.Enable = '1'
		LEFT JOIN tbSysAccount st 
			ON w.AccountId = st.Id
		WHERE c.MasterId = @MasterId 
			and c.enable = '1'
		ORDER BY LayerPath;
/*		WITH RecursiveCTE AS (
			-- 取得根節點(Layer = 1的資料)
			SELECT 
				Id,
				ParentId,
				MasterId,
				Layer,
				LayerNo,
				ViewNo,
				ViewName,
				AccountId,
				enable,
				CAST(ViewName AS VARCHAR(MAX)) AS Hierarchy,
				CAST(LayerNo AS VARCHAR(10)) AS LayerPath
			FROM vCustView
			WHERE Layer = 1

			UNION ALL

			-- 遞迴取得子節點
			SELECT 
				t.Id,
				t.ParentId,
				t.MasterId,
				t.Layer,
				t.LayerNo,
				t.ViewNo,
				t.ViewName,
				t.AccountId,
				t.enable,
				CAST(r.Hierarchy + ' -> ' + t.ViewName AS VARCHAR(MAX)),
				CAST(r.LayerPath + '.' + CAST(t.LayerNo AS VARCHAR(10)) AS VARCHAR(10))
			FROM vCustView t
			INNER JOIN RecursiveCTE r ON t.ParentId = r.Id

		)
		SELECT 
			c.Id,
			ParentId,
			--MasterId,
			--Layer,
			--LayerNo,
			--ViewNo,
			--ViewName,
			Hierarchy,
			p.Name AS GroupName,
			--CASE WHEN c.AccountId = @AccountId THEN '1'
				 --WHEN w.AccountId = @AccountId THEN '1' ELSE '0' END AS IsEnable,
			--c.AccountId AS OwnerAccountID,
			t.DeptCode AS Owener_Dept,
			t.EmpName as Owner_Name,
			--Upper(t.EmpNo) as Owner_EmpNo,
			--w.AccountId AS UserAccountID,
			st.DeptCode as User_Dept,
			st.EmpName as User_Name
			--,
			--Upper(st.EmpNo) AS User_EmpNo
			--,
			
			--,
			--LayerPath
		FROM RecursiveCTE c
		LEFT JOIN tbSysAccount t 
		  ON c.AccountId = t.Id
		LEFT JOIN tbPublish p 
		  ON c.Id = p.CustViewId and p.Enable = '1'
		LEFT JOIN tbWksItem w 
		  ON p.Id = w.PublishId and w.Enable = '1'
		LEFT JOIN tbSysAccount st 
		  ON w.AccountId = st.Id
		WHERE c.MasterId = @MasterId and c.enable = '1'
		ORDER BY LayerPath;*/
	END
	ELSE IF @Kind='3'
	BEGIN
		WITH RecursiveCTE AS (
			-- 取得根節點(Layer = 1的資料)
			SELECT 
				Id,
				ParentId,
				MasterId,
				Layer,
				LayerNo,
				ViewNo,
				ViewName,
				AccountId,
				enable,
				CAST(ViewName AS VARCHAR(MAX)) AS Hierarchy,
				CAST(LayerNo AS VARCHAR(10)) AS LayerPath
			FROM vCustView
			WHERE Layer = 1 and Enable = '1'
			UNION ALL
			-- 遞迴取得子節點
			SELECT 
				t.Id,
				t.ParentId,
				t.MasterId,
				t.Layer,
				t.LayerNo,
				t.ViewNo,
				t.ViewName,
				t.AccountId,
				t.enable,
				CAST(r.Hierarchy + ' -> ' + t.ViewName AS VARCHAR(MAX)),
				CAST(r.LayerPath + '.' + CAST(t.LayerNo AS VARCHAR(10)) AS VARCHAR(10))
			FROM vCustView t
			INNER JOIN RecursiveCTE r ON t.ParentId = r.Id
		)
		SELECT 
			--c.Id,
			--c.ParentId,
			--c.Layer,
			--CASE WHEN c.Layer = '1' THEN 'Source'
				-- WHEN c.Layer = '2' THEN 'Owner Custom'
				-- WHEN c.Layer = '3' THEN 'Custom from shared' END Table_Type,
			tr.ResNo AS Source_Table,
			c.ViewName+CASE WHEN len(c.ViewNo) > 0 THEN '_'+C.ViewNo ELSE '' END AS Table_Name,
			--c.ViewNo AS Alias,
			--t.DeptCode AS Owner_Dept,
			--t.SecontNickNm AS Owner_Dept,
			--t.EmpName as Owner_Name,
			--p.Name AS GroupName,
			--st.DeptCode as User_Dept,
			st.ReviewUnit as Shared_Factory,
			st.DeptCode as Shared_Dept,
			st.EmpName as Shared_Name,
			Upper(st.EmpNO) AS Shared_No,
			st.Notes AS Sheard_Note_Id
			/*CONCAT(
			UPPER(LEFT(SUBSTRING(st.EmpEmail, 1, CHARINDEX('_', st.EmpEmail) - 1), 1)),
			LOWER(SUBSTRING(st.EmpEmail, 2, CHARINDEX('_', st.EmpEmail) - 2)),
			' ',
			UPPER(SUBSTRING(st.EmpEmail, CHARINDEX('_', st.EmpEmail) + 1, 1)),
			LOWER(SUBSTRING(st.EmpEmail, CHARINDEX('_', st.EmpEmail) + 2, CHARINDEX('@', st.EmpEmail) - CHARINDEX('_', st.EmpEmail) - 2))) AS Sheard_Note_Id*/
			--,
			--st.Ext,
			--st.EmpEmail,
			--LEFT(c.Hierarchy, CHARINDEX('->', c.Hierarchy) - 2) AS Soure
			-- Layer 分類欄位
			/*CASE 
				WHEN c.Layer = 1 THEN CONCAT(c.ViewName, '(', c.ViewNo, ')')
				ELSE NULL 
			END AS Source,
			CASE 
				WHEN c.Layer = 2 THEN CONCAT(c.ViewName, '(', c.ViewNo, ')')
				ELSE NULL 
			END AS Custom,
			CASE 
				WHEN c.Layer = 3 THEN CONCAT(c.ViewName, '(', c.ViewNo, ')')
				ELSE NULL 
			END AS ShareCustom*/
		FROM RecursiveCTE c
		LEFT JOIN tbRes tr
			ON tr.id = c.MasterId and tr.Enable = '1'
		LEFT JOIN tbSysAccount t 
			ON c.AccountId = t.Id
		LEFT JOIN tbPublish p 
			ON c.Id = p.CustViewId and p.Enable = '1' and p.Kind ='1'
		LEFT JOIN tbWksItem w 
			ON p.Id = w.PublishId and w.Enable = '1' 
		LEFT JOIN tbSysAccount st 
			ON w.AccountId = st.Id
		WHERE c.MasterId = @MasterId 
			and c.enable = '1'
			and st.EmpName is not null
        GROUP BY tr.ResNo, c.ViewName, c.ViewNo, st.ReviewUnit, st.EmpName, st.EmpNo, st.EmpEmail, st.DeptCode, st.Notes;
	END
END
GO


