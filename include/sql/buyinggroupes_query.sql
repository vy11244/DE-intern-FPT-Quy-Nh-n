(SELECT  [BuyingGroupID]
      ,[BuyingGroupName]   
      ,[LastEditedBy]   
      ,CAST([ValidFrom] AS VARCHAR(30)) AS [ValidFrom]
      ,CAST([ValidTo] AS VARCHAR(30)) AS [ValidTo]
FROM [WideWorldImporters].[Sales].[BuyingGroups_Archive]
WHERE ValidFrom <> ValidTo)
union all
(SELECT  [BuyingGroupID]
      ,[BuyingGroupName]   
      ,[LastEditedBy]   
      ,CAST([ValidFrom] AS VARCHAR(30)) AS [ValidFrom]
      ,CAST([ValidTo] AS VARCHAR(30)) AS [ValidTo]
FROM [WideWorldImporters].[Sales].[BuyingGroups]
WHERE ValidFrom <> ValidTo)