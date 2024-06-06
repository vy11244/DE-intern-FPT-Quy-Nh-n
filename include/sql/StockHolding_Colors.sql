(SELECT [ColorID]
      ,[ColorName]
      ,[LastEditedBy]
      ,CAST (ValidFrom AS VARCHAR(30)) AS ValidFrom
      ,CAST (ValidTo AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Warehouse].[Colors_Archive]
WHERE ValidFrom <> ValidTo)
union all
(SELECT [ColorID]
      ,[ColorName]
      ,[LastEditedBy]
      ,CAST (ValidFrom AS VARCHAR(30)) AS ValidFrom
      ,CAST (ValidTo AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Warehouse].[Colors]
WHERE ValidFrom <> ValidTo)