(SELECT  [PackageTypeID]
      ,[PackageTypeName]
      ,[LastEditedBy]
      ,CAST([ValidFrom] AS VARCHAR(30)) AS ValidFrom
      ,CAST([ValidTo] AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Warehouse].[PackageTypes_Archive]
WHERE ValidFrom <> ValidTo)
union all
(SELECT  [PackageTypeID]
      ,[PackageTypeName]
      ,[LastEditedBy]
      ,CAST([ValidFrom] AS VARCHAR(30)) AS ValidFrom
      ,CAST([ValidTo] AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Warehouse].[PackageTypes]
WHERE ValidFrom <> ValidTo)