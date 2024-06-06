(SELECT [SupplierCategoryID]
      ,[SupplierCategoryName]
      ,[LastEditedBy]
      ,CAST([ValidFrom] AS VARCHAR(30)) AS ValidFrom
      ,CAST([ValidTo] AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Purchasing].[SupplierCategories_Archive]
WHERE ValidFrom <> ValidTo)
union all
(SELECT [SupplierCategoryID]
      ,[SupplierCategoryName]
      ,[LastEditedBy]
      ,CAST([ValidFrom] AS VARCHAR(30)) AS ValidFrom
      ,CAST([ValidTo] AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Purchasing].[SupplierCategories]
WHERE ValidFrom <> ValidTo)