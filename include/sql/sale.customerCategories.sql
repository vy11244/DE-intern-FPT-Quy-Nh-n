(SELECT  [CustomerCategoryID]
      ,[CustomerCategoryName]
      ,[LastEditedBy]
      ,CAST(ValidFrom AS VARCHAR(30)) AS ValidFrom
      ,CAST(ValidTo AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Sales].[CustomerCategories_Archive]
WHERE ValidFrom <> ValidTo)
union all
(SELECT  [CustomerCategoryID]
      ,[CustomerCategoryName]
      ,[LastEditedBy]
      ,CAST(ValidFrom AS VARCHAR(30)) AS ValidFrom
      ,CAST(ValidTo AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Sales].[CustomerCategories]
WHERE ValidFrom <> ValidTo)