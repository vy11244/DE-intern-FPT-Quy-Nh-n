(SELECT [TransactionTypeID]
      ,[TransactionTypeName]
      ,[LastEditedBy]
      ,CAST([ValidFrom] AS VARCHAR(30)) AS ValidFrom
      ,CAST([ValidTo] AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Application].[TransactionTypes_Archive]
WHERE ValidFrom <> ValidTo)
union all
(SELECT [TransactionTypeID]
      ,[TransactionTypeName]
      ,[LastEditedBy]
      ,CAST([ValidFrom] AS VARCHAR(30)) AS ValidFrom
      ,CAST([ValidTo] AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Application].[TransactionTypes]
WHERE ValidFrom <> ValidTo)