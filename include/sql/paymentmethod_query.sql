(SELECT [PaymentMethodID]
      ,[PaymentMethodName]
      ,[LastEditedBy]
      ,CAST([ValidFrom] AS VARCHAR(30)) AS ValidFrom
      ,CAST([ValidTo] AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Application].[PaymentMethods_Archive]
WHERE ValidFrom <> ValidTo)
union all
(SELECT [PaymentMethodID]
      ,[PaymentMethodName]
      ,[LastEditedBy]
      ,CAST([ValidFrom] AS VARCHAR(30)) AS ValidFrom
      ,CAST([ValidTo] AS VARCHAR(30)) AS ValidTo
FROM [WideWorldImporters].[Application].[PaymentMethods]
WHERE ValidFrom <> ValidTo)