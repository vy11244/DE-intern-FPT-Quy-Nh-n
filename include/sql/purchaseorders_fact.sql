SELECT [PurchaseOrderID]
      ,[SupplierID]
      ,[OrderDate]
      ,[DeliveryMethodID]
      ,[ContactPersonID]
      ,[ExpectedDeliveryDate]
      ,[SupplierReference]
      ,[IsOrderFinalized]
      ,[Comments]
      ,[InternalComments]
      ,[LastEditedBy]
      ,CAST([LastEditedWhen] AS VARCHAR(30)) AS [LastEditedWhen]
FROM {table_name}
WHERE CAST([LastEditedWhen] AS datetime2(0)) >= '{last_execution_date}'