SELECT [OrderID]
      ,[CustomerID]
      ,[SalespersonPersonID]
      ,[PickedByPersonID]
      ,[ContactPersonID]
      ,[BackorderOrderID]
      ,[OrderDate]
      ,[ExpectedDeliveryDate]
      ,[CustomerPurchaseOrderNumber]
      ,[IsUndersupplyBackordered]
      ,[Comments]
      ,[DeliveryInstructions]
      ,[InternalComments]
      ,CONVERT(VARCHAR(30), [PickingCompletedWhen], 121) AS PickingCompletedWhen
      ,[LastEditedBy]
      ,CAST ([LastEditedWhen] AS VARCHAR(30)) AS LastEditedWhen
FROM {table_name};