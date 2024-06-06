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
      ,CAST (PickingCompletedWhen AS VARCHAR(30)) AS PickingCompletedWhen
      ,[LastEditedBy]
      ,CAST (LastEditedWhen AS VARCHAR(30)) AS LastEditedWhen
FROM {table_name}
WHERE CAST([LastEditedWhen] AS datetime2(0)) >= '{last_execution_date}'