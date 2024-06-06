SELECT [OrderLineID]
      ,[OrderID]
      ,[StockItemID]
      ,[Description]
      ,[PackageTypeID]
      ,[Quantity]
      ,[UnitPrice]
      ,[TaxRate]
      ,[PickedQuantity]
      ,CONVERT(VARCHAR(30), [PickingCompletedWhen], 121) AS PickingCompletedWhen
      ,[LastEditedBy]
      ,CAST ([LastEditedWhen] AS VARCHAR(30)) AS LastEditedWhen
FROM {table_name};