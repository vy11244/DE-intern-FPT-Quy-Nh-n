SELECT [StockItemID]
      ,[QuantityOnHand]
      ,[BinLocation]
      ,[LastStocktakeQuantity]
      ,[LastCostPrice]
      ,[ReorderLevel]
      ,[TargetStockLevel]
      ,CAST ([LastEditedWhen] AS VARCHAR(30)) AS LastEditedWhen
FROM {table_name}
WHERE CAST([LastEditedWhen] AS datetime2(0)) >= '{last_execution_date}'