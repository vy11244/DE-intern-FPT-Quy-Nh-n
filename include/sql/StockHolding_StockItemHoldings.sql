SELECT [StockItemID]
      ,[QuantityOnHand]
      ,[BinLocation]
      ,[LastStocktakeQuantity]
      ,[LastCostPrice]
      ,[ReorderLevel]
      ,[TargetStockLevel]
      ,CAST([LastEditedWhen] AS VARCHAR(30)) AS LastEditedWhen
FROM {table_name};