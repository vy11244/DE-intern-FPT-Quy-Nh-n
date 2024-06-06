SELECT [StockItemTransactionID]
      ,[StockItemID]
      ,[TransactionTypeID]
      ,[CustomerID]
      ,[InvoiceID]
      ,[SupplierID]
      ,[PurchaseOrderID]
      ,[TransactionOccurredWhen]
      ,[Quantity]
      ,[LastEditedBy]
      ,CAST ([LastEditedWhen] AS VARCHAR(30)) AS LastEditedWhen
FROM {table_name};