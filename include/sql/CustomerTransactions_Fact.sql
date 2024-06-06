SELECT [CustomerTransactionID]
      ,[CustomerID]
      ,[TransactionTypeID]
      ,[InvoiceID]
      ,[PaymentMethodID]
      ,[TransactionDate]
      ,[AmountExcludingTax]
      ,[TaxAmount]
      ,[TransactionAmount]
      ,[OutstandingBalance]
      ,[FinalizationDate]
      ,[IsFinalized]
      ,[LastEditedBy]
      ,CAST ([LastEditedWhen] AS VARCHAR(30)) AS LastEditedWhen
FROM {table_name}
WHERE CAST([LastEditedWhen] AS datetime2(0)) >= '{last_execution_date}'