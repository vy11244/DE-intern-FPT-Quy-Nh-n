MERGE INTO MOVEMENT.MOVEMENT_SILVER.MOVEMENT tgt
USING (
  SELECT
    A.TRANSACTIONOCCURREDWHEN AS DateKey,
    si.STOCKITEMKEY AS StockItemKey,
    COALESCE(c.CUSTOMERKEY, '0') AS CustomerKey,
    COALESCE(s.SUPPLIERKEY, '0') AS SupplierKey,
    COALESCE(tt.TRANSACTIONTYPEKEY, '0') AS TransactionTypeKey,
    COALESCE(A.STOCKITEMTRANSACTIONID, 0) AS WWIStockItemTransactionID,
    COALESCE(A.INVOICEID, 0) AS WWIInvoiceID,
    COALESCE(A.PURCHASEORDERID, 0) AS WWIPurchaseOrderID,
    COALESCE(A.QUANTITY, 0) AS Quantity,
    NULL AS LineageKey
  FROM MOVEMENT.MOVEMENT_BRONZE.STOCKITEMTRANSACTIONS AS A
  LEFT JOIN MOVEMENT.MOVEMENT_SILVER.CUSTOMER AS c ON c.WWICUSTOMERID = A.CUSTOMERID
    AND A.LASTEDITEDWHEN > c.VALIDFROM
    AND A.LASTEDITEDWHEN <= c.VALIDTO
  LEFT JOIN MOVEMENT.MOVEMENT_SILVER.STOCKITEM AS si ON si.WWISTOCKITEMID = A.STOCKITEMID
    AND A.LASTEDITEDWHEN > si.VALIDFROM
    AND A.LASTEDITEDWHEN <= si.VALIDTO
  LEFT JOIN MOVEMENT.MOVEMENT_SILVER.TRANSACTIONTYPES AS tt ON tt.WWITRANSACTIONTYPEID = A.TRANSACTIONTYPEID
    AND A.LASTEDITEDWHEN > tt.VALIDFROM
    AND A.LASTEDITEDWHEN <= tt.VALIDTO
  LEFT JOIN MOVEMENT.MOVEMENT_SILVER.SUPPLIER AS s ON s.WWISUPPLIERID = A.SUPPLIERID
    AND A.LASTEDITEDWHEN > s.VALIDFROM
    AND A.LASTEDITEDWHEN <= s.VALIDTO
) AS src
ON tgt.WWIStockItemTransactionID = src.WWIStockItemTransactionID
WHEN MATCHED THEN
  UPDATE SET
    tgt.DateKey = src.DateKey,
    tgt.StockItemKey = src.StockItemKey,
    tgt.CustomerKey = src.CustomerKey,
    tgt.SupplierKey = src.SupplierKey,
    tgt.TransactionTypeKey = src.TransactionTypeKey,
    tgt.WWIStockItemTransactionID = src.WWIStockItemTransactionID,
    tgt.WWIInvoiceID = src.WWIInvoiceID,
    tgt.WWIPurchaseOrderID = src.WWIPurchaseOrderID,
    tgt.Quantity = src.Quantity,
    tgt.LineageKey = src.LineageKey
WHEN NOT MATCHED AND src.WWIStockItemTransactionID NOT IN (SELECT WWIStockItemTransactionID FROM MOVEMENT.MOVEMENT_SILVER.MOVEMENT) THEN
  INSERT (
    DateKey,
    StockItemKey,
    CustomerKey,
    SupplierKey,
    TransactionTypeKey,
    WWIStockItemTransactionID,
    WWIInvoiceID,
    WWIPurchaseOrderID,
    Quantity,
    LineageKey
  )
  VALUES (
    src.DateKey,
    src.StockItemKey,
    src.CustomerKey,
    src.SupplierKey,
    src.TransactionTypeKey,
    src.WWIStockItemTransactionID,
    src.WWIInvoiceID,
    src.WWIPurchaseOrderID,
    src.Quantity,
    src.LineageKey
  );

