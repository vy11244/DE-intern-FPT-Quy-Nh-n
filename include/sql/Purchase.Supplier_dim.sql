/* 
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: PURCHASE
     Contributed Bronze Table: PURCHASE.PURCHASE_BRONZE.PEOPLE,PURCHASE.PURCHASE_BRONZE.SUPPLIERS,
     PURCHASE.PURCHASE_BRONZE.SUPPLIERCATEGORY
     Silver table: PURCHASE.PURCHASE_SILVER.SUPPLIER
     Done by: TanBN2
     Last update when: 2024-03-22 09:21:00.0000000
     Update content: Edit primary key format and query template
*/
TRUNCATE TABLE IF EXISTS PURCHASE.PURCHASE_SILVER.SUPPLIER;-- This statement clears out any existing data in the silver table if it exists.

-- This part begins the insertion of data into the silver table.
INSERT INTO PURCHASE.PURCHASE_SILVER.SUPPLIER (
    SupplierKey,
    WWISupplierID,
    Supplier,
    CATEGORY,
    PrimaryContact,
    SupplierReference,
    PaymentDays,
    POSTALCODE,
    VALIDFROM,
    VALIDTO
)
-- There are 2 temp queries that support main query
/*
  This temp table called a: extract all neccessary data from Suppliers table and extract more column from SUPPLIERCATEGORIES table
*/
with a as
(SELECT
    T1.SUPPLIERID,
    T1.SUPPLIERNAME,
    T1.PRIMARYCONTACTPERSONID,
    T2.SUPPLIERCATEGORYNAME,   
    T1.SUPPLIERREFERENCE,
    T1.PAYMENTDAYS,
    T1.deliverypostalcode,
    case when T1.SUPPLIERCATEGORYID is not null then (
        CASE WHEN T1.ValidFrom < T2.ValidFrom THEN T2.ValidFRom ELSE T1.ValidFrom END
    )
    ELSE T1.ValidFrom END as ValidFrom,
    case when T1.SUPPLIERCATEGORYID is not null then (
        CASE WHEN T1.ValidTo < T2.ValidTo THEN T1.ValidTo ELSE T2.ValidTo END
    )
    ELSE T1.ValidTo END as ValidTo
   
FROM
    PURCHASE.PURCHASE_BRONZE.SUPPLIERS AS T1 
    left join  PURCHASE.PURCHASE_BRONZE.SUPPLIERCATEGORIES AS T2 
    ON T1.SUPPLIERCATEGORYID = T2.SUPPLIERCATEGORYID
    and T1.VALIDFROM < T2.VALIDTO
    and T1.VALIDTO > T2.VALIDFROM 
),
/*
   With temp query called b: It is used to extract more column: 
   FULLNAME AS PrimaryContact from People table 
*/   
b as (select 
    a.SUPPLIERID as WWISupplierID,
    a.SUPPLIERNAME as Supplier,
    a.SUPPLIERCATEGORYNAME as CATEGORY,
    T3.FULLNAME AS PrimaryContact,
    a.SUPPLIERREFERENCE as SupplierReference,
    a.PAYMENTDAYS as PaymentDays,
    a.deliverypostalcode as PostalCode,
    case when a.PRIMARYCONTACTPERSONID is not null then (
        CASE WHEN a.ValidFrom < T3.ValidFrom THEN T3.ValidFRom ELSE a.ValidFrom END
    )
    ELSE a.ValidFrom END as ValidFrom,
    case when a.PRIMARYCONTACTPERSONID is not null then (
        CASE WHEN a.ValidTo < T3.ValidTo THEN a.ValidTo ELSE T3.ValidTo END
    )
    ELSE a.ValidTo END as ValidTo
    
from a
left join PURCHASE.PURCHASE_BRONZE.PEOPLE AS T3 
on a.PRIMARYCONTACTPERSONID = T3.PERSONID
and a.VALIDFROM < T3.VALIDTO
and a.VALIDTO > T3.VALIDFROM)

-- Main query: combinate all column again and add SupplierKey by concating WWISupplierID and ValidFrom
select 
    concat(b.WWISupplierID, TO_CHAR(b.ValidFrom,'YYYYMMDDHH24MISSFF7')) as SupplierKey,
    b.WWISupplierID,
    b.Supplier,
    b.CATEGORY,
    b.PrimaryContact,
    b.SupplierReference,
    b.PaymentDays,
    b.PostalCode,
    b.ValidFrom,
    b.ValidTo
from b