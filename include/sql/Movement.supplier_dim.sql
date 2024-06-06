/*
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: MOVEMENT
     Contributed Bronze Table: MOVEMENT.MOVEMENT_BRONZE.SUPPLIERS,
     MOVEMENT.MOVEMENT_BRONZE.SUPPLIERCATEGORIES, MOVEMENT.MOVEMENT_BRONZE.PEOPLE
     Silver table: MOVEMENT.MOVEMENT_SILVER.SUPPLIER
     Done by: DuyNVT4
     Last update when: 2024-03-21 15:20:00.0000000
     Update content: add cmt 
*/


TRUNCATE TABLE MOVEMENT.MOVEMENT_SILVER.SUPPLIER;
-- This statement clears out any existing data in the silver table if it exists.
 
-- This part begins the insertion of data into the silver table.

INSERT INTO MOVEMENT.MOVEMENT_SILVER.SUPPLIER (
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
    CASE WHEN T1.ValidFrom < T2.ValidFrom THEN T2.ValidFrom ELSE T1.ValidFrom END AS VALIDFROM,
    CASE WHEN T1.ValidTo < T2.ValidTo THEN T1.ValidTo ELSE T2.ValidTo END AS VALIDTO
   
FROM
    MOVEMENT.MOVEMENT_BRONZE.SUPPLIERS AS T1 
    left join  MOVEMENT.MOVEMENT_BRONZE.SUPPLIERCATEGORIES AS T2 
    ON T1.SUPPLIERCATEGORYID = T2.SUPPLIERCATEGORYID
    and T1.VALIDFROM < T2.VALIDTO
    and T1.VALIDTO > T2.VALIDFROM ),
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
    CASE WHEN a.ValidFrom < T3.ValidFrom THEN T3.ValidFrom ELSE a.ValidFrom END AS VALIDFROM,
    CASE WHEN a.ValidTo < T3.ValidTo THEN a.ValidTo ELSE T3.ValidTo END AS VALIDTO
from a
left join MOVEMENT.MOVEMENT_BRONZE.PEOPLE AS T3 
on a.PRIMARYCONTACTPERSONID = T3.PERSONID
and a.VALIDFROM < T3.VALIDTO
and a.VALIDTO > T3.VALIDFROM)
-- Main query: combinate all column again and add SupplierKey by concating WWISupplierID and ValidFrom
select concat(b.WWISupplierID, TO_CHAR(b.ValidFrom,'YYYYMMDDHH24MISSFF7')) as SupplierKey,
    b.WWISupplierID,
    b.Supplier,
    b.CATEGORY,
    b.PrimaryContact,
    b.SupplierReference,    
    b.PaymentDays,
    b.PostalCode,
    b.VALIDFROM,
    b.VALIDTO
from b