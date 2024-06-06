/*
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: Transaction 
     Contributed Bronze Table: TRANSACTION.TRANSACTION_BRONZE.PAYMENTMETHOD
     Silver table: TRANSACTION.TRANSACTION_SILVER.PAYMENTMETHOD
     Done by: KietNT19
     Last update when: 2024-03-15 06:38:00.0000000
     Update content: Edit primary key format, SCD Type 2 and query template
*/
TRUNCATE TABLE IF EXISTS TRANSACTION.TRANSACTION_SILVER.PAYMENTMETHOD;-- This statement clears out any existing data in the silver table if it exists.

-- This part begins the insertion of data into the silver table.
INSERT INTO TRANSACTION.TRANSACTION_SILVER.PAYMENTMETHOD (
    PAYMENTMETHODKEY,
    WWIPAYMENTMETHODID,
    PAYMENTMETHOD,
    VALIDFROM,
    VALIDTO
)
-- Main query: combinate all column again and add PAYMENTMETHODKEY by concating PAYMENTMETHODID and ValidFrom
select 
    concat(P.PAYMENTMETHODID, TO_CHAR(P.ValidFrom,'YYYYMMDDHH24MISSFF7')) as PAYMENTMETHODKEY,
    P.PAYMENTMETHODID as WWIPAYMENTMETHODID,
    P.PAYMENTMETHODNAME as PAYMENTMETHOD ,
    P.ValidFrom,
    P.ValidTo
from TRANSACTION.TRANSACTION_BRONZE.PAYMENTMETHOD as P