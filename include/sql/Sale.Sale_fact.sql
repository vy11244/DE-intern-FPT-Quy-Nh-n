/*
  The query is done to convert from bronze data into silver data using others silver data
     Database: WideWorldImporters
     Module: SALE
     Contributed Bronze Table: SALE.SALE_BRONZE.STOCKITEMS,SALE.SALE_BRONZE.INVOICES,SALE.SALE_BRONZE.INVOICELINES,SALE.SALE_BRONZE.CUSTOMERS,SALE.SALE_BRONZE.PACKAGETYPES
     Contributed Silver Table : SALE.SALE_SILVER.CITY,SALE.SALE_SILVER.CUSTOMER,SALE.SALE_SILVER.EMPLOYEE,SALE.SALE_SILVER.STOCKITEM
     Silver table: SALE.SALE_SILVER.SALE
     Done by: NguyenTT24
     Last update when: 2024-03-19 01:11:00.0000000
     Update content: Incremental load for FACT.SALE, method : delete/insert
*/


USE DATABASE SALE;
USE SCHEMA SALE_SILVER;


-- Delete rows that have wwinvoiceid in source data

DELETE FROM SALE_SILVER.SALE as s WHERE 
s.WWIINVOICEID IN (
SELECT  

    i.INVOICEID as WWIINVOICEID,

    FROM SALE_BRONZE.INVOICELINES AS inl 
    
    INNER JOIN SALE_BRONZE.INVOICES as i ON i.invoiceid  = inl.invoiceid
    
 );

-- insert new data 

INSERT INTO SALE.SALE_SILVER.SALE (
                CITYKEY,
                CUSTOMERKEY,
                BILLTOCUSTOMERKEY,
                SALESPERSONKEY,
                STOCKITEMKEY,
                INVOICEDATEKEY,
                DELIVERYDATEKEY,
                WWIINVOICEID,
                DESCRIPTION,
                PACKAGE,
                QUANTITY,
                UNITPRICE,
                TAXRATE,
                TOTALEXCLUDINGTAX,
                Taxamount,
                PROFIT,
                TOTALINCLUDINGTAX,
                TOTALDRYITEMS,
                TOTALCHILLERITEMS

) 
-- There are 6 temp queries that support main query
/*
  This temp table called SALE_FACT: extract all neccessary data from bronze tables :
  Invoices,InvoiceLines,PackageTypes,StockItem,Customers tables, take their ID 's to use them later on
  to take DateKeys from dim tables
  tables
*/
with SALE_FACT as (
SELECT  
    si.VALIDFROM,
    si.VALIDTO,
    si.STOCKITEMID,
    cus.DELIVERYCITYID,
    inl.DESCRIPTION,
    inl.QUANTITY,
	inl.UNITPRICE,
	inl.TAXRATE,
    inl.EXTENDEDPRICE,
	inl.TAXAMOUNT,
	inl.LINEPROFIT as PROFIT,
    i.SALESPERSONPERSONID,
    i.CUSTOMERID,
    i.BILLTOCUSTOMERID,
    i.INVOICEID as WWIINVOICEID,
    i.INVOICEDATE as INVOICEDATEKEY,
    i.CONFIRMEDDELIVERYTIME as DELIVERYDATEKEY,
    pck.PackageTypeName AS PACKAGE ,
    -- TotalExcludingTax,
    inl.ExtendedPrice - inl.TaxAmount AS TOTALEXCLUDINGTAX ,
    
    -- Contidion for lastmodifiedwhen
    CASE WHEN inl.LASTEDITEDWHEN > i.LASTEDITEDWHEN 
    Then inl.LASTEDITEDWHEN ELSE i.LASTEDITEDWHEN END as LASTMODIFIEDWHEN,
    
    -- Condition for totaldryitems
    CASE WHEN si.IsChillerStock = 0 THEN inl.Quantity ELSE 0 END as TOTALDRYITEMS,
    
    -- -- Condition  for totalchillerstock
    CASE WHEN si.IsChillerStock <> 0 THEN inl.Quantity ELSE 0 END as TOTALCHILLERITEMS
    
    FROM SALE_BRONZE.INVOICELINES AS inl 
    
    INNER JOIN SALE_BRONZE.INVOICES as i ON i.invoiceid  = inl.invoiceid 
    
    INNER JOIN SALE_BRONZE.PACKAGETYPES as pck on inl.packagetypeid = pck.packagetypeid 
    
    INNER JOIN SALE_BRONZE.CUSTOMERS as cus on i.customerid = cus.customerid
    AND LASTMODIFIEDWHEN > cus.ValidFrom
        AND LASTMODIFIEDWHEN <= cus.ValidTo 

    INNER JOIN SALE_BRONZE.CUSTOMERS as bt on i.billtocustomerid = bt.customerid
    AND LASTMODIFIEDWHEN > bt.ValidFrom
        AND LASTMODIFIEDWHEN <= bt.ValidTo 
    
    
    INNER JOIN SALE_BRONZE.STOCKITEMS as si on inl.stockitemid = si.stockitemid 
    AND LASTMODIFIEDWHEN > si.ValidFrom
        AND LASTMODIFIEDWHEN <= si.ValidTo 

    ORDER BY i.InvoiceID
    ),
-- This temp table called UPDATE_CUSTOMER_KEY : use customer ID to join silver.customer and take CUSTOMERKEY
    UPDATE_CUSTOMER_KEY as (
        SELECT  cus.CUSTOMERKEY,
                A.INVOICEDATEKEY,
                A.DELIVERYDATEKEY,
                A.WWIINVOICEID ,
                A.DESCRIPTION,
                A.PACKAGE,
                A.QUANTITY,
                A.UNITPRICE,
                A.TAXRATE,
                A.TOTALEXCLUDINGTAX,
                A.Taxamount,
                A.PROFIT,
                A.ExtendedPrice as TOTALINCLUDINGTAX,
                A.TOTALDRYITEMS,
                A.TOTALCHILLERITEMS,
                A.LASTMODIFIEDWHEN,
                A.DELIVERYCITYID,
                A.BILLTOCUSTOMERID,
                A.STOCKITEMID,
                A.SALESPERSONPERSONID
            From SALE_FACT as A
        inner JOIN SALE_SILVER.CUSTOMER as cus on cus.wwicustomerid = A.customerid
        AND A.LASTMODIFIEDWHEN > cus.ValidFrom
        AND A.LASTMODIFIEDWHEN <= cus.ValidTo
    ),

-- This temp table called UPDATE_CITY_KEY : use delivery city ID to join silver.city and take CITYKEY

    UPDATE_CITY_KEY as (
        SELECT  CT.CITYKEY,
                CK.CUSTOMERKEY,
                CK.INVOICEDATEKEY,
                CK.DELIVERYDATEKEY,
                CK.WWIINVOICEID ,
                CK.DESCRIPTION,
                CK.PACKAGE,
                CK.QUANTITY,
                CK.UNITPRICE,
                CK.TAXRATE,
                CK.TOTALEXCLUDINGTAX,
                CK.Taxamount,
                CK.PROFIT,
                CK.TOTALINCLUDINGTAX,
                CK.TOTALDRYITEMS,
                CK.TOTALCHILLERITEMS,
                CK.LASTMODIFIEDWHEN,
                CK.BILLTOCUSTOMERID,
                CK.STOCKITEMID,
                CK.SALESPERSONPERSONID
            From UPDATE_CUSTOMER_KEY as CK 
        inner join SALE_SILVER.CITY as CT on CT.wwicityid = CK.deliverycityid 
        AND CK.LASTMODIFIEDWHEN > CT.ValidFrom
        AND CK.LASTMODIFIEDWHEN <= CT.ValidTo
    ),

-- This temp table called UPDATE_BILL_TO_CUSTOMER_KEY : use bill to customer ID to join silver.customer and take BILLTOCUSTOMERKEY

    UPDATE_BILL_TO_CUSTOMER_KEY as (
        SELECT  CUS.CUSTOMERKEY as BILLTOCUSTOMERKEY,
                CTK.CITYKEY,
                CTK.CUSTOMERKEY,
                CTK.INVOICEDATEKEY,
                CTK.DELIVERYDATEKEY,
                CTK.WWIINVOICEID,
                CTK.DESCRIPTION,
                CTK.PACKAGE,
                CTK.QUANTITY,
                CTK.UNITPRICE,
                CTK.TAXRATE,
                CTK.TOTALEXCLUDINGTAX,
                CTK.Taxamount,
                CTK.PROFIT,
                CTK.TOTALINCLUDINGTAX,
                CTK.TOTALDRYITEMS,
                CTK.TOTALCHILLERITEMS,
                CTK.LASTMODIFIEDWHEN,
                CTK.STOCKITEMID,
                CTK.SALESPERSONPERSONID
            From UPDATE_CITY_KEY as CTK 
        inner JOIN SALE_SILVER.CUSTOMER as CUS on CUS.wwicustomerid = CTK.billtocustomerid
        AND CTK.LASTMODIFIEDWHEN > CUS.ValidFrom
        AND CTK.LASTMODIFIEDWHEN <= CUS.ValidTo
        ),

-- This temp table called UPDATE_SALE_PERSON_KEY : use sales person person ID to join silver.employee and take EMPLOYEEKEY
   
    UPDATE_SALE_PERSON_KEY as (
        SELECT  EMP.EMPLOYEEKEY as SALESPERSONKEY,
                BTCK.CITYKEY,
                BTCK.CUSTOMERKEY,
                BTCK.BILLTOCUSTOMERKEY,
                BTCK.INVOICEDATEKEY,
                BTCK.DELIVERYDATEKEY,
                BTCK.WWIINVOICEID,
                BTCK.DESCRIPTION,
                BTCK.PACKAGE,
                BTCK.QUANTITY,
                BTCK.UNITPRICE,
                BTCK.TAXRATE,
                BTCK.TOTALEXCLUDINGTAX,
                BTCK.Taxamount,
                BTCK.PROFIT,
                BTCK.TOTALINCLUDINGTAX,
                BTCK.TOTALDRYITEMS,
                BTCK.TOTALCHILLERITEMS,
                BTCK.LASTMODIFIEDWHEN,
                BTCK.STOCKITEMID
            From UPDATE_BILL_TO_CUSTOMER_KEY as BTCK 
        inner join SALE_SILVER.EMPLOYEE AS EMP on EMP.wwiemployeeid = BTCK.salespersonpersonid
        AND BTCK.LASTMODIFIEDWHEN > EMP.ValidFrom
        AND BTCK.LASTMODIFIEDWHEN <= EMP.ValidTo
        ),

-- This temp table called COMPLETE_FACT_SALE : use stockitemid ID to join silver.stockitem and take STOCKITEMKEY
-- Contain all data require for fact table
  
    COMPLETE_FACT_SALE  as (
        SELECT  SI.STOCKITEMKEY,
                SPK.SALESPERSONKEY,
                SPK.CITYKEY,
                SPK.CUSTOMERKEY,
                SPK.BILLTOCUSTOMERKEY,
                SPK.INVOICEDATEKEY,
                SPK.DELIVERYDATEKEY,
                SPK.WWIINVOICEID,
                SPK.DESCRIPTION,
                SPK.PACKAGE,
                SPK.QUANTITY,
                SPK.UNITPRICE,
                SPK.TAXRATE,
                SPK.TOTALEXCLUDINGTAX,
                SPK.Taxamount,
                SPK.PROFIT,
                SPK.TOTALINCLUDINGTAX,
                SPK.TOTALDRYITEMS,
                SPK.TOTALCHILLERITEMS,
                SPK.LASTMODIFIEDWHEN
            From UPDATE_SALE_PERSON_KEY as SPK 
        inner JOIN SALE_SILVER."STOCKITEM" as SI on SI.wwistockitemid = SPK.stockitemid
        AND SPK.LASTMODIFIEDWHEN > SI.ValidFrom
        AND SPK.LASTMODIFIEDWHEN <= SI.ValidTo 
        )
-- Main query: combinate all column and insert it into table
    SELECT
                CITYKEY,
                CUSTOMERKEY,
                BILLTOCUSTOMERKEY,
                SALESPERSONKEY,
                STOCKITEMKEY,
                INVOICEDATEKEY,
                DELIVERYDATEKEY,
                WWIINVOICEID,
                DESCRIPTION,
                PACKAGE,
                QUANTITY,
                UNITPRICE,
                TAXRATE,
                TOTALEXCLUDINGTAX,
                Taxamount,
                PROFIT,
                TOTALINCLUDINGTAX,
                TOTALDRYITEMS,
                TOTALCHILLERITEMS
            From COMPLETE_FACT_SALE 






    
    
        
        


