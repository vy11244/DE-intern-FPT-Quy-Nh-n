/*
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: SALE
     Contributed Bronze Table: SALE.SALE_BRONZE.CUSTOMER,SALE.SALE_BRONZE.BUYINGGROUPS,SALE.SALE_BRONZE.CUSTOMERCATEGORIES, APPLICATION.PEOPLE
     Silver table: SALE.SALE_SILVER.CUSTOMER
     Done by: bachv2
     Last update when: 2024-03-21
     Update content: Edit primary key format, add missing data from the tables, add the condition to check the validfrom and validto date, version basic, update with basic logic code.
*/
-- Clear existing data from the CUSTOMER table in the SALE.SALE_SILVER schema.
TRUNCATE SALE.SALE_SILVER.CUSTOMER;
-- Insert updated customer data into the SALE.SALE_SILVER.CUSTOMER table.
INSERT INTO SALE.SALE_SILVER.CUSTOMER (
    CUSTOMERKEY,
    WWICUSTOMERID,
    CUSTOMER,
    BILLTOCUSTOMER,
    CATEGORY,
    BUYINGGROUP,
    PRIMARYCONTACT,
    POSTALCODE,
    VALIDFROM,
    VALIDTO
)

SELECT
        CONCAT(V4.WWICUSTOMERID, TO_CHAR(V4.ValidFrom, 'YYYYMMDDHH24MISSFF7')) AS CUSTOMERKEY,
        V4.WWICUSTOMERID,
        V4.CUSTOMER,
        V4.BILLTOCUSTOMER,
        V4.CATEGORY,
        V4.BUYINGGROUP,
        V4.PRIMARYCONTACT,
        V4.POSTALCODE,
        V4.VALIDFROM,
        V4.VALIDTO
            FROM(
                SELECT
                    V3.WWICUSTOMERID,
                    V3.CUSTOMER,
                    V3.BILLTOCUSTOMER,
                    V3.CATEGORY,
                    V3.BUYINGGROUP,
                    T5.FULLNAME AS PRIMARYCONTACT,
                    V3.POSTALCODE,
                    CASE WHEN V3.PRIMARYCONTACTPERSONID IS NOT NULL THEN (CASE WHEN V3.ValidFrom < T5.ValidFrom THEN T5.ValidFRom ELSE V3.ValidFrom END) ELSE V3.ValidFrom END as ValidFrom,
                    CASE WHEN V3.PRIMARYCONTACTPERSONID IS NOT NULL THEN (CASE WHEN V3.ValidTo < T5.ValidTo THEN V3.ValidTo ELSE T5.ValidTo END) ELSE V3.ValidTo END as ValidTo
                        FROM(
                            SELECT
                                V2.PRIMARYCONTACTPERSONID,
                                V2.WWICUSTOMERID,
                                V2.CUSTOMER,
                                V2.CATEGORY,
                                V2.BUYINGGROUP,
                                T4.CUSTOMERNAME AS BILLTOCUSTOMER,
                                V2.POSTALCODE,
                                CASE WHEN V2.BILLTOCUSTOMERID IS NOT NULL THEN (CASE WHEN V2.ValidFrom < T4.ValidFrom THEN T4.ValidFRom ELSE V2.ValidFrom END) ELSE V2.ValidFrom END as ValidFrom,
                                CASE WHEN V2.BILLTOCUSTOMERID IS NOT NULL THEN (CASE WHEN V2.ValidTo < T4.ValidTo THEN V2.ValidTo ELSE T4.ValidTo END) ELSE V2.ValidTo END as ValidTo
                                    FROM(
                                        SELECT 
                                            V1.PRIMARYCONTACTPERSONID,
                                            V1.BILLTOCUSTOMERID,
                                            V1.WWICUSTOMERID,
                                            V1.CUSTOMER,
                                            T3.BUYINGGROUPNAME AS BUYINGGROUP,
                                            V1.CUSTOMERCATEGORYNAME AS CATEGORY,
                                            V1.POSTALCODE,
                                            CASE WHEN V1.BUYINGGROUPID IS NOT NULL THEN (CASE WHEN V1.ValidFrom < T3.ValidFrom THEN T3.ValidFRom ELSE V1.ValidFrom END) ELSE V1.ValidFrom END as ValidFrom,
                                            CASE WHEN V1.BUYINGGROUPID IS NOT NULL THEN (CASE WHEN V1.ValidTo < T3.ValidTo THEN V1.ValidTo ELSE T3.ValidTo END) ELSE V1.ValidTo END as ValidTo
                                            FROM(
                                                SELECT
                                                    T1.PRIMARYCONTACTPERSONID,
                                                    T1.BILLTOCUSTOMERID AS BILLTOCUSTOMERID,
                                                    T1.BUYINGGROUPID,
                                                    T1.CUSTOMERID AS WWICUSTOMERID,
                                                    T1.CUSTOMERNAME AS CUSTOMER,
                                                    T2.CUSTOMERCATEGORYNAME,
                                                    T1.DELIVERYPOSTALCODE AS POSTALCODE,
                                                    CASE WHEN T1.CUSTOMERCATEGORYID IS NOT NULL THEN (CASE WHEN T1.ValidFrom < T2.ValidFrom THEN T2.ValidFRom ELSE T1.ValidFrom END) ELSE T1.ValidFrom END as ValidFrom,
                                                    CASE WHEN T1.CUSTOMERCATEGORYID IS NOT NULL THEN (CASE WHEN T1.ValidTo < T2.ValidTo THEN T1.ValidTo ELSE T2.ValidTo END) ELSE T1.ValidTo END as ValidTo
                                                    FROM
                                                        SALE.SALE_BRONZE.CUSTOMERS T1
                                                        LEFT JOIN SALE.SALE_BRONZE.CUSTOMERCATEGORIES T2 ON T1.CUSTOMERCATEGORYID = T2.CUSTOMERCATEGORYID
                                                        AND T1.VALIDFROM < T2.VALIDTO AND T1.VALIDTO > T2.VALIDFROM) AS V1
                                            LEFT JOIN SALE.SALE_BRONZE.BUYINGGROUPS T3 ON V1.BUYINGGROUPID = T3.BUYINGGROUPID
                                            AND V1.VALIDFROM < T3.VALIDTO AND V1.VALIDTO > T3.VALIDFROM) AS V2
                                        INNER JOIN SALE.SALE_BRONZE.CUSTOMERS T4 ON V2.BILLTOCUSTOMERID = T4.CUSTOMERID
                                        AND V2.VALIDFROM < T4.VALIDTO AND V2.VALIDTO > T4.VALIDFROM) AS V3
                        LEFT JOIN SALE.SALE_BRONZE.PEOPLE T5 ON V3.PRIMARYCONTACTPERSONID = T5.PERSONID
                        AND V3.VALIDFROM < T5.VALIDTO AND V3.VALIDTO > T5.VALIDFROM) AS V4;