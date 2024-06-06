/*
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: ORDERS
     Contributed Bronze Table: ORDERS.ORDERS_BRONZE.COUNTRY,ORDERS.ORDERS_BRONZE.STATEPROVINCES,ORDERS.ORDERS_BRONZE.CITIES
     Silver table: ORDERS.ORDERS_SILVER.CITY
     Done by: THAOPLT2
     Last update when: 2024-03-22 10:35:00.0000000
     Update content: Edit primary key format, SCD Type 2 and query template
*/
truncate TABLE ORDERS.ORDERS_SILVER.CITY;
INSERT INTO ORDERS.ORDERS_SILVER.City (
    CITYKEY,
    WWICITYID,
    CITY,
    STATEPROVINCE,
    COUNTRY,
    CONTINENT,
    SALESTERRITORY,
    REGION,
    SUBREGION,
    LOCATION,
    LATESTRECORDEDPOPULATION,
    VALIDFROM,
    VALIDTO 
)
-- Step 2: create temp (temporary) table to combine table A with Cities and figure out ValidFrom, ValidTo according to SCD type 2
with temp as(
-- Step 1: create A table to join Countries and StateProvinces tables and find ValidFrom, ValidTo according to SCD type 2
with A as 
(SELECT
    S.STATEPROVINCEID,
    S.StateProvinceName,
    C.CountryName,
    C.Continent,
    S.SALESTERRITORY,
    C.Region,
    C.Subregion,
    CASE WHEN S.COUNTRYID IS NOT NULL THEN (CASE WHEN S.ValidFrom < C.ValidFrom THEN C.ValidFRom ELSE S.ValidFrom END) ELSE S.ValidFrom END as ValidFrom,
    CASE WHEN S.COUNTRYID IS NOT NULL THEN (CASE WHEN S.ValidTo < C.ValidTo THEN S.ValidTo ELSE C.ValidTo END) ELSE S.ValidTo END as ValidTo
FROM
    ORDERS.ORDERS_BRONZE.STATEPROVINCES as S
    left join ORDERS.ORDERS_BRONZE.COUNTRIES as C on S.COUNTRYID = C.COUNTRYID
    and S.VALIDFROM < C.VALIDTO and S.VALIDTO > C.VALIDFROM
)

SELECT 
     C.CITYID as WWICITYID,
     C.CITYNAME as CITY,
     A.STATEPROVINCENAME as STATEPROVINCE,
     A.COUNTRYNAME as COUNTRY,
     A.CONTINENT as CONTINENT,
     A.SALESTERRITORY as SALESTERRITORY,
     A.REGION as REGION,
     A.SUBREGION as SUBREGION,
     C.LOCATION as LOCATION,
     case when C.LATESTRECORDEDPOPULATION is null then 0 else C.LATESTRECORDEDPOPULATION end as LATESTRECORDEDPOPULATION,
     CASE WHEN C.STATEPROVINCEID IS NOT NULL THEN (CASE WHEN C.ValidFrom < A.ValidFrom THEN A.ValidFRom ELSE C.ValidFrom END) ELSE C.ValidFrom END as ValidFrom,
     CASE WHEN C.STATEPROVINCEID IS NOT NULL THEN (CASE WHEN C.ValidTo < A.ValidTo THEN C.ValidTo ELSE A.ValidTo END) ELSE C.ValidTo END as ValidTo 

FROM 
     ORDERS.ORDERS_BRONZE.CITIES as C
     left join A on C.STATEPROVINCEID = A.STATEPROVINCEID
     and C.VALIDFROM < A.VALIDTO and C.VALIDTO > A.VALIDFROM)
-- select temp table and concat WWICITYID with ValidFrom found before to make CITYKEY
select
    concat(temp.WWICITYID, TO_CHAR(temp.ValidFrom,'YYYYMMDDHH24MISSFF7')) as CITYKEY,
    temp.WWICITYID,
    temp.CITY,
    temp.STATEPROVINCE,
    temp.COUNTRY,
    temp.CONTINENT,
    temp.SALESTERRITORY,
    temp.REGION,
    temp.SUBREGION,
    temp.LOCATION,
    temp.LATESTRECORDEDPOPULATION,
    temp.VALIDFROM,
    temp.VALIDTO
from temp


