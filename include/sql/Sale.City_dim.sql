/*
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: SALE
     Contributed Bronze Table: SALE.SALE_BRONZE.CITIES,SALE.SALE_BRONZE.COUNTRIES,SALE.SALE_BRONZE.STATEPROVINCES
     Silver table: SALE.SALE_SILVER.City
     Done by: DatTT85
     Last update when: 2024-03-14 06:38:00.0000000
     Update content: Edit primary key format, SCD Type 2 and query template
*/
TRUNCATE TABLE IF EXISTS SALE.SALE_SILVER.City;-- This statement clears out any existing data in the silver table if it exists.

-- This part begins the insertion of data into the silver table.
INSERT INTO SALE.SALE_SILVER.City (
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

with a as 
(
SELECT
    S.STATEPROVINCEID,
    S.StateProvinceName,
    C.countryid,
    C.CountryName,
    C.Continent,
    S.SalesTerritory,
    C.Region,
    C.Subregion,
    case when S.COUNTRYID is not null then (
	CASE WHEN S.VALIDFROM < C.ValidFrom THEN C.ValidFrom ELSE S.VALIDFROM END
    )
    ELSE S.VALIDFROM END AS VALIDFROM,
    case when S.COUNTRYID is not null then (
        CASE WHEN S.VALIDTO < C.ValidTo THEN S.VALIDTO ELSE C.ValidTo END 
    )
    ELSE S.ValidTo END AS ValidTo

FROM
    SALE.SALE_BRONZE.STATEPROVINCES as S
    left join SALE.SALE_BRONZE.COUNTRIES as C 
    on S.COUNTRYID = C.COUNTRYID
    and S.VALIDFROM < C.VALIDTO 
    and S.VALIDTO > C.VALIDFROM
),

b as (
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
     case when C.STATEPROVINCEID is not null then (
	CASE WHEN C.VALIDFROM < A.ValidFrom THEN A.ValidFrom ELSE C.VALIDFROM END
    )
    ELSE C.VALIDFROM END AS VALIDFROM,
    case when C.STATEPROVINCEID is not null then (
        CASE WHEN C.VALIDTO < A.ValidTo THEN C.VALIDTO ELSE A.ValidTo END 
    )
    ELSE C.ValidTo END AS ValidTo

FROM 
     SALE.SALE_BRONZE.CITIES as C
     left join a
     on C.STATEPROVINCEID = A.STATEPROVINCEID
     and C.VALIDFROM < A.VALIDTO 
     and C.VALIDTO > A.VALIDFROM
)
     
select 
    CONCAT(b.WWICITYID,TO_CHAR(b.ValidFrom,'YYYYMMDDHH24MISSFF7')) as CITYKEY,
    b.WWICITYID,
    b.CITY,
    b.STATEPROVINCE,
    b.COUNTRY,
    b.CONTINENT,
    b.SALESTERRITORY,
    b.REGION,
    b.SUBREGION,
    b.LOCATION,
    b.LATESTRECORDEDPOPULATION,
    b.VALIDFROM,
    b.VALIDTO 
from b