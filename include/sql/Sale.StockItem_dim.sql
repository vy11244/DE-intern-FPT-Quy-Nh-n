/*
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: SALE
     Contributed Bronze Table: SALE.SALE_BRONZE.STOCKITEMS,SALE.SALE_BRONZE.COLORS,SALE.SALE_BRONZE.PACKAGETYPES
     Silver table: SALE.SALE_SILVER.STOCKITEM
     Done by: HungPQ33
     Last update when: 2024-03-14 06:38:00.0000000
     Update content: Edit primary key format, SCD Type 2 and query template
*/

TRUNCATE TABLE IF EXISTS SALE.SALE_SILVER.STOCKITEM; -- This statement clears out any existing data in the silver table if it exists.

-- This part begins the insertion of data into the silver table.
INSERT INTO SALE.SALE_SILVER.STOCKITEM (
    StockItemKey,
    WWIStockItemID,
    StockItem,
    Color,
    SellingPackage,
    BuyingPackage,
    Brand,
    Size,
    LeadTimeDays,
    QuanityPerOuter,
    IsChillerStock,
    Barcode,
    TaxRate,
    UnitPrice,
    RecommendedRetail,
    TypicalWeightPerUnit,
    Photo,
    ValidFrom,
    ValidTo
)

-- There are 3 temp queries that support main query
/*
  This temp table called a: extract all neccessary data from People table and extract more column from Colors table
*/
with a as 
(
SELECT
    S.StockItemID,
    S.StockItemName,
    C.ColorName,
    S.UNITPACKAGEID,
    S.OUTERPACKAGEID,
    S.Brand as Brand,
    S.Size as Size,
    S.LEADTIMEDAYS as LeadTimeDays,
    S.QUANTITYPEROUTER as QuanityPerOuter,
    S.ISCHILLERSTOCK as IsChillerStock,
    S.BARCODE as Barcode,
    S.TAXRATE as TaxRate,
    S.UNITPRICE as UnitPrice,
    S.RECOMMENDEDRETAILPRICE as RECOMMENDEDRETAILPRICE,
    S.TYPICALWEIGHTPERUNIT as TypicalWeightPerUnit,
    S.PHOTO as Photo,
    case when S.COLORID is not null then (
        CASE WHEN S.ValidFrom < C.ValidFrom THEN C.ValidFRom ELSE S.ValidFrom END
    )
    ELSE S.ValidFrom END as ValidFrom,
    case when S.COLORID is not null then (
        CASE WHEN S.ValidTo < C.ValidTo THEN S.ValidTo ELSE C.ValidTo END
    )
    ELSE S.ValidTo END as ValidTo
 
FROM SALE.SALE_BRONZE.STOCKITEMS as S 
    left join SALE.SALE_BRONZE.COLORS as C 
    on S.COLORID = C.COLORID 
    and S.VALIDFROM < C.VALIDTO 
    and S.VALIDTO > C.VALIDFROM
),

/*
   With temp query called b: It is used to extract more column: 
   PackageTypeName as SellingPackage from PackageTypes table 
*/
b as (
SELECT 
    a.StockItemID AS WWIStockItemID,
    a.StockItemName as StockItem,
    a.ColorName as Color,
    P1.PackageTypeName as SellingPackage,
    a.OUTERPACKAGEID,
    a.Brand,
    a.Size,
    a.LeadTimeDays,
    a.QuanityPerOuter,
    a.IsChillerStock,
    a.Barcode,
    a.TaxRate,
    a.UnitPrice,
    a.RecommendedRetailPrice,
    a.TypicalWeightPerUnit,
    a.Photo,
    case when a.UNITPACKAGEID is not null then (
        CASE WHEN a.ValidFrom < P1.ValidFrom THEN P1.ValidFRom ELSE a.ValidFrom END
    )
    ELSE a.ValidFrom END as ValidFrom,
    case when a.UNITPACKAGEID is not null then (
        CASE WHEN a.ValidTo < P1.ValidTo THEN a.ValidTo ELSE P1.ValidTo END
    )
    ELSE a.ValidTo END as ValidTo
    
FROM a 
    left join SALE.SALE_BRONZE.packagetypes as P1 
    on a.UNITPACKAGEID = P1.PACKAGETYPEID 
    and a.ValidFrom < P1.ValidTo 
    and a.ValidTo > P1.ValidFrom
),

/*
    With temp query called c: It is used to extract more column: 
       PackageTypeName as BuyPackage from PackageTypes table 
*/
c as (
SELECT   
    b.WWIStockItemID,
    b.StockItem,
    b.Color,
    b.SellingPackage,
    P2.PackageTypeName as BuyingPackage,
    b.Brand,
    b.Size,
    b.LeadTimeDays,
    b.QuanityPerOuter,
    b.IsChillerStock,
    b.Barcode,
    b.TaxRate,
    b.UnitPrice,
    b.RecommendedRetailPrice,
    b.TypicalWeightPerUnit,
    b.Photo,
    case when b.OUTERPACKAGEID is not null then (
        CASE WHEN b.ValidFrom < P2.ValidFrom THEN P2.ValidFRom ELSE b.ValidFrom END
    )
    ELSE b.ValidFrom END as ValidFrom,
    case when b.OUTERPACKAGEID is not null then (
        CASE WHEN b.ValidTo < P2.ValidTo THEN b.ValidTo ELSE P2.ValidTo END
    )
    ELSE b.ValidTo END as ValidTo
    
FROM b
    left join SALE.SALE_BRONZE.packagetypes as P2 
    on b.OUTERPACKAGEID = P2.PACKAGETYPEID 
    and b.ValidFrom < P2.ValidTo 
    and b.ValidTo > P2.ValidFrom 
)
-- Main query: combinate all column again and add StockItemKey by concating WWIStockItemID and ValidFrom
select 
    concat(c.WWIStockItemID, TO_CHAR(c.ValidFrom,'YYYYMMDDHH24MISSFF7')) as StockItemKey,
    c.WWIStockItemID,
    c.StockItem,
    c.Color,
    c.SellingPackage,
    c.BuyingPackage,
    c.Brand,
    c.Size,
    c.LeadTimeDays,
    c.QuanityPerOuter,
    c.IsChillerStock,
    c.Barcode,
    c.TaxRate,
    c.UnitPrice,
    c.RecommendedRetailPrice,
    c.TypicalWeightPerUnit,
    c.Photo,
    c.ValidFrom,
    c.ValidTo
from c