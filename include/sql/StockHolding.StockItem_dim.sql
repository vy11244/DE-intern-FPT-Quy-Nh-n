
--   The query is done to convert from bronze data into silver data
--      Database: WideWorldImporters
--      Module: STOCKHOLDING
--      Contributed Bronze Table: STOCKHOLDING.STOCKHOLDING_BRONZE.PACKAGETYPES , STOCKHOLDING.STOCKHOLDING_BRONZE.COLORS,STOCKHOLDING.STOCKHOLDING_BRONZE.STOCKITEMS
--      Silver table: STOCKHOLDING.STOCKHOLDING_SILVER.STOCKITEM
--      Done by: VyCK
--      Last update when: 2024-03-14 06:38:00.0000000


TRUNCATE TABLE IF EXISTS STOCKHOLDING.STOCKHOLDING_SILVER.STOCKITEM;

INSERT INTO STOCKHOLDING.STOCKHOLDING_SILVER.STOCKITEM (
    StockItemKey,
    WWIStockItemID,
    StockItem,
    Color,
    SellingPackage,
    BuyingPackage,
    Brand,
    Size,
    LeadTimeDays,
    QuantityPerOuter,
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
SELECT 
    CONCAT(V3.WWIStockItemID, TO_CHAR(V3.ValidFrom, 'YYYYMMDDHH24MISSFF7')) as StockItemKey,
    V3.WWIStockItemID,
    V3.StockItem,
    V3.Color,
    V3.SellingPackage,
    V3.BuyingPackage,
    V3.Brand,
    V3.Size,
    V3.LeadTimeDays,
    V3.QuanityPerOuter,
    V3.IsChillerStock,
    V3.Barcode,
    V3.TaxRate,
    V3.UnitPrice,
    V3.RecommendedRetailPrice,
    V3.TypicalWeightPerUnit,
    V3.Photo,
    V3.ValidFrom,
    V3.ValidTo
FROM (
    SELECT   
        V2.WWIStockItemID,
        V2.StockItem,
        V2.Color,
        V2.SellingPackage,
        T4.PackageTypeName as BuyingPackage,
        V2.Brand,
        V2.Size,
        V2.LeadTimeDays,
        V2.QuanityPerOuter,
        V2.IsChillerStock,
        V2.Barcode,
        V2.TaxRate,
        V2.UnitPrice,
        V2.RecommendedRetailPrice,
        V2.TypicalWeightPerUnit,
        V2.Photo,
        CASE WHEN V2.OUTERPACKAGEID IS NOT NULL THEN (CASE WHEN V2.ValidFrom < T4.ValidFrom THEN T4.ValidFRom ELSE V2.ValidFrom END) ELSE V2.ValidFrom END as ValidFrom,
        CASE WHEN V2.OUTERPACKAGEID IS NOT NULL THEN (CASE WHEN V2.ValidTo < T4.ValidTo THEN V2.ValidTo ELSE T4.ValidTo END) ELSE V2.ValidTo END as ValidTo
    FROM (
        SELECT 
            V1.StockItemID AS WWIStockItemID,
            V1.StockItemName as StockItem,
            V1.ColorName as Color,
            T3.PackageTypeName as SellingPackage,
            V1.OUTERPACKAGEID,
            V1.Brand,
            V1.Size,
            V1.LeadTimeDays,
            V1.QuanityPerOuter,
            V1.IsChillerStock,
            V1.Barcode,
            V1.TaxRate,
            V1.UnitPrice,
            V1.RecommendedRetailPrice,
            V1.TypicalWeightPerUnit,
            V1.Photo,
            CASE WHEN V1.UNITPACKAGEID IS NOT NULL THEN (CASE WHEN V1.ValidFrom < T3.ValidFrom THEN T3.ValidFRom ELSE V1.ValidFrom END) ELSE V1.ValidFrom END as ValidFrom,
            CASE WHEN V1.UNITPACKAGEID IS NOT NULL THEN (CASE WHEN V1.ValidTo < T3.ValidTo THEN V1.ValidTo ELSE T3.ValidTo END) ELSE V1.ValidTo END as ValidTo
        FROM (
            SELECT
                T1.StockItemID,
                T1.StockItemName,
                T2.ColorName,
                T1.UNITPACKAGEID,
                T1.OUTERPACKAGEID,
                T1.Brand as Brand,
                T1.Size as Size,
                T1.LEADTIMEDAYS as LeadTimeDays,
                T1.QUANTITYPEROUTER as QuanityPerOuter,
                T1.ISCHILLERSTOCK as IsChillerStock,
                T1.BARCODE as Barcode,
                T1.TAXRATE as TaxRate,
                T1.UNITPRICE as UnitPrice,
                T1.RECOMMENDEDRETAILPRICE as RECOMMENDEDRETAILPRICE,
                T1.TYPICALWEIGHTPERUNIT as TypicalWeightPerUnit,
                T1.PHOTO as Photo,
                CASE WHEN T1.COLORID IS NOT NULL THEN (CASE WHEN T1.ValidFrom < T2.ValidFrom THEN T2.ValidFRom ELSE T1.ValidFrom END) ELSE T1.ValidFrom END as ValidFrom,
                CASE WHEN T1.COLORID IS NOT NULL THEN (CASE WHEN T1.ValidTo < T2.ValidTo THEN T1.ValidTo ELSE T2.ValidTo END) ELSE T1.ValidTo END as ValidTo
            FROM STOCKHOLDING.STOCKHOLDING_BRONZE.STOCKITEMS as T1 
            LEFT JOIN STOCKHOLDING.STOCKHOLDING_BRONZE.COLORS as T2 
            ON T1.COLORID = T2.COLORID 
            AND T1.VALIDFROM < T2.VALIDTO 
            AND T1.VALIDTO > T2.VALIDFROM
        ) as V1 
        LEFT JOIN STOCKHOLDING.STOCKHOLDING_BRONZE.PACKAGETYPES as T3 
        ON V1.UNITPACKAGEID = T3.PACKAGETYPEID 
        AND V1.ValidFrom < T3.ValidTo 
        AND V1.ValidTo > T3.ValidFrom
    ) as V2
    LEFT JOIN STOCKHOLDING.STOCKHOLDING_BRONZE.PACKAGETYPES as T4 
    ON V2.OUTERPACKAGEID = T4.PACKAGETYPEID 
    AND V2.ValidFrom < T4.ValidTo 
    AND V2.ValidTo > T4.ValidFrom 
) as V3;