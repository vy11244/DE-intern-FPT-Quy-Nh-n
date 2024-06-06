/*
  The query is done to Aggregate date 
     Database: WideWorldImporters
     Module: SALE
     Silver table: SALE.SALE_SILVER.DATE
     Done by: NguyenTT24
     Last update when: 2024-03-19 01:11:00.0000000
*/
USE DATABASE SALE;
USE SCHEMA SALE_SILVER;

TRUNCATE TABLE IF EXISTS SALE.SALE_SILVER.DATE;

CREATE OR REPLACE TABLE SALE.SALE_SILVER.date_sequence AS
WITH RECURSIVE date_sequence_cte AS (
  SELECT '2013-01-01'::DATE AS Date
  UNION ALL
  SELECT DATEADD(DAY, 1, Date)
  FROM date_sequence_cte
  WHERE Date <  DATEADD(YEAR, 20, CURRENT_DATE) 
)
SELECT Date FROM date_sequence_cte;

INSERT INTO SALE.SALE_SILVER.DATE (Date)
SELECT Date FROM date_sequence;

-- Populate Day column
UPDATE SALE.SALE_SILVER.DATE
SET DAY = DAY(Date);

-- Populate Short Month column
UPDATE SALE.SALE_SILVER.DATE
SET SHORTMONTH = TO_CHAR(Date, 'Mon');

-- Populate Month column
UPDATE SALE.SALE_SILVER.DATE
SET MONTH = CASE
             WHEN SHORTMONTH = 'Jan' THEN 'January'
             WHEN SHORTMONTH = 'Feb' THEN 'February'
             WHEN SHORTMONTH = 'Mar' THEN 'March'
             WHEN SHORTMONTH = 'Apr' THEN 'April'
             WHEN SHORTMONTH = 'May' THEN 'May'
             WHEN SHORTMONTH = 'Jun' THEN 'June'
             WHEN SHORTMONTH = 'Jul' THEN 'July'
             WHEN SHORTMONTH = 'Aug' THEN 'August'
             WHEN SHORTMONTH = 'Sep' THEN 'September'
             WHEN SHORTMONTH = 'Oct' THEN 'October'
             WHEN SHORTMONTH = 'Nov' THEN 'November'
             WHEN SHORTMONTH = 'Dec' THEN 'December'
           END;
-- Populate Day Number column
UPDATE SALE.SALE_SILVER.DATE
SET DAYNUMBER = DAY(Date);

-- Populate Calendar Month Number column
UPDATE SALE.SALE_SILVER.DATE
SET CALENDARMONTHNUMBER = MONTH(Date);

-- Populate Calendar Month Label column
UPDATE SALE.SALE_SILVER.DATE
SET CALENDARMONTHLABEL = 'CY' || YEAR(Date) || '-' || SHORTMONTH;

-- Populate Calendar Year column
UPDATE SALE.SALE_SILVER.DATE
SET CALENDARYEAR = YEAR(Date);

-- Populate Calendar Year Label column
UPDATE SALE.SALE_SILVER.DATE
SET CALENDARYEARLABEL = 'CY' || YEAR(Date);

-- Populate Fiscal Month Number column
UPDATE SALE.SALE_SILVER.DATE
SET FISCALMONTHNUMBER = MOD(MONTH(Date) + 5, 12) + 1;

-- Populate Fiscal Month Label column
UPDATE SALE.SALE_SILVER.DATE
SET FISCALMONTHLABEL = 'FY' || YEAR(Date) || '-' || SHORTMONTH;

-- Populate Fiscal Year column
UPDATE SALE.SALE_SILVER.DATE
SET FISCALYEAR = CASE WHEN MONTH(Date) < 11 THEN YEAR(Date) - 1 ELSE YEAR(Date) END;

-- Populate Fiscal Year Label column
UPDATE SALE.SALE_SILVER.DATE
SET FISCALYEARLABEL = 'FY' || FISCALYEAR;

-- Populate ISO Week Number column
UPDATE SALE.SALE_SILVER.DATE
SET ISOWEEKNUMBER = WEEKOFYEAR(Date);

-- Dop table DATE_SEQUENCE
DROP TABLE SALE.SALE_SILVER.DATE_SEQUENCE;

-- Step 3: Delete duplicate
-- Create a temporary table to store unique rows
CREATE OR REPLACE TEMP TABLE SALE.SALE_SILVER.temp_date_table AS
SELECT *, ROW_NUMBER() OVER (PARTITION BY Date ORDER BY Date) AS row_num
FROM SALE.SALE_SILVER.DATE;

-- Delete duplicate rows from the original table
DELETE FROM SALE.SALE_SILVER.DATE
WHERE (Date) IN (
    SELECT Date
    FROM temp_date_table
    WHERE row_num > 1
);

-- Drop the temporary table
DROP TABLE SALE.SALE_SILVER.temp_date_table;