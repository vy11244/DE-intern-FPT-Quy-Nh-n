/*
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: ORDERS
     Contributed Bronze Table: None
     Silver table: ORDERS.ORDERS_SILVER.DATE
     Done by: ThongNNV
     Last update when: 2024-03-19 14:27:00.0000000
     Update content: Fix code truncate table
*/
USE DATABASE ORDERS;
USE SCHEMA ORDERS_SILVER;

TRUNCATE TABLE ORDERS.ORDERS_SILVER.DATE;

-- Step 1: Create a table with a sequence of dates
CREATE OR REPLACE TABLE ORDERS.ORDERS_SILVER.date_sequence AS
WITH RECURSIVE date_sequence_cte AS (
  SELECT '2013-01-01'::DATE AS Date
  UNION ALL
  SELECT DATEADD(DAY, 1, Date)
  FROM date_sequence_cte
  WHERE Date < DATEADD(YEAR, 20, CURRENT_DATE()) 
)
SELECT Date FROM date_sequence_cte;

-- Step 2: Use the sequence of dates to populate columns in your existing table
-- Ensure to replace TRANSACTION.TRANSACTION_SILVER.DATE with your actual table name
INSERT INTO ORDERS.ORDERS_SILVER.DATE (Date)
SELECT Date FROM date_sequence;

-- Populate Day column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET DAY = DAY(Date);

-- Populate Short Month column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET SHORTMONTH = TO_CHAR(Date, 'Mon');

-- Populate Month column
UPDATE ORDERS.ORDERS_SILVER.DATE
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
UPDATE ORDERS.ORDERS_SILVER.DATE
SET DAYNUMBER = DAY(Date);

-- Populate Calendar Month Number column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET CALENDARMONTHNUMBER = MONTH(Date);

-- Populate Calendar Month Label column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET CALENDARMONTHLABEL = 'CY' || YEAR(Date) || '-' || SHORTMONTH;

-- Populate Calendar Year column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET CALENDARYEAR = YEAR(Date);

-- Populate Calendar Year Label column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET CALENDARYEARLABEL = 'CY' || YEAR(Date);

-- Populate Fiscal Month Number column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET FISCALMONTHNUMBER = MOD(MONTH(Date) + 5, 12) + 1;

-- Populate Fiscal Month Label column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET FISCALMONTHLABEL = 'FY' || YEAR(Date) || '-' || SHORTMONTH;

-- Populate Fiscal Year column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET FISCALYEAR = CASE WHEN MONTH(Date) < 11 THEN YEAR(Date) - 1 ELSE YEAR(Date) END;

-- Populate Fiscal Year Label column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET FISCALYEARLABEL = 'FY' || FISCALYEAR;

-- Populate ISO Week Number column
UPDATE ORDERS.ORDERS_SILVER.DATE
SET ISOWEEKNUMBER = WEEKOFYEAR(Date);

-- Dop table DATE_SEQUENCE
DROP TABLE ORDERS.ORDERS_SILVER.DATE_SEQUENCE;

-- Step 3: Delete duplicate
-- Create a temporary table to store unique rows
CREATE OR REPLACE TEMP TABLE ORDERS.ORDERS_SILVER.temp_date_table AS
SELECT *, ROW_NUMBER() OVER (PARTITION BY Date ORDER BY Date) AS row_num
FROM ORDERS.ORDERS_SILVER.DATE;

-- Delete duplicate rows from the original table
DELETE FROM ORDERS.ORDERS_SILVER.DATE
WHERE (Date) IN (
    SELECT Date
    FROM temp_date_table
    WHERE row_num > 1
);

-- Drop the temporary table
DROP TABLE ORDERS.ORDERS_SILVER.temp_date_table;