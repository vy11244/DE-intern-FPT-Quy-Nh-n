-- Below is an example demonstrating how to add comments similar to the one above for the second part of the script.

/*
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: ORDERS
     Contributed Bronze Table: ORDERS.ORDERS_BRONZE.PEOPLE
     Silver table: ORDERS.ORDERS_SILVER.EMPLOYEE
     Done by: HuyBT5
     Last update when: 2024-03-19 14:38:00.0000000
     Update content: Edit primary key and template
*/
-- Clearing out any existing data in the silver table if it exists.
TRUNCATE TABLE ORDERS.ORDERS_SILVER.EMPLOYEE;

-- Inserting data into the silver table.
INSERT INTO ORDERS.ORDERS_SILVER.EMPLOYEE (
    EmployeeKey,
    WWIEmployeeID, 
    Employee, 
    PreferredName, 
    IsSalesPerson, 
    Photo, 
    ValidFrom, 
    ValidTo 
)
SELECT
    CONCAT(PersonID, TO_CHAR(ValidFrom, 'YYYYMMDDHH24MISSFF7')) AS EmployeeKey,
    PersonID AS WWIEmployeeID,
    FullName AS Employee,
    PreferredName,
    IsSalesPerson,
    Photo,
    ValidFrom,
    ValidTo
FROM
    ORDERS.ORDERS_BRONZE.PEOPLE
WHERE
    ORDERS.ORDERS_BRONZE.PEOPLE.ISEMPLOYEE <> 0;
