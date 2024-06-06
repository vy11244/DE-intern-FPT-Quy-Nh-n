/* 
  The query is done to convert from bronze data into silver data
     Database: WideWorldImporters
     Module: SALE
     Contributed Bronze Table: SALE.SALE_BRONZE.PEOPLE
     Silver table: SALE.SALE_SILVER.EMPLOYEE
     Done by: HungPQ33
     Last update when: 2024-03-14 06:30:00.0000000
     Update content: Edit primary key format and query template
*/
TRUNCATE TABLE IF EXISTS SALE.SALE_SILVER.EMPLOYEE; -- This statement clears out any existing data in the silver table if it exists.

-- This part begins the insertion of data into the silver table.
INSERT INTO SALE.SALE_SILVER.EMPLOYEE (
    EmployeeKey,
    WWIEmployeeID,
    Employee,
    PreferredName,
    IsSalesPerson,
    Photo,
    ValidFrom,
    ValidTo
)
-- This query is used to extract neccessary information from bronze table: People with condition that this person is an employee.
SELECT
    CONCAT(PersonID,TO_CHAR(ValidFrom,'YYYYMMDDHH24MISSFF7')) as EmployeeKey,
    PersonID AS WWIEmployeeID,
    FullName AS Employee,
    PreferredName,
    IsSalesPerson,
    Photo,
    ValidFrom,
    ValidTo
    
FROM
    SALE.SALE_BRONZE.PEOPLE as P
WHERE
    P.ISEMPLOYEE <> 0