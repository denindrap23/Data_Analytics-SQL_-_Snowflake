-- 19.1 Creating Data Masking Policy

USE DEMO_DB;
USE ROLE ACCOUNTADMIN;

-- Prepare table --
CREATE OR REPLACE table customers(
  id number,
  full_name varchar, 
  email varchar,
  phone varchar,
  spent number,
  create_date DATE DEFAULT CURRENT_DATE);

-- insert values in table --
INSERT INTO customers (id, full_name, email,phone,spent)
VALUES
  (1,'Lewiss MacDwyer','lmacdwyer0@un.org','262-665-9168',140),
  (2,'Ty Pettingall','tpettingall1@mayoclinic.com','734-987-7120',254),
  (3,'Marlee Spadazzi','mspadazzi2@txnews.com','867-946-3659',120),
  (4,'Heywood Tearney','htearney3@patch.com','563-853-8192',1230),
  (5,'Odilia Seti','oseti4@globo.com','730-451-8637',143),
  (6,'Meggie Washtell','mwashtell5@rediff.com','568-896-6138',600);

-- set up roles
CREATE OR REPLACE ROLE ANALYST_MASKED;
CREATE OR REPLACE ROLE ANALYST_FULL;

-- grant select on table to roles
GRANT SELECT ON TABLE DEMO_DB.PUBLIC.CUSTOMERS TO ROLE ANALYST_MASKED;
GRANT SELECT ON TABLE DEMO_DB.PUBLIC.CUSTOMERS TO ROLE ANALYST_FULL;

GRANT USAGE ON SCHEMA DEMO_DB.PUBLIC TO ROLE ANALYST_MASKED;
GRANT USAGE ON SCHEMA DEMO_DB.PUBLIC TO ROLE ANALYST_FULL;

-- grant warehouse access to roles
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_MASKED;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_FULL;

-- assign roles to a user
GRANT ROLE ANALYST_MASKED TO USER NIKOLAISCHULER;
GRANT ROLE ANALYST_FULL TO USER NIKOLAISCHULER;

-- Set up masking policy

CREATE OR REPLACE masking policy phone 
    AS (val varchar) RETURN varchar ->
            CASE 
              WHEN CURRENT_ROLE() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val
              ELSE '##-###-##'
            END;
  
-- Apply policy on a specific column 
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN phone 
SET MASKING POLICY PHONE;

-- Validating policies
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;

USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;


-- 19.2 Unset & Replace Policy

USE ROLE ACCOUNTADMIN;

--- 1) Apply policy to multiple columns
-- Apply policy on a specific column 
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name 
SET MASKING POLICY phone;

-- Apply policy on another specific column 
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN phone
SET MASKING POLICY phone;

--- 2) Replace or drop policy
DROP masking policy phone;

CREATE OR REPLACE masking policy phone 
    AS (val varchar) RETURN varchar ->
            CASE
              WHEN CURRENT_ROLE() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val
              ELSE CONCAT(LEFT(val,2),'*******')
            END;

-- List and describe policies
DESC MASKING POLICY phone;
SHOW MASKING POLICIES;

-- Show columns with applied policies
SELECT * FROM table(information_schema.policy_references(policy_name=>'phone'));


-- Remove policy before replacing/dropping 
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name 
SET MASKING POLICY phone;

ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN email
UNSET MASKING POLICY;

ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN phone
UNSET MASKING POLICY;

-- replace policy
CREATE OR REPLACE masking policy NAMES 
    AS (val varchar) RETURN varchar ->
            CASE
              WHEN CURRENT_ROLE() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val
              ELSE CONCAT(LEFT(val,2),'*******')
            END;

-- apply policy
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name
SET MASKING POLICY NAMES;

-- Validating policies
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;

USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;


-- 19.3 Alter Existing Policy

-- Alter existing policies 
USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;

USE ROLE ACCOUNTADMIN;

ALTER masking policy phone SET body ->
  CASE        
      WHEN CURRENT_ROLE() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val
      ELSE '**-**-**'
  END;

ALTER TABLE CUSTOMERS MODIFY COLUMN email UNSET MASKING POLICY;


-- 19.4 Real-Life Examples

--- More examples - 1 - ---
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE masking policy emails 
    AS (val varchar) RETURN varchar ->
        CASE
            WHEN CURRENT_ROLE() IN ('ANALYST_FULL') THEN val
            WHEN CURRENT_ROLE() IN ('ANALYST_MASKED') THEN regexp_replace(val,'.+\@','*****@') -- leave email domain unmasked
            ELSE '********'
        END;

-- apply policy
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN email
SET MASKING POLICY emails;

-- Validating policies
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;

USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;

USE ROLE ACCOUNTADMIN;

--- More examples - 2 - ---
CREATE OR REPLACE masking policy sha2 
    AS (val varchar) RETURN varchar ->
        CASE
            WHEN CURRENT_ROLE() IN ('ANALYST_FULL') then val
            ELSE sha2(val) -- return hash of the column value
        END;

-- apply policy
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name
SET MASKING POLICY sha2;

ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name
UNSET MASKING POLICY;

-- Validating policies
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;

USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;

USE ROLE ACCOUNTADMIN;

--- More examples - 3 - ---
CREATE OR REPLACE masking policy dates 
    AS (val date) RETURN date ->
        CASE
            WHEN CURRENT_ROLE() IN ('ANALYST_FULL') THEN val
            ELSE date_from_parts(0001, 01, 01)::date -- returns 0001-01-01 00:00:00.000
        END;

-- Apply policy on a specific column 
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN create_date 
SET MASKING POLICY dates;

-- Validating policies

USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;

USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;
