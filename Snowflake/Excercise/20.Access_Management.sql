-- 20.1 Account Admin

--- User 1 ---
CREATE USER maria PASSWORD = '123' 
DEFAULT_ROLE = ACCOUNTADMIN 
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE ACCOUNTADMIN TO USER maria;

--- User 2 ---
CREATE USER frank PASSWORD = '123' 
DEFAULT_ROLE = SECURITYADMIN 
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE SECURITYADMIN TO USER frank;

--- User 3 ---
CREATE USER adam PASSWORD = '123' 
DEFAULT_ROLE = SYSADMIN 
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE SYSADMIN TO USER adam;


-- 20.2 Security Admin
  
-- SECURITYADMIN role --
--  Create and Manage Roles & Users --
-- Create Sales Roles & Users for SALES--
CREATE ROLE sales_admin;
CREATE ROLE sales_users;

-- Create hierarchy
GRANT ROLE sales_users TO ROLE sales_admin;

-- As per best practice assign roles to SYSADMIN
GRANT ROLE sales_admin TO ROLE SYSADMIN;

-- create sales user
CREATE USER simon_sales PASSWORD = '123' DEFAULT_ROLE =  sales_users 
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE sales_users TO USER simon_sales;

-- create user for sales administration
CREATE USER olivia_sales_admin PASSWORD = '123' DEFAULT_ROLE =  sales_admin
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE sales_admin TO USER  olivia_sales_admin;

-----------------------------------
-- Create Sales Roles & Users for HR--
CREATE ROLE hr_admin;
CREATE ROLE hr_users;

-- Create hierarchy
GRANT ROLE hr_users TO ROLE hr_admin;
-- This time we will not assign roles to SYSADMIN (against best practice)
-- grant role hr_admin to role SYSADMIN;

-- create hr user
CREATE USER oliver_hr PASSWORD = '123' DEFAULT_ROLE =  hr_users 
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE hr_users TO USER oliver_hr;

-- create user for sales administration
CREATE USER mike_hr_admin PASSWORD = '123' DEFAULT_ROLE =  hr_admin
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE hr_admin TO USER mike_hr_admin;


-- 20.3 System  Admin
  
-- SYSADMIN --
-- Create a warehouse of size X-SMALL
CREATE warehouse public_wh WITH
warehouse_size='X-SMALL'
auto_suspend=300 
auto_resume= true

-- grant usage to role public
GRANT USAGE ON warehouse public_wh TO ROLE PUBLIC

-- create a database accessible to everyone
CREATE database common_db;
GRANT USAGE ON database common_db TO ROLE PUBLIC

-- create sales database for sales
CREATE database sales_database;
GRANT ownership ON database sales_database TO ROLE sales_admin;
GRANT ownership ON SCHEMA sales_database.public TO TOLE sales_admin

SHOW DATABASES;

-- create database for hr
DROP database hr_db;
GRANT ownership ON database hr_db TO ROLE hr_admin;
GRANT ownership ON SCHEMA hr_db.public TO ROLE hr_admin


-- 20.4 Custom Role

USE ROLE SALES_ADMIN;
USE SALES_DATABASE;

-- Create table --
CREATE OR REPLACE table customers(
    id NUMBER,
    full_name VARCHAR, 
    email VARCHAR,
    phone VARCHAR,
    spent NUMBER,
    create_date DATE DEFAULT CURRENT_DATE
    );

-- insert values in table --
INSERT INTO customers (id, full_name, email,phone,spent)
VALUES
  (1,'Lewiss MacDwyer','lmacdwyer0@un.org','262-665-9168',140),
  (2,'Ty Pettingall','tpettingall1@mayoclinic.com','734-987-7120',254),
  (3,'Marlee Spadazzi','mspadazzi2@txnews.com','867-946-3659',120),
  (4,'Heywood Tearney','htearney3@patch.com','563-853-8192',1230),
  (5,'Odilia Seti','oseti4@globo.com','730-451-8637',143),
  (6,'Meggie Washtell','mwashtell5@rediff.com','568-896-6138',600);
  
SHOW TABLES;

-- query from table --
SELECT * FROM CUSTOMERS;
USE ROLE SALES_USERS;

-- grant usage to role
USE ROLE SALES_ADMIN;

GRANT USAGE ON DATABASE SALES_DATABASE TO ROLE SALES_USERS;
GRANT USAGE ON SCHEMA SALES_DATABASE.PUBLIC TO ROLE SALES_USERS;
GRANT SELECT ON TABLE SALES_DATABASE.PUBLIC.CUSTOMERS TO ROLE SALES_USERS


-- Validate privileges --
USE ROLE SALES_USERS;
SELECT * FROM CUSTOMERS;
DROP TABLE CUSTOMERS;
DELETE FROM CUSTOMERS;
SHOW TABLES;

-- grant DROP on table
USE ROLE SALES_ADMIN;
GRANT DELETE ON TABLE SALES_DATABASE.PUBLIC.CUSTOMERS TO ROLE SALES_USERS

USE ROLE SALES_ADMIN;


-- 20.5 User Admin

-- USERADMIN --
--- User 4 ---
CREATE USER ben PASSWORD = '123' 
DEFAULT_ROLE = ACCOUNTADMIN 
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE HR_ADMIN TO USER ben;

SHOW ROLES;

GRANT ROLE HR_ADMIN TO ROLE SYSADMIN;
