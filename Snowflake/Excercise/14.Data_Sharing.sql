-- 14.1 Using Data Sharing
  
CREATE OR REPLACE DATABASE DATA_S;

CREATE OR REPLACE STAGE aws_stage
    url='s3:--bucketsnowflakes3';

-- List files in stage
LIST @aws_stage;

-- Create table
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID	VARCHAR(30),
    AMOUNT	NUMBER(38,0),
    PROFIT	NUMBER(38,0),
    QUANTITY	NUMBER(38,0),
    CATEGORY	VARCHAR(30),
    SUBCATEGORY	VARCHAR(30)
    );

-- Load data using copy command
COPY INTO ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*OrderDetails.*';
    
SELECT * FROM ORDERS;

-- Create a share object
CREATE OR REPLACE SHARE ORDERS_SHARE;

---- Setup Grants ----
-- Grant usage on database
GRANT USAGE ON DATABASE DATA_S TO SHARE ORDERS_SHARE; 

-- Grant usage on schema
GRANT USAGE ON SCHEMA DATA_S.PUBLIC TO SHARE ORDERS_SHARE; 

-- Grant SELECT on table
GRANT SELECT ON TABLE DATA_S.PUBLIC.ORDERS TO SHARE ORDERS_SHARE; 

-- Validate Grants
SHOW GRANTS TO SHARE ORDERS_SHARE;

---- Add Consumer Account ----
ALTER SHARE ORDERS_SHARE ADD ACCOUNT=<consumer-account-id>;


-- 14.2 Create Reader Account

-- Create Reader Account --
CREATE MANAGED ACCOUNT tech_joy_account
ADMIN_NAME = tech_joy_admin,
ADMIN_PASSWORD = 'set-pwd',
TYPE = READER;

-- Make sure to have selected the role of accountadmin
-- Show accounts
SHOW MANAGED ACCOUNTS;

-- Share the data -- 
ALTER SHARE ORDERS_SHARE 
ADD ACCOUNT = <reader-account-id>;

ALTER SHARE ORDERS_SHARE 
ADD ACCOUNT =  <reader-account-id>
SHARE_RESTRICTIONS=false;

-- Create database from share --
-- Show ALL shares (consumer & producers)
SHOW SHARES;

-- See details on share
DESC SHARE QNA46172.ORDERS_SHARE;

-- Create a database in consumer account using the share
CREATE DATABASE DATA_SHARE_DB FROM SHARE <account_name_producer>.ORDERS_SHARE;

-- Validate table access
SELECT * FROM  DATA_SHARE_DB.PUBLIC.ORDERS

-- Setup virtual warehouse
CREATE WAREHOUSE READ_WH WITH
WAREHOUSE_SIZE='X-SMALL'
AUTO_SUSPEND = 180
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

-- Create and set up users --
-- Create user
CREATE USER MYRIAM PASSWORD = 'difficult_passw@ord=123'

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE READ_WH TO ROLE PUBLIC;

-- Grating privileges on a Shared Database for other users
GRANT IMPORTED PRIVILEGES ON DATABASE DATA_SHARE_DB TO ROLE PUBLIC;


-- 14.3 Secure View

-- Create database & table --
CREATE OR REPLACE DATABASE CUSTOMER_DB;

CREATE OR REPLACE TABLE CUSTOMER_DB.public.customers (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );
  
-- Stage and file format
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file
    type = csv
    field_delimiter = ','
    skip_header = 1;
    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3:--data-snowflake-fundamentals/time-travel/'
    file_format = MANAGE_DB.file_formats.csv_file;
    
LIST  @MANAGE_DB.external_stages.time_travel_stage;

-- Copy data and insert in table
COPY INTO CUSTOMER_DB.public.customers
FROM @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');

SELECT * FROM  CUSTOMER_DB.PUBLIC.CUSTOMERS;

-- Create VIEW -- 
CREATE OR REPLACE VIEW CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW AS
SELECT 
FIRST_NAME,
LAST_NAME,
EMAIL
FROM CUSTOMER_DB.PUBLIC.CUSTOMERS
WHERE JOB != 'DATA SCIENTIST'; 

-- Grant usage & SELECT --
GRANT USAGE ON DATABASE CUSTOMER_DB TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA CUSTOMER_DB.PUBLIC TO ROLE PUBLIC;
GRANT SELECT ON TABLE CUSTOMER_DB.PUBLIC.CUSTOMERS TO ROLE PUBLIC;
GRANT SELECT ON VIEW CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW TO ROLE PUBLIC;

SHOW VIEWS LIKE '%CUSTOMER%';

-- Create SECURE VIEW -- 

CREATE OR REPLACE SECURE VIEW CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW_SECURE AS
    SELECT 
        FIRST_NAME,
        LAST_NAME,
        EMAIL
    FROM CUSTOMER_DB.PUBLIC.CUSTOMERS
    WHERE JOB != 'DATA SCIENTIST'; 

GRANT SELECT ON VIEW CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW_SECURE TO ROLE PUBLIC;

SHOW VIEWS LIKE '%CUSTOMER%';


-- 14.4 Sharing Views

SHOW SHARES;

-- Create share object
CREATE OR REPLACE SHARE VIEW_SHARE;

-- Grant usage on dabase & schema
GRANT USAGE ON DATABASE CUSTOMER_DB TO SHARE VIEW_SHARE;
GRANT USAGE ON SCHEMA CUSTOMER_DB.PUBLIC TO SHARE VIEW_SHARE;

-- Grant select on view
GRANT SELECT ON VIEW  CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW TO SHARE VIEW_SHARE;
GRANT SELECT ON VIEW  CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW_SECURE TO SHARE VIEW_SHARE;

-- Add account to share
ALTER SHARE VIEW_SHARE
ADD ACCOUNT=KAA74702
