-- 10.1 Using Time Travel

-- Setting up table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );
    
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file
    type = csv
    field_delimiter = ','
    skip_header = 1;
    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3:--data-snowflake-fundamentals/time-travel/'
    file_format = MANAGE_DB.file_formats.csv_file;
    
LIST @MANAGE_DB.external_stages.time_travel_stage;

COPY INTO OUR_FIRST_DB.public.test
FROM @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv')

SELECT * FROM OUR_FIRST_DB.public.test;

-- Use-case: Update data (by mistake)
UPDATE OUR_FIRST_DB.public.test
SET FIRST_NAME = 'Joyen';

-- Using time travel: Method 1 - 2 minutes back
SELECT * FROM OUR_FIRST_DB.public.test at (OFFSET => -60*1.5);

-- Using time travel: Method 2 - before timestamp
SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => '2025-07-07 17:47:50.581'::timestamp);

-- Setting up table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );

COPY INTO OUR_FIRST_DB.public.test
FROM @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');

SELECT * FROM OUR_FIRST_DB.public.test;

-- Setting up UTC time for convenience
ALTER SESSION SET TIMEZONE ='UTC'
SELECT DATEADD(DAY, 1, CURRENT_TIMESTAMP);

UPDATE OUR_FIRST_DB.public.test
SET Job = 'Data Scientist';

SELECT * FROM OUR_FIRST_DB.public.test;

SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => '2025-07-08 07:30:47.145'::timestamp)

-- Using time travel: Method 3 - before Query ID
-- Preparing table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );

COPY INTO OUR_FIRST_DB.public.test
FROM @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');

SELECT * FROM OUR_FIRST_DB.public.test;

-- Altering table (by mistake)
UPDATE OUR_FIRST_DB.public.test
SET EMAIL = null;

SELECT * FROM OUR_FIRST_DB.public.test;

SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9ee5-0500-8473-0043-4d8300073062');


-- 10.2 Restoring in Time Travel

-- Use-case: Update data (by mistake)
UPDATE OUR_FIRST_DB.public.test
SET LAST_NAME = 'Tyson';

UPDATE OUR_FIRST_DB.public.test
SET JOB = 'Data Analyst';

SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9eea-0500-845a-0043-4d830007402a');

-- Bad method
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test as
SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9eea-0500-845a-0043-4d830007402a');

SELECT * FROM OUR_FIRST_DB.public.test;

CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test as
SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9eea-0500-8473-0043-4d830007307a');

-- Good method
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test_backup AS
SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9ef0-0500-8473-0043-4d830007309a');

TRUNCATE OUR_FIRST_DB.public.test;

INSERT INTO OUR_FIRST_DB.public.test
SELECT * FROM OUR_FIRST_DB.public.test_backup;

SELECT * FROM OUR_FIRST_DB.public.test;


-- 10.3 Undrop Table
-- UNDROP command - Tables
DROP TABLE OUR_FIRST_DB.public.customers;

SELECT * FROM OUR_FIRST_DB.public.customers;

UNDROP TABLE OUR_FIRST_DB.public.customers;

-- UNDROP command - Schemas
DROP SCHEMA OUR_FIRST_DB.public;

SELECT * FROM OUR_FIRST_DB.public.customers;

UNDROP SCHEMA OUR_FIRST_DB.public;

-- UNDROP command - Database
DROP DATABASE OUR_FIRST_DB;

SELECT * FROM OUR_FIRST_DB.public.customers;

UNDROP DATABASE OUR_FIRST_DB;

-- Restore replaced table 
UPDATE OUR_FIRST_DB.public.customers
SET LAST_NAME = 'Tyson';

UPDATE OUR_FIRST_DB.public.customers
SET JOB = 'Data Analyst';

-- Undroping a with a name that already exists
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.customers as
SELECT * FROM OUR_FIRST_DB.public.customers before (statement => '019b9f7c-0500-851b-0043-4d83000762be')

SELECT * FROM OUR_FIRST_DB.public.customers

UNDROP table OUR_FIRST_DB.public.customers;

ALTER TABLE OUR_FIRST_DB.public.customers
RENAME TO OUR_FIRST_DB.public.customers_wrong;

DESC table OUR_FIRST_DB.public.customers;


-- 10.4 Time Travel Cost

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

-- Query time travel storage
SELECT 	ID, 
	TABLE_NAME, 
	TABLE_SCHEMA,
        TABLE_CATALOG,
	ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED_GB,
	TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY STORAGE_USED_GB DESC,TIME_TRAVEL_STORAGE_USED_GB DESC;
