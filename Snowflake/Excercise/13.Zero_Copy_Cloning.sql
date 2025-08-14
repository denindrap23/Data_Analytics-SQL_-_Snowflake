-- 13.1 Cloning Schema & Databases
  
-- Cloning Schema
CREATE TRANSIENT SCHEMA OUR_FIRST_DB.COPIED_SCHEMA
CLONE OUR_FIRST_DB.PUBLIC;

SELECT * FROM COPIED_SCHEMA.CUSTOMERS;

CREATE TRANSIENT SCHEMA OUR_FIRST_DB.EXTERNAL_STAGES_COPIED
CLONE MANAGE_DB.EXTERNAL_STAGES;

-- Cloning Database
CREATE TRANSIENT DATABASE OUR_FIRST_DB_COPY
CLONE OUR_FIRST_DB;

DROP DATABASE OUR_FIRST_DB_COPY
DROP SCHEMA OUR_FIRST_DB.EXTERNAL_STAGES_COPIED
DROP SCHEMA OUR_FIRST_DB.COPIED_SCHEMA


-- 13.2 Cloning with Time Travel

-- Setting up table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.time_travel (
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

COPY INTO OUR_FIRST_DB.public.time_travel
FROM @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');

SELECT * FROM OUR_FIRST_DB.public.time_travel;

-- Update data

UPDATE OUR_FIRST_DB.public.time_travel
SET FIRST_NAME = 'Frank';

-- Using time travel
SELECT * FROM OUR_FIRST_DB.public.time_travel AT (OFFSET => -60*1);

-- Using time travel
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.time_travel_clone
CLONE OUR_FIRST_DB.public.time_travel AT (OFFSET => -60*1.5);

SELECT * FROM OUR_FIRST_DB.PUBLIC.time_travel_clone;

-- Update data again

UPDATE OUR_FIRST_DB.public.time_travel_clone
SET JOB = 'Snowflake Analyst';

-- Using time travel: Method 2 - before Query
SELECT * FROM OUR_FIRST_DB.public.time_travel_clone BEFORE (statement => '<your-query-id>');

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.time_travel_clone_of_clone
CLONE OUR_FIRST_DB.public.time_travel_clone BEFORE (statement => '<your-query-id>');

SELECT * FROM OUR_FIRST_DB.public.time_travel_clone_of_clone;
