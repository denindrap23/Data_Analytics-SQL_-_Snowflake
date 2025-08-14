-- 4.1 Create Stage & Load Raw (JSON)
  
-- First step: Load Raw JSON
CREATE OR REPLACE stage MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
     url='s3:--bucketsnowflake-jsondemo';

CREATE OR REPLACE file format MANAGE_DB.FILE_FORMATS.JSONFORMAT
    TYPE = JSON;
        
CREATE OR REPLACE table OUR_FIRST_DB.PUBLIC.JSON_RAW (
    raw_file variant);
    
COPY INTO OUR_FIRST_DB.PUBLIC.JSON_RAW
    FROM @MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
    file_format= MANAGE_DB.FILE_FORMATS.JSONFORMAT
    files = ('HR_data.json');
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


-- 4.2 Parsing & Analyze (JSON)

-- Selecting attribute/column
SELECT RAW_FILE:city FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT $1:first_name FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- Selecting attribute/column - formattted
SELECT RAW_FILE:first_name::string AS first_name  FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT RAW_FILE:id::int AS id  FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:id::int AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:last_name::STRING AS last_name,
    RAW_FILE:gender::STRING AS gender
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- Handling nested data
SELECT RAW_FILE:job AS job  FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


-- 4.3 Handling Nested Data (JSON)

-- Handling nested data  
SELECT RAW_FILE:job AS job  FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
      RAW_FILE:job.salary::INT AS salary
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:job.salary::INT AS salary,
    RAW_FILE:job.title::STRING AS title
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- Handling arreys

SELECT
    RAW_FILE:prev_company AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT
    RAW_FILE:prev_company[1]::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT
    ARRAY_SIZE(RAW_FILE:prev_company) AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:id::int AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:prev_company[0]::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::int AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:prev_company[1]::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
ORDER BY id;

  
-- 4.4 Dealing with Hierarchy (JSON)

SELECT 
    RAW_FILE:spoken_languages AS spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
     array_size(RAW_FILE:spoken_languages) AS spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
     RAW_FILE:first_name::STRING AS first_name,
     array_size(RAW_FILE:spoken_languages) AS spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:spoken_languages[0] AS First_language
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:spoken_languages[0] AS First_language
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:first_name::STRING AS First_name,
    RAW_FILE:spoken_languages[0].LANGUAGE::STRING AS First_language,
    RAW_FILE:spoken_languages[0].LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:id::int AS id,
    RAW_FILE:first_name::STRING AS First_name,
    RAW_FILE:spoken_languages[0].LANGUAGE::STRING AS First_language,
    RAW_FILE:spoken_languages[0].LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::int AS id,
    RAW_FILE:first_name::STRING AS First_name,
    RAW_FILE:spoken_languages[1].LANGUAGE::STRING AS First_language,
    RAW_FILE:spoken_languages[1].LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::int AS id,
    RAW_FILE:first_name::STRING AS First_name,
    RAW_FILE:spoken_languages[2].LANGUAGE::STRING AS First_language,
    RAW_FILE:spoken_languages[2].LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
ORDER BY ID;

SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.VALUE:LANGUAGE::STRING AS First_language,
    f.VALUE:LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(flatten(RAW_FILE:spoken_languages)) f;


-- 4.5 Insert the Final Data

-- Option 1: CREATE TABLE AS
CREATE OR REPLACE TABLE Languages AS
SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.VALUE:LANGUAGE::STRING AS First_language,
    f.VALUE:LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(flatten(RAW_FILE:spoken_languages)) f;

SELECT * FROM Languages;

TRUNCATE TABLE languages;

-- Option 2: INSERT INTO
INSERT INTO Languages
SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.VALUE:LANGUAGE::STRING AS First_language,
    f.VALUE:LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(flatten(RAW_FILE:spoken_languages)) f;

SELECT * FROM Languages;


-- 4.6 Querying PARQUET Data

-- Create file format and stage object  
CREATE OR REPLACE FILE FORMAT MANAGE_DB.FILE_FORMATS.PARQUET_FORMAT
    TYPE = 'parquet';

CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE
    url = 's3:--snowflakeparquetdemo'   
    FILE_FORMAT = MANAGE_DB.FILE_FORMATS.PARQUET_FORMAT;

-- Preview the data
LIST  @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE;   
    
SELECT * FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE;
    
-- File format in Queries
CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE
    url = 's3:--snowflakeparquetdemo'  
    
SELECT * 
FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE
(file_format => 'MANAGE_DB.FILE_FORMATS.PARQUET_FORMAT');

-- Quotes can be omitted in case of the current namespace
USE MANAGE_DB.FILE_FORMATS;

SELECT * 
FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE
(file_format => MANAGE_DB.FILE_FORMATS.PARQUET_FORMAT);

CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE
    url = 's3:--snowflakeparquetdemo'   
    FILE_FORMAT = MANAGE_DB.FILE_FORMATS.PARQUET_FORMAT;

-- Syntax for Querying unstructured data
SELECT 
    $1:__index_level_0__,
    $1:cat_id,
    $1:date,
    $1:"__index_level_0__",
    $1:"cat_id",
    $1:"d",
    $1:"date",
    $1:"dept_id",
    $1:"id",
    $1:"item_id",
    $1:"state_id",
    $1:"store_id",
    $1:"value"
FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE;

-- Date conversion 
SELECT 1;

SELECT DATE(365*60*60*24);

-- Querying with conversions and aliases  
SELECT 
    $1:__index_level_0__::INT AS index_level,
    $1:cat_id::VARCHAR(50) AS category,
    DATE($1:DATE::INT ) AS Date,
    $1:"dept_id"::VARCHAR(50) AS Dept_ID,
    $1:"id"::VARCHAR(50) AS ID,
    $1:"item_id"::VARCHAR(50) AS Item_ID,
    $1:"state_id"::VARCHAR(50) AS State_ID,
    $1:"store_id"::VARCHAR(50) AS Store_ID,
    $1:"value"::INT AS value
FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE;


-- 4.7 Loading PARQUET Data
  
-- Adding metadata
SELECT 
    $1:__index_level_0__::INT AS index_level,
    $1:cat_id::VARCHAR(50) AS category,
    DATE($1:DATE::INT ) AS Date,
    $1:"dept_id"::VARCHAR(50) AS Dept_ID,
    $1:"id"::VARCHAR(50) AS ID,
    $1:"item_id"::VARCHAR(50) AS Item_ID,
    $1:"state_id"::VARCHAR(50) AS State_ID,
    $1:"store_id"::VARCHAR(50) AS Store_ID,
    $1:"value"::INT AS value,
    METADATA$FILENAME AS FILENAME,
    METADATA$FILE_ROW_NUMBER AS ROWNUMBER,
    TO_TIMESTAMP_NTZ(current_timestamp) AS LOAD_DATE
FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE;

SELECT TO_TIMESTAMP_NTZ(current_timestamp);

-- Create destination table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.PARQUET_DATA (
    ROW_NUMBER INT,
    index_level INT,
    cat_id VARCHAR(50),
    date DATE,
    dept_id VARCHAR(50),
    id VARCHAR(50),
    item_id VARCHAR(50),
    state_id VARCHAR(50),
    store_id VARCHAR(50),
    value INT,
    Load_date timestamp DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    );

-- Load the parquet data
COPY INTO OUR_FIRST_DB.PUBLIC.PARQUET_DATA
    FROM (
        SELECT 
            METADATA$FILE_ROW_NUMBER,
            $1:__index_level_0__::INT,
            $1:cat_id::VARCHAR(50),
            DATE($1:date::INT),
            $1:"dept_id"::VARCHAR(50),
            $1:"id"::VARCHAR(50),
            $1:"item_id"::VARCHAR(50),
            $1:"state_id"::VARCHAR(50),
            $1:"store_id"::VARCHAR(50),
            $1:"value"::INT,
            TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
        FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE);
            
SELECT * FROM OUR_FIRST_DB.PUBLIC.PARQUET_DATA;
