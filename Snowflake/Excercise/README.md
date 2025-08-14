# 1. Create Data
  
## 1.1 Creating the table / Meta data

```sql
CREATE TABLE "OUR_FIRST_DB"."PUBLIC"."LOAN_PAYMENT" (
  "Loan_ID" STRING,
  "loan_status" STRING,
  "Principal" STRING,
  "terms" STRING,
  "effective_date" STRING,
  "due_date" STRING,
  "paid_off_time" STRING,
  "past_due_days" STRING,
  "age" STRING,
  "education" STRING,
  "Gender" STRING);
 ```
 
## 1.2 Check that table is empty

```sql
 USE DATABASE OUR_FIRST_DB;

 SELECT * FROM LOAN_PAYMENT;
```
 
## 1.3 Loading the data from S3 bucket

```sql
 COPY INTO LOAN_PAYMENT
    FROM s3:--bucketsnowflakes3/Loan_payments_data.csv
    file_format = (type = csv 
                   field_delimiter = ',' 
                   skip_header=1);
```
    
## 1.4 Validate

```sql
 SELECT * FROM LOAN_PAYMENT;
```

# 2. Loading Data

## 2.1 Create Stage

```sql
-- Database to manage stage objects, fileformats etc.
CREATE OR REPLACE DATABASE MANAGE_DB;

CREATE OR REPLACE SCHEMA external_stages;

-- Creating external stage
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3:--bucketsnowflakes3'
    credentials=(aws_key_id='ABCD_DUMMY_ID' aws_secret_key='1234abcd_key');

-- Description of external stage
DESC STAGE MANAGE_DB.external_stages.aws_stage; 
      
-- Alter external stage   
ALTER STAGE aws_stage
    SET credentials=(aws_key_id='XYZ_DUMMY_ID' aws_secret_key='987xyz');
    
-- Publicly accessible staging area    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3:--bucketsnowflakes3';

-- List files in stage
LIST @aws_stage;

--Load data using copy command
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';
```

## 2.2 Copy Command

```sql
-- Creating ORDERS table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS;
   
-- First copy command
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @aws_stage
    file_format = (type = csv field_delimiter=',' skip_header=1);

-- Copy command with fully qualified stage object
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1);

-- List files contained in stage
LIST @MANAGE_DB.external_stages.aws_stage;    

-- Copy command with specified file(s)
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails.csv');
    
-- Copy command with pattern for file names
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';
```

## 2.3 Transforming Data

```sql
-- Transforming using the SELECT statement
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM (select s.$1, s.$2 from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');

-- Example 1 - Table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT
    );
      
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;
   
-- Example 2 - Table    
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)  
    );

-- Example 2 - Copy Command using a SQL function (subset of functions available)
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM (SELECT 
            s.$1,
            s.$2, 
            s.$3,
            CASE WHEN CAST(s.$3 as int) < 0 THEN 'not profitable' ELSE 'profitable' END 
          FROM @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;

-- Example 3 - Table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    CATEGORY_SUBSTRING VARCHAR(5)  
    );

-- Example 3 - Copy Command using a SQL function (subset of functions available)
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM (SELECT 
            s.$1,
            s.$2, 
            s.$3,
            substring(s.$5,1,5) 
          FROM @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;
```

## 2.4 More Transformation

```sql
-- Example 3 - Table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
    );

-- Example 4 - Using subset of columns
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX (ORDER_ID,PROFIT)
    FROM (SELECT 
            s.$1,
            s.$3
          FROM @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX;

-- Example 5 - Table Auto increment

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID number autoincrement start 1 increment 1,
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
    );

-- Example 5 - Auto increment ID

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX (PROFIT,AMOUNT)
    FROM (SELECT 
            s.$2,
            s.$3
          FROM @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX WHERE ORDER_ID > 15;
   
DROP TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;
```

## 2.5 Copy Option & ON_ERROR

```sql
-- Create new stage
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage_errorex
      url='s3:--bucketsnowflakes4'
 
-- List files in stage
LIST @MANAGE_DB.external_stages.aws_stage_errorex;
 
-- Create example table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));
 
-- Demonstrating error message
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv');
    
-- Validating table is empty    
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX    
    
-- Error handling using the ON_ERROR option
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'CONTINUE';
    
-- Validating results and truncating table 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX
SELECT COUNT(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;

-- Error handling using the ON_ERROR option = ABORT_STATEMENT (default)
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'ABORT_STATEMENT';

-- Validating results and truncating table 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX
SELECT COUNT(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;

-- Error handling using the ON_ERROR option = SKIP_FILE
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE';
    
-- Validating results and truncating table 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX
SELECT COUNT(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;    
    
-- Error handling using the ON_ERROR option = SKIP_FILE_<number>
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE_2';    
    
-- Validating results and truncating table 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX
SELECT COUNT(*) FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX

TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX;    

-- Error handling using the ON_ERROR option = SKIP_FILE_<number>
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE_0.5%'; 
  
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_EX

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = SKIP_FILE_3 
    SIZE_LIMIT = 30;
```

## 2.6 File Format Object

```sql
-- Specifying file_format in Copy command
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format = (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 
    
-- Creating table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );    
    
-- Creating schema to keep things organized
CREATE OR REPLACE SCHEMA MANAGE_DB.file_formats;

-- Creating file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.my_file_format;

-- See properties of file format object
DESC file format MANAGE_DB.file_formats.my_file_format;

-- Using file format object in Copy command       
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (FORMAT_NAME=MANAGE_DB.file_formats.my_file_format)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 

-- Altering file format object
ALTER file format MANAGE_DB.file_formats.my_file_format
    SET SKIP_HEADER = 1;
    
-- Defining properties on creation of file format object   
CREATE OR REPLACE file format MANAGE_DB.file_formats.my_file_format
    TYPE=JSON,
    TIME_FORMAT=AUTO;    
    
-- See properties of file format object    
DESC file format MANAGE_DB.file_formats.my_file_format;   
  
-- Using file format object in Copy command       
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (FORMAT_NAME=MANAGE_DB.file_formats.my_file_format)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 

-- Altering the type of a file format is not possible
ALTER file format MANAGE_DB.file_formats.my_file_format
SET TYPE = CSV;

-- Recreate file format (default = CSV)
CREATE OR REPLACE file format MANAGE_DB.file_formats.my_file_format

-- See properties of file format object    
DESC file format MANAGE_DB.file_formats.my_file_format;   

-- Truncate table
TRUNCATE table OUR_FIRST_DB.PUBLIC.ORDERS_EX;

-- Overwriting properties of file format object      
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM  @MANAGE_DB.external_stages.aws_stage_errorex
    file_format = (FORMAT_NAME= MANAGE_DB.file_formats.my_file_format  field_delimiter = ',' skip_header=1 )
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 

DESC STAGE MANAGE_DB.external_stages.aws_stage_errorex;
```

# 3. Copy Options

## 3.1 Validation Mode

```sql
---- VALIDATION_MODE ----
-- Prepare database & table
CREATE OR REPLACE DATABASE COPY_DB;

CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

-- Prepare stage object
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3:--snowflakebucket-copyoption/size/';
  
LIST @COPY_DB.PUBLIC.aws_stage_copy;
      
--Load data using copy command
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;
       
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_5_ROWS;
```
  
## 3.2 Rejected Records

```sql
---- Use files with errors ----
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3:--snowflakebucket-copyoption/returnfailed/';

LIST @COPY_DB.PUBLIC.aws_stage_copy;    

COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;

COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_1_rows;
    
-------------- Working with error results -----------

---- 1) Saving rejected files after VALIDATION_MODE ---- 
CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;

-- Storing rejected /failed results in a table
CREATE OR REPLACE TABLE rejected AS 
select rejected_record from table(result_scan(last_query_id()));

INSERT INTO rejected
SELECT rejected_record FROM TABLE(result_scan(last_query_id()));

SELECT * FROM rejected;

---- 2) Saving rejected files without VALIDATION_MODE ---- 
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    ON_ERROR=CONTINUE;
   
SELECT * FROM TABLE(validate(orders, job_id => '_last'));

--## 3) Working with rejected records ---- 
SELECT REJECTED_RECORD FROM rejected;

CREATE OR REPLACE TABLE rejected_values as
SELECT 
  SPLIT_PART(rejected_record,',',1) as ORDER_ID, 
  SPLIT_PART(rejected_record,',',2) as AMOUNT, 
  SPLIT_PART(rejected_record,',',3) as PROFIT, 
  SPLIT_PART(rejected_record,',',4) as QUATNTITY, 
  SPLIT_PART(rejected_record,',',5) as CATEGORY, 
  SPLIT_PART(rejected_record,',',6) as SUBCATEGORY
FROM 
  rejected; 

SELECT * FROM rejected_values;
```

## 3.3 Size Limit

```sql
---- SIZE_LIMIT ----
-- Prepare database & table
CREATE OR REPLACE DATABASE COPY_DB;

CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );
    
-- Prepare stage object
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3:--snowflakebucket-copyoption/size/';
     
-- List files in stage
LIST @aws_stage_copy;

--Load data using copy command
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    SIZE_LIMIT=20000;
```

## 3.4 Return Failed Only

```sql
---- RETURN_FAILED_ONLY ----
CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

-- Prepare stage object
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3:--snowflakebucket-copyoption/returnfailed/';
  
LIST @COPY_DB.PUBLIC.aws_stage_copy;
    
--Load data using copy command
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    RETURN_FAILED_ONLY = TRUE;
   
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    ON_ERROR =CONTINUE
    RETURN_FAILED_ONLY = TRUE;

-- Default = FALSE

CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    ON_ERROR =CONTINUE;
```

## 3.5 Truncate Columns

```sql
---- TRUNCATECOLUMNS ----
CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(10),
    SUBCATEGORY VARCHAR(30)
    );

-- Prepare stage object
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3:--snowflakebucket-copyoption/size/';
  
LIST @COPY_DB.PUBLIC.aws_stage_copy;
     
--Load data using copy command
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';

COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    TRUNCATECOLUMNS = true; 
       
SELECT * FROM ORDERS; 
```

## 3.6 Force

```sql
---- FORCE ----
CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));

-- Prepare stage object
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3:--snowflakebucket-copyoption/size/';
  
LIST @COPY_DB.PUBLIC.aws_stage_copy
    
--Load data using copy command
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'

-- Not possible to load file that have been loaded and data has not been modified
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
   
SELECT * FROM ORDERS;    

-- Using the FORCE option

COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    FORCE = TRUE;
```

## 3.7 Load History

```sql
-- Query load history within a database --
USE COPY_DB;

SELECT * FROM information_schema.load_history;

-- Query load history gloabally from SNOWFLAKE database --
SELECT * FROM snowflake.account_usage.load_history;

-- Filter on specific table & schema
SELECT * FROM snowflake.account_usage.load_history
  WHERE schema_name='PUBLIC' AND
  table_name='ORDERS';
    
-- Filter on specific table & schema
SELECT * FROM snowflake.account_usage.load_history
  WHERE schema_name='PUBLIC' AND
  table_name='ORDERS' AND
  error_count > 0;
    
-- Filter on specific table & schema
SELECT * FROM snowflake.account_usage.load_history
WHERE DATE(LAST_LOAD_TIME) <= DATEADD(days,-1,CURRENT_DATE);
```

# 4. Loading Unstructure Data

## 4.1 Create Stage & Load Raw (JSON)

```sql
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
```

## 4.2 Parsing & Analyze (JSON)

```sql
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
```

## 4.3 Handling Nested Data (JSON)

```sql
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
```
  
## 4.4 Dealing with Hierarchy (JSON)

```sql
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
```

## 4.5 Insert the Final Data

```sql
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
```

## 4.6 Querying PARQUET Data

```sql
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
```

## 4.7 Loading PARQUET Data

```sql
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
```

# 5. Perfomance

## 5.1 Dedicated VW

```sql
--  Create virtual warehouse for data scientist & DBA
-- Data Scientists
CREATE WAREHOUSE DS_WH 
WITH WAREHOUSE_SIZE = 'SMALL'
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';

-- DBA
CREATE WAREHOUSE DBA_WH 
WITH WAREHOUSE_SIZE = 'XSMALL'
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';

-- Create role for Data Scientists & DBAs
CREATE ROLE DATA_SCIENTIST;
GRANT USAGE ON WAREHOUSE DS_WH TO ROLE DATA_SCIENTIST;

CREATE ROLE DBA;
GRANT USAGE ON WAREHOUSE DBA_WH TO ROLE DBA;

-- Setting up users with roles
-- Data Scientists
CREATE USER DS1 PASSWORD = 'DS1' LOGIN_NAME = 'DS1' DEFAULT_ROLE='DATA_SCIENTIST' DEFAULT_WAREHOUSE = 'DS_WH'  MUST_CHANGE_PASSWORD = FALSE;
CREATE USER DS2 PASSWORD = 'DS2' LOGIN_NAME = 'DS2' DEFAULT_ROLE='DATA_SCIENTIST' DEFAULT_WAREHOUSE = 'DS_WH'  MUST_CHANGE_PASSWORD = FALSE;
CREATE USER DS3 PASSWORD = 'DS3' LOGIN_NAME = 'DS3' DEFAULT_ROLE='DATA_SCIENTIST' DEFAULT_WAREHOUSE = 'DS_WH'  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE DATA_SCIENTIST TO USER DS1;
GRANT ROLE DATA_SCIENTIST TO USER DS2;
GRANT ROLE DATA_SCIENTIST TO USER DS3;

-- DBAs
CREATE USER DBA1 PASSWORD = 'DBA1' LOGIN_NAME = 'DBA1' DEFAULT_ROLE='DBA' DEFAULT_WAREHOUSE = 'DBA_WH'  MUST_CHANGE_PASSWORD = FALSE;
CREATE USER DBA2 PASSWORD = 'DBA2' LOGIN_NAME = 'DBA2' DEFAULT_ROLE='DBA' DEFAULT_WAREHOUSE = 'DBA_WH'  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE DBA TO USER DBA1;
GRANT ROLE DBA TO USER DBA2;

-- Drop objects again
DROP USER DBA1;
DROP USER DBA2;

DROP USER DS1;
DROP USER DS2;
DROP USER DS3;

DROP ROLE DATA_SCIENTIST;
DROP ROLE DBA;

DROP WAREHOUSE DS_WH;
DROP WAREHOUSE DBA_WH;
```

## 5.2 Scalling Out

```sql
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.WEB_SITE T1
CROSS JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.WEB_SITE T2
CROSS JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.WEB_SITE T3
CROSS JOIN (SELECT TOP 57 * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.WEB_SITE)  T4;
```

## 5.3 Caching

```sql
SELECT AVG(C_BIRTH_YEAR) FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER;

-- Setting up an additional user
CREATE ROLE DATA_SCIENTIST;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_SCIENTIST;

CREATE USER DS1 PASSWORD = 'DS1' LOGIN_NAME = 'DS1' DEFAULT_ROLE='DATA_SCIENTIST' DEFAULT_WAREHOUSE = 'DS_WH'  MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE DATA_SCIENTIST TO USER DS1;
```

## 5.4 Clustering

```sql
-- Publicly accessible staging area    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3:--bucketsnowflakes3';

-- List files in stage
LIST @MANAGE_DB.external_stages.aws_stage;

--Load data using copy command
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*OrderDetails.*';
    
-- Create table
CREATE OR REPLACE TABLE ORDERS_CACHING (
    ORDER_ID	VARCHAR(30),
    AMOUNT	NUMBER(38,0),
    PROFIT	NUMBER(38,0),
    QUANTITY	NUMBER(38,0),
    CATEGORY	VARCHAR(30),
    SUBCATEGORY	VARCHAR(30),
    DATE DATE
    );    

INSERT INTO ORDERS_CACHING 
SELECT
    t1.ORDER_ID,
    t1.AMOUNT,
    t1.PROFIT,
    t1.QUANTITY,
    t1.CATEGORY,
    t1.SUBCATEGORY,
    DATE(UNIFORM(1500000000,1700000000,(RANDOM())))
FROM ORDERS t1
CROSS JOIN (SELECT * FROM ORDERS) t2
CROSS JOIN (SELECT TOP 100 * FROM ORDERS) t3;

-- Query Performance before Cluster Key
SELECT * FROM ORDERS_CACHING  WHERE DATE = '2020-06-09';

-- Adding Cluster Key & Compare the result
ALTER TABLE ORDERS_CACHING CLUSTER BY (DATE); 

SELECT * FROM ORDERS_CACHING  WHERE DATE = '2020-01-05';

-- Not ideal clustering & adding a different Cluster Key using function
SELECT * FROM ORDERS_CACHING  WHERE MONTH(DATE)=11;

ALTER TABLE ORDERS_CACHING CLUSTER BY (MONTH(DATE));
```

# 6. Loading AWS

## 6.1 Create Storage Integration

```sql  
-- Create storage integration object
CREATE OR REPLACE storage integration s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = ''
    STORAGE_ALLOWED_LOCATIONS = ('s3:--<your-bucket-name>/<your-path>/', 's3:--<your-bucket-name>/<your-path>/')
    COMMENT = 'This an optional comment'; 
   
-- See storage integration properties to fetch external_id so we can update it in S3
DESC integration s3_int;
```

## 6.2 Load Data S3

```sql
-- Create table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.movie_titles (
    show_id STRING,
    type STRING,
    title STRING,
    director STRING,
    cast STRING,
    country STRING,
    date_added STRING,
    release_year STRING,
    rating STRING,
    duration STRING,
    listed_in STRING,
    description STRING
    );
  
-- Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;
    
-- Create stage object with integration object & file format object
CREATE OR REPLACE stage MANAGE_DB.external_stages.csv_folder
    URL = 's3:--<your-bucket-name>/<your-path>/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat;

-- Use Copy command       
COPY INTO OUR_FIRST_DB.PUBLIC.movie_titles
    FROM @MANAGE_DB.external_stages.csv_folder;
  
-- Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE    
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';  
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.movie_titles;
```

## 6.3 Handling JSON

```sql
-- Taming the JSON file
-- First query from S3 Bucket   
SELECT * FROM @MANAGE_DB.external_stages.json_folder;

-- Introduce columns 
SELECT 
    $1:asin,
    $1:helpful,
    $1:overall,
    $1:reviewText,
    $1:reviewTime,
    $1:reviewerID,
    $1:reviewTime,
    $1:reviewerName,
    $1:summary,
    $1:unixReviewTime
FROM @MANAGE_DB.external_stages.json_folder;

-- Format columns & use DATE function
SELECT 
    $1:asin::STRING AS ASIN,
    $1:helpful AS helpful,
    $1:overall AS overall,
    $1:reviewText::STRING AS reviewtext,
    $1:reviewTime::STRING,
    $1:reviewerID::STRING,
    $1:reviewTime::STRING,
    $1:reviewerName::STRING,
    $1:summary::STRING,
    DATE($1:unixReviewTime::INT) AS Revewtime
FROM @MANAGE_DB.external_stages.json_folder;

-- Format columns & handle custom date 
SELECT 
    $1:asin::STRING AS ASIN,
    $1:helpful AS helpful,
    $1:overall AS overall,
    $1:reviewText::STRING as reviewtext,
    DATE_FROM_PARTS( <YEAR>, <MONTH>, <DAY> )
    $1:reviewTime::STRING,
    $1:reviewerID::STRING,
    $1:reviewTime::STRING,
    $1:reviewerName::STRING,
    $1:summary::STRING,
    DATE($1:unixReviewTime::INT) AS Revewtime
FROM @MANAGE_DB.external_stages.json_folder;

-- Use DATE_FROM_PARTS and see another difficulty
SELECT 
    $1:asin::STRING AS ASIN,
    $1:helpful AS helpful,
    $1:overall AS overall,
    $1:reviewText::STRING AS reviewtext,
    DATE_FROM_PARTS(RIGHT($1:reviewTime::STRING,4), LEFT($1:reviewTime::STRING,2), SUBSTRING($1:reviewTime::STRING,4,2)),
    $1:reviewerID::STRING,
    $1:reviewTime::STRING,
    $1:reviewerName::STRING,
    $1:summary::STRING,
    DATE($1:unixReviewTime::INT) AS unixRevewtime
FROM @MANAGE_DB.external_stages.json_folder;

-- Use DATE_FROM_PARTS and handle the case difficulty
SELECT 
    $1:asin::STRING AS ASIN,
    $1:helpful AS helpful,
    $1:overall AS overall,
    $1:reviewText::STRING AS reviewtext,
    DATE_FROM_PARTS( 
      RIGHT($1:reviewTime::STRING,4), 
      LEFT($1:reviewTime::STRING,2), 
      CASE WHEN SUBSTRING($1:reviewTime::STRING,5,1)=',' 
           THEN SUBSTRING($1:reviewTime::STRING,4,1) 
           ELSE SUBSTRING($1:reviewTime::STRING,4,2) END),
    $1:reviewerID::STRING,
    $1:reviewTime::STRING,
    $1:reviewerName::STRING,
    $1:summary::STRING,
    DATE($1:unixReviewTime::INT) AS UnixRevewtime
FROM @MANAGE_DB.external_stages.json_folder;

-- Create destination table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.reviews (
    asin STRING,
    helpful STRING,
    overall STRING,
    reviewtext STRING,
    reviewtime DATE,
    reviewerid STRING,
    reviewername STRING,
    summary STRING,
    unixreviewtime DATE
    );

-- Copy transformed data into destination table
COPY INTO OUR_FIRST_DB.PUBLIC.reviews
    FROM (
      SELECT 
        $1:asin::STRING AS ASIN,
        $1:helpful AS helpful,
        $1:overall AS overall,
        $1:reviewText::STRING AS reviewtext,
        DATE_FROM_PARTS( 
           RIGHT($1:reviewTime::STRING,4), 
           LEFT($1:reviewTime::STRING,2), 
           CASE WHEN SUBSTRING($1:reviewTime::STRING,5,1)=',' 
                THEN SUBSTRING($1:reviewTime::STRING,4,1) 
                ELSE SUBSTRING($1:reviewTime::STRING,4,2) END),
        $1:reviewerID::STRING,
        $1:reviewerName::STRING,
        $1:summary::STRING,
        DATE($1:unixReviewTime::INT) Revewtime
      FROM @MANAGE_DB.external_stages.json_folder
    );

-- Validate results
SELECT * FROM OUR_FIRST_DB.PUBLIC.reviews;
```

# 7. Loading Azure

## 7.1 Create Integration

```sql
USE DATABASE DEMO_DB;
-- create integration object that contains the access information
CREATE STORAGE INTEGRATION azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '9ecede0b-0e07-4da4-8047-e0672d6e403e'
  STORAGE_ALLOWED_LOCATIONS = ('azure:--storageaccountsnow.blob.core.windows.net/snowflakecsv', 'azure:--storageaccountsnow.blob.core.windows.net/snowflakejson');

-- Describe integration object to provide access
DESC STORAGE integration azure_integration;
```

## 7.2 Create Stage

```sql
---- Create file format & stage objects ----
-- create file format
create or replace file format demo_db.public.fileformat_azure
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
create or replace stage demo_db.public.stage_azure
    STORAGE_INTEGRATION = azure_integration
    URL = 'azure:--storageaccountsnow.blob.core.windows.net/snowflakecsv'
    FILE_FORMAT = fileformat_azure;

-- list files
LIST @demo_db.public.stage_azure;
```

## 7.3 Load CSV

```sql
---- Query files & Load data ----
--query files
SELECT 
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 
      $12, $13, $14, $15, $16, $17, $18, $19, $20
FROM @demo_db.public.stage_azure;

CREATE OR REPLACE table happiness (
      country_name varchar,
      regional_indicator varchar,
      ladder_score number(4,3),
      standard_error number(4,3),
      upperwhisker number(4,3),
      lowerwhisker number(4,3),
      logged_gdp number(5,3),
      social_support number(4,3),
      healthy_life_expectancy number(5,3),
      freedom_to_make_life_choices number(4,3),
      generosity number(4,3),
      perceptions_of_corruption number(4,3),
      ladder_score_in_dystopia number(4,3),
      explained_by_log_gpd_per_capita number(4,3),
      explained_by_social_support number(4,3),
      explained_by_healthy_life_expectancy number(4,3),
      explained_by_freedom_to_make_life_choices number(4,3),
      explained_by_generosity number(4,3),
      explained_by_perceptions_of_corruption number(4,3),
      dystopia_residual number (4,3)
      );
    
COPY INTO HAPPINESS
FROM @demo_db.public.stage_azure;

SELECT * FROM HAPPINESS;
```

## 7.4 Load JSON

```sql
--- Load JSON ---- 
CREATE OR REPLACE file format demo_db.public.fileformat_azure_json
      TYPE = JSON;

CREATE OR REPLACE stage demo_db.public.stage_azure
      STORAGE_INTEGRATION = azure_integration
      URL = 'azure:--storageaccountsnow.blob.core.windows.net/snowflakejson'
      FILE_FORMAT = fileformat_azure_json; 

LIST  @demo_db.public.stage_azure;

-- Query from stage  
SELECT * FROM @demo_db.public.stage_azure;  

-- Query one attribute/column
SELECT $1:"Car Model" FROM @demo_db.public.stage_azure; 

-- Convert data type  
SELECT $1:"Car Model"::STRING FROM @demo_db.public.stage_azure; 

-- Query all attributes  
SELECT 
    $1:"Car Model"::STRING, 
    $1:"Car Model Year"::INT,
    $1:"car make"::STRING, 
    $1:"first_name"::STRING,
    $1:"last_name"::STRING
FROM @demo_db.public.stage_azure;   
  
-- Query all attributes and use aliases 
SELECT 
    $1:"Car Model"::STRING as car_model, 
    $1:"Car Model Year"::INT as car_model_year,
    $1:"car make"::STRING AS car_make, 
    $1:"first_name"::STRING AS first_name,
    $1:"last_name"::STRING AS last_name
FROM @demo_db.public.stage_azure;     

CREATE OR REPLACE table car_owner (
    car_model varchar, 
    car_model_year int,
    car_make varchar, 
    first_name varchar,
    last_name varchar
    );
 
COPY INTO car_owner
    FROM (
      SELECT 
        $1:"Car Model"::STRING AS car_model, 
        $1:"Car Model Year"::INT AS car_model_year,
        $1:"car make"::STRING AS car_make, 
        $1:"first_name"::STRING AS first_name,
        $1:"last_name"::STRING AS last_name
FROM @demo_db.public.stage_azure);

SELECT * FROM CAR_OWNER;

-- Alternative: Using a raw file table step
TRUNCATE table car_owner;
SELECT * FROM car_owner;

CREATE OR REPLACE table car_owner_raw (
    raw variant);

COPY INTO car_owner_raw
FROM @demo_db.public.stage_azure;

SELECT * FROM car_owner_raw;
  
INSERT INTO car_owner (
    SELECT 
        $1:"Car Model"::STRING AS car_model, 
        $1:"Car Model Year"::INT AS car_model_year,
        $1:"car make"::STRING AS car_make, 
        $1:"first_name"::STRING AS first_name,
        $1:"last_name"::STRING AS last_name
    FROM car_owner_raw
    )
  
SELECT * FROM car_owner;
```

# 8. Loading GCP

## 8.1 Create Integration Object

```sql
-- create integration object that contains the access information
CREATE STORAGE INTEGRATION gcp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs:--bucket/path', 'gcs:--bucket/path2');

-- Describe integration object to provide access
DESC STORAGE integration gcp_integration;
```

## 8.2 Query & Load Data

```sql
---- Query files & Load data ----
--query files
SELECT 
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16, $17, $18, $19, $20
FROM @demo_db.public.stage_gcp;

CREATE OR REPLACE table happiness (
    country_name VARCHAR,
    regional_indicator VARCHAR,
    ladder_score NUMBER(4,3),
    standard_error NUMBER(4,3),
    upperwhisker NUMBER(4,3),
    lowerwhisker NUMBER(4,3),
    logged_gdp NUMBER(5,3),
    social_support NUMBER(4,3),
    healthy_life_expectancy NUMBER(5,3),
    freedom_to_make_life_choices NUMBER(4,3),
    generosity NUMBER(4,3),
    perceptions_of_corruption NUMBER(4,3),
    ladder_score_in_dystopia NUMBER(4,3),
    explained_by_log_gpd_per_capita NUMBER(4,3),
    explained_by_social_support NUMBER(4,3),
    explained_by_healthy_life_expectancy NUMBER(4,3),
    explained_by_freedom_to_make_life_choices NUMBER(4,3),
    explained_by_generosity NUMBER(4,3),
    explained_by_perceptions_of_corruption NUMBER(4,3),
    dystopia_residual NUMBER(4,3)
    );
    
COPY INTO HAPPINESS
FROM @demo_db.public.stage_gcp;

SELECT * FROM HAPPINESS;
```

## 8.2 Unload Data

```sql
------- Unload data -----
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_DB;

-- create integration object that contains the access information
CREATE STORAGE INTEGRATION gcp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs:--snowflakebucketgcp', 'gcs:--snowflakebucketgcpjson');
  
-- create file format
CREATE OR REPLACE file format demo_db.public.fileformat_gcp
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
CREATE OR REPLACE stage demo_db.public.stage_gcp
    STORAGE_INTEGRATION = gcp_integration
    URL = 'gcs:--snowflakebucketgcp/csv_happiness'
    FILE_FORMAT = fileformat_gcp
    ;

ALTER STORAGE INTEGRATION gcp_integration
SET  storage_allowed_locations=('gcs:--snowflakebucketgcp', 'gcs:--snowflakebucketgcpjson');

SELECT * FROM HAPPINESS;

COPY INTO @stage_gcp
FROM
HAPPINESS;
```

# 9. Snowpipe

-- 9.1 Create Stage and Pipe

```sql
-- Create table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.employees (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    location STRING,
    department STRING
    );
    
-- Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;
    
-- Create stage object with integration object & file format object
CREATE OR REPLACE stage MANAGE_DB.external_stages.csv_folder
    URL = 's3:--snowflakes3bucket123/csv/snowpipe'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat
   
-- Create stage object with integration object & file format object
LIST @MANAGE_DB.external_stages.csv_folder;

-- Create schema to keep things organized
CREATE OR REPLACE SCHEMA MANAGE_DB.pipes;

-- Define pipe
CREATE OR REPLACE pipe MANAGE_DB.pipes.employee_pipe
    auto_ingest = TRUE
    AS
    COPY INTO OUR_FIRST_DB.PUBLIC.employees
FROM @MANAGE_DB.external_stages.csv_folder; 

-- Describe Pipe
DESC pipe employee_pipe;
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.employees;
```

-- 9.2 Create Pipe

```sql
-- Define pipe
CREATE OR REPLACE pipe MANAGE_DB.pipes.employee_pipe
    auto_ingest = TRUE
    AS
    COPY INTO OUR_FIRST_DB.PUBLIC.employees
FROM @MANAGE_DB.external_stages.csv_folder; 

-- Describe pipe
DESC pipe employee_pipe;
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.employees;


-- 9.3 Error Handling
  
-- Handling errors
-- Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.employees;  

ALTER PIPE employee_pipe refresh;
 
-- Validate pipe is actually working
SELECT SYSTEM$PIPE_STATUS('employee_pipe');

-- Snowpipe error message
SELECT * FROM TABLE(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'MANAGE_DB.pipes.employee_pipe',
    START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())));

-- COPY command history from table to see error massage
SELECT * FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
   table_name  =>  'OUR_FIRST_DB.PUBLIC.EMPLOYEES',
   START_TIME =>DATEADD(HOUR,-2,CURRENT_TIMESTAMP())));
```

-- 9.3 Manage Pipe

```sql
-- Manage pipes -- 
DESC pipe MANAGE_DB.pipes.employee_pipe;

SHOW PIPES;

SHOW PIPES LIKE '%employee%';

SHOW PIPES IN database MANAGE_DB;

SHOW PIPES IN SCHEMA MANAGE_DB.pipes;

SHOW PIPES LIKE '%employee%' IN Database MANAGE_DB;

-- Changing pipe (alter stage or file format) --
-- Preparation table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.employees2 (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    location STRING,
    department STRING
    );

-- Pause pipe
ALTER PIPE MANAGE_DB.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = true;
 
-- Verify pipe is paused and has pendingFileCount 0 
SELECT SYSTEM$PIPE_STATUS('MANAGE_DB.pipes.employee_pipe');
 
 -- Recreate the pipe to change the COPY statement in the definition
CREATE OR REPLACE pipe MANAGE_DB.pipes.employee_pipe
    auto_ingest = TRUE
    AS
    COPY INTO OUR_FIRST_DB.PUBLIC.employees2
FROM @MANAGE_DB.external_stages.csv_folder  

ALTER PIPE  MANAGE_DB.pipes.employee_pipe refresh;

-- List files in stage
LIST @MANAGE_DB.external_stages.csv_folder; 

SELECT * FROM OUR_FIRST_DB.PUBLIC.employees2;

 -- Reload files manually that where aleady in the bucket
COPY INTO OUR_FIRST_DB.PUBLIC.employees2
FROM @MANAGE_DB.external_stages.csv_folder  

-- Resume pipe
ALTER PIPE MANAGE_DB.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = false;

-- Verify pipe is running again
SELECT SYSTEM$PIPE_STATUS('MANAGE_DB.pipes.employee_pipe');
```

# 10. Time Travel

## 10.1 Using Time Travel

```sql
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
```

## 10.2 Restoring in Time Travel

```sql
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
```

## 10.3 Undrop Table

```sql
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
```

## 10.4 Time Travel Cost

```sql
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
```

# 11. Fail Sale

## 11.1 Fail Safe

```sql
-- Storage usage on account level

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;

-- Storage usage on account level formatted

SELECT 	USAGE_DATE, 
	STORAGE_BYTES / (1024*1024*1024) AS STORAGE_GB,  
	STAGE_BYTES / (1024*1024*1024) AS STAGE_GB,
	FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;

-- Storage usage on table level

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

-- Storage usage on table level formatted

SELECT 	ID, 
	TABLE_NAME, 
	TABLE_SCHEMA,
	ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED_GB,
	TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB,
	FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_STORAGE_USED_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY FAILSAFE_STORAGE_USED_GB DESC;
```

# 12. Type of Table

## 12.1 Permanent Tables & Database

```sql
CREATE OR REPLACE DATABASE PDB;

CREATE OR REPLACE TABLE PDB.public.customers (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );
  
CREATE OR REPLACE TABLE PDB.public.helper (
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
COPY INTO PDB.public.helper
FROM @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');

SELECT * FROM PDB.public.helper;

INSERT INTO PDB.public.customers
SELECT
    t1.ID,
    t1.FIRST_NAME,
    t1.LAST_NAME,
    t1.EMAIL,
    t1.GENDER,
    t1.JOB,t1.PHONE
FROM PDB.public.helper t1
CROSS JOIN (SELECT * FROM PDB.public.helper) t2
CROSS JOIN (SELECT TOP 100 * FROM PDB.public.helper) t3;

-- Show table and validate
SHOW TABLES;
```

## 12.2 Transient Tables

```sql
CREATE OR REPLACE DATABASE TDB;

CREATE OR REPLACE TRANSIENT TABLE TDB.public.customers_transient (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );

INSERT INTO TDB.public.customers_transient
SELECT t1.* FROM OUR_FIRST_DB.public.customers t1
CROSS JOIN (SELECT * FROM OUR_FIRST_DB.public.customers) t2

SHOW TABLES;

-- Query storage
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS

SELECT 	
    	ID, 
    	TABLE_NAME, 
	TABLE_SCHEMA,
	TABLE_CATALOG,
	ACTIVE_BYTES,
	TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB,
	FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_STORAGE_USED_GB,
        IS_TRANSIENT,
        DELETED,
        TABLE_CREATED,
        TABLE_DROPPED,
        TABLE_ENTERED_FAILSAFE
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG ='TDB'
ORDER BY TABLE_CREATED DESC;

-- Set retention time to 0
ALTER TABLE TDB.public.customers_transient
SET DATA_RETENTION_TIME_IN_DAYS  = 0;

DROP TABLE TDB.public.customers_transient;

UNDROP TABLE TDB.public.customers_transient;

SHOW TABLES;

-- Creating transient schema and then table 
CREATE OR REPLACE TRANSIENT SCHEMA TRANSIENT_SCHEMA;

SHOW SCHEMAS;

CREATE OR REPLACE TABLE TDB.TRANSIENT_SCHEMA.new_table (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );
  
ALTER TABLE TDB.TRANSIENT_SCHEMA.new_table
SET DATA_RETENTION_TIME_IN_DAYS  = 2

SHOW TABLES;
```

## 12.3 Temporary Tables

```sql
USE DATABASE PDB;

-- Create permanent table 
CREATE OR REPLACE TABLE PDB.public.customers (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );

INSERT INTO PDB.public.customers
SELECT t1.* FROM OUR_FIRST_DB.public.customers t1;

SELECT * FROM PDB.public.customers;

-- Create temporary table (with the same name)
CREATE OR REPLACE TEMPORARY TABLE PDB.public.customers (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );

-- Validate temporary table is the active table
SELECT * FROM PDB.public.customers;

-- Create second temporary table (with a new name)
CREATE OR REPLACE TEMPORARY TABLE PDB.public.temp_table (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );

-- Insert data in the new table
INSERT INTO PDB.public.temp_table
SELECT * FROM PDB.public.customers;

SELECT * FROM PDB.public.temp_table;

SHOW TABLES;
```

# 13. Zero Copy Cloning

## 13.1 Cloning Schema & Databases

```sql  
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
```

## 13.2 Cloning with Time Travel

```sql
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
```

# 14. Data Sharing

## 14.1 Using Data Sharing

  ```sql
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
```

## 14.2 Create Reader Account

```sql
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
```

## 14.3 Secure View

```sql
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
```

## 14.4 Sharing Views

```sql
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
```

# 15. Data Sampling

## 15.1 Data Sampling

```sql
-- Data Sampling

CREATE OR REPLACE TRANSIENT DATABASE SAMPLING_DB;

CREATE OR REPLACE VIEW ADDRESS_SAMPLE AS 
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_ADDRESS 
    SAMPLE ROW (1) SEED(27);

SELECT * FROM ADDRESS_SAMPLE

SELECT CA_LOCATION_TYPE, COUNT(*)/3254250*100
FROM ADDRESS_SAMPLE
GROUP BY CA_LOCATION_TYPE

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_ADDRESS 
SAMPLE SYSTEM (1) SEED(23);

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_ADDRESS 
SAMPLE SYSTEM (10) SEED(23);
```

# 16. Scheduling Tasks

## 16.1 Creating Tasks

```sql
CREATE OR REPLACE TRANSIENT DATABASE TASK_DB;

-- Prepare table
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID INT AUTOINCREMENT START = 1 INCREMENT =1,
    FIRST_NAME VARCHAR(40) DEFAULT 'JENNIFER' ,
    CREATE_DATE DATE)
    
-- Create task
CREATE OR REPLACE TASK CUSTOMER_INSERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);
    
SHOW TASKS;

-- Task starting and suspending
ALTER TASK CUSTOMER_INSERT RESUME;
ALTER TASK CUSTOMER_INSERT SUSPEND;

SELECT * FROM CUSTOMERS
```

## 16.2 Using CRON

```sql
CREATE OR REPLACE TASK CUSTOMER_INSERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '60 MINUTE'
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);

CREATE OR REPLACE TASK CUSTOMER_INSERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 7,10 * * 5L UTC'
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);
    
# __________ minute (0-59)
# | ________ hour (0-23)
# | | ______ day of month (1-31, or L)
# | | | ____ month (1-12, JAN-DEC)
# | | | | __ day of week (0-6, SUN-SAT, or L)
# | | | | |
# | | | | |
# * * * * *

-- Every minute
SCHEDULE = 'USING CRON * * * * * UTC'
  
-- Every day at 6am UTC timezone
SCHEDULE = 'USING CRON 0 6 * * * UTC'

-- Every hour starting at 9 AM and ending at 5 PM on Sundays 
SCHEDULE = 'USING CRON 0 9-17 * * SUN America/Los_Angeles'

CREATE OR REPLACE TASK CUSTOMER_INSERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 9,17 * * * UTC'
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);
```

## 16.3 Creating Tree of Tasks

```sql
USE TASK_DB;
 
SHOW TASKS;

SELECT * FROM CUSTOMERS;

-- Prepare a second table
CREATE OR REPLACE TABLE CUSTOMERS2 (
    CUSTOMER_ID INT,
    FIRST_NAME VARCHAR(40),
    CREATE_DATE DATE)
     
-- Suspend parent task
ALTER TASK CUSTOMER_INSERT SUSPEND;
    
-- Create a child task
CREATE OR REPLACE TASK CUSTOMER_INSERT2
    WAREHOUSE = COMPUTE_WH
    AFTER CUSTOMER_INSERT
    AS 
    INSERT INTO CUSTOMERS2 SELECT * FROM CUSTOMERS;
      
-- Prepare a third table
CREATE OR REPLACE TABLE CUSTOMERS3 (
    CUSTOMER_ID INT,
    FIRST_NAME VARCHAR(40),
    CREATE_DATE DATE,
    INSERT_DATE DATE DEFAULT DATE(CURRENT_TIMESTAMP))    
    
-- Create a child task
CREATE OR REPLACE TASK CUSTOMER_INSERT3
    WAREHOUSE = COMPUTE_WH
    AFTER CUSTOMER_INSERT2
    AS 
    INSERT INTO CUSTOMERS3 (CUSTOMER_ID,FIRST_NAME,CREATE_DATE) SELECT * FROM CUSTOMERS2;

SHOW TASKS;

ALTER TASK CUSTOMER_INSERT 
SET SCHEDULE = '1 MINUTE'

-- Resume tasks (first root task)
ALTER TASK CUSTOMER_INSERT RESUME;
ALTER TASK CUSTOMER_INSERT2 RESUME;
ALTER TASK CUSTOMER_INSERT3 RESUME;

SELECT * FROM CUSTOMERS2

SELECT * FROM CUSTOMERS3

-- Suspend tasks again
ALTER TASK CUSTOMER_INSERT SUSPEND;
ALTER TASK CUSTOMER_INSERT2 SUSPEND;
ALTER TASK CUSTOMER_INSERT3 SUSPEND;
```

## 16.4 Calling a Stotrd Procedure

```sql
-- Create a stored procedure
USE TASK_DB;

SELECT * FROM CUSTOMERS

CREATE OR REPLACE PROCEDURE CUSTOMERS_INSERT_PROCEDURE (CREATE_DATE varchar)
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        var sql_command = 'INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(:1);'
        snowflake.EXECUTE(
            {
            sqlText: sql_command,
            binds: [CREATE_DATE]
            });
        RETURN "Successfully executed.";
        $$;
        
CREATE OR REPLACE TASK CUSTOMER_TAKS_PROCEDURE
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS CALL  CUSTOMERS_INSERT_PROCEDURE (CURRENT_TIMESTAMP);

SHOW TASKS;

ALTER TASK CUSTOMER_TAKS_PROCEDURE RESUME;

SELECT * FROM CUSTOMERS;
```

## 16.5 Task History & Error Handling

```sql
-- Create a stored procedure
USE TASK_DB;

SELECT * FROM CUSTOMERS

CREATE OR REPLACE PROCEDURE CUSTOMERS_INSERT_PROCEDURE (CREATE_DATE varchar)
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        var sql_command = 'INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(:1);'
        snowflake.EXECUTE(
            {
            sqlText: sql_command,
            binds: [CREATE_DATE]
            });
        RETURN "Successfully executed.";
        $$;

CREATE OR REPLACE TASK CUSTOMER_TAKS_PROCEDURE
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS CALL  CUSTOMERS_INSERT_PROCEDURE (CURRENT_TIMESTAMP);

SHOW TASKS;

ALTER TASK CUSTOMER_TAKS_PROCEDURE RESUME;

SELECT * FROM CUSTOMERS;
```

# 17. Streams

## 17.1 Insert Operation

```sql
-------------------- Stream example: INSERT ------------------------
CREATE OR REPLACE TRANSIENT DATABASE STREAMS_DB;

-- Create example table 
CREATE OR REPLACE table sales_raw_staging(
    id varchar,
    product varchar,
    price varchar,
    amount varchar,
    store_id varchar
    );
  
-- insert values 
INSERT INTO sales_raw_staging 
    VALUES
        (1,'Banana',1.99,1,1),
        (2,'Lemon',0.99,1,1),
        (3,'Apple',1.79,1,2),
        (4,'Orange Juice',1.89,1,2),
        (5,'Cereals',5.98,2,1);  

CREATE OR REPLACE table store_table(
	store_id number,
	location varchar,
	employees number);

INSERT INTO STORE_TABLE VALUES(1,'Chicago',33);
INSERT INTO STORE_TABLE VALUES(2,'London',12);

CREATE OR REPLACE table sales_final_table(
    id INT,
    product VARCHAR,
    price NUMBER,
    amount INT,
    store_id INT,
    location VARCHAR,
    employees INT
    );

 -- Insert into final table
INSERT INTO sales_final_table 
    SELECT 
    SA.id,
    SA.product,
    SA.price,
    SA.amount,
    ST.STORE_ID,
    ST.LOCATION, 
    ST.EMPLOYEES 
    FROM SALES_RAW_STAGING SA
    JOIN STORE_TABLE ST ON ST.STORE_ID=SA.STORE_ID ;

-- Create a stream object
CREATE OR REPLACE stream sales_stream on table sales_raw_staging;

SHOW STREAMS;

DESC STREAM sales_stream;

-- Get changes on data using stream (INSERTS)
SELECT * FROM sales_stream;

SELECT * FROM sales_raw_staging;
        
-- insert values 
INSERT INTO sales_raw_staging  
    VALUES
        (6,'Mango',1.99,1,2),
        (7,'Garlic',0.99,1,1);
        
-- Get changes on data using stream (INSERTS)
SELECT * FROM sales_stream;

SELECT * FROM sales_raw_staging;
                
SELECT * FROM sales_final_table;        
        
-- Consume stream object
INSERT INTO sales_final_table 
    SELECT 
    SA.id,
    SA.product,
    SA.price,
    SA.amount,
    ST.STORE_ID,
    ST.LOCATION, 
    ST.EMPLOYEES 
    FROM SALES_STREAM SA
    JOIN STORE_TABLE ST ON ST.STORE_ID = SA.STORE_ID ;

-- Get changes on data using stream (INSERTS)
SELECT * FROM sales_stream;

-- insert values 
INSERT INTO sales_raw_staging  
    VALUES
        (8,'Paprika',4.99,1,2),
        (9,'Tomato',3.99,1,2);
              
 -- Consume stream object
INSERT INTO sales_final_table 
    SELECT 
    SA.id,
    SA.product,
    SA.price,
    SA.amount,
    ST.STORE_ID,
    ST.LOCATION, 
    ST.EMPLOYEES 
    FROM SALES_STREAM SA
    JOIN STORE_TABLE ST ON ST.STORE_ID = SA.STORE_ID ;
                 
SELECT * FROM SALES_FINAL_TABLE;        

SELECT * FROM SALES_RAW_STAGING;     
        
SELECT * FROM SALES_STREAM;
```

## 17.2 Update Operation

```sql
-- ******* UPDATE 1 ********

SELECT * FROM SALES_RAW_STAGING;     
        
SELECT * FROM SALES_STREAM;

UPDATE SALES_RAW_STAGING
SET PRODUCT ='Potato' WHERE PRODUCT = 'Banana'

MERGE INTO SALES_FINAL_TABLE F      -- Target table to merge changes from source table
USING SALES_STREAM S                -- Stream that has captured the changes
   ON  F.id = S.id                 
WHEN matched AND
    S.METADATA$ACTION ='INSERT' AND
    S.METADATA$ISUPDATE ='TRUE'        -- Indicates the record has been updated 
    THEN UPDATE 
    SET F.product = S.product,
        F.price = S.price,
        F.amount = S.amount,
        F.store_id = S.store_id;
        
SELECT * FROM SALES_FINAL_TABLE

SELECT * FROM SALES_RAW_STAGING;     
        
SELECT * FROM SALES_STREAM;

-- ******* UPDATE 2 ********

UPDATE SALES_RAW_STAGING
SET PRODUCT ='Green apple' WHERE PRODUCT = 'Apple';

MERGE INTO SALES_FINAL_TABLE F      -- Target table to merge changes from source table
USING SALES_STREAM S                -- Stream that has captured the changes
   ON  F.id = S.id                 
WHEN matched AND
    S.METADATA$ACTION ='INSERT' AND
    S.METADATA$ISUPDATE ='TRUE'        -- Indicates the record has been updated 
    THEN UPDATE 
    SET F.product = S.product,
        F.price = S.price,
        F.amount = S.amount,
        F.store_id = S.store_id;

SELECT * FROM SALES_FINAL_TABLE;

SELECT * FROM SALES_RAW_STAGING;     
        
SELECT * FROM SALES_STREAM;


## 17.3 Delete Operation
  
-- ******* DELETE  ********                
SELECT * FROM SALES_FINAL_TABLE

SELECT * FROM SALES_RAW_STAGING;     
        
SELECT * FROM SALES_STREAM;    

DELETE FROM SALES_RAW_STAGING
WHERE PRODUCT = 'Lemon';
        
-- ******* Process stream  ********            
MERGE INTO SALES_FINAL_TABLE F      -- Target table to merge changes from source table
USING SALES_STREAM S                -- Stream that has captured the changes
   ON  F.id = S.id          
WHEN matched AND
    S.METADATA$ACTION ='DELETE' AND
    S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE
```

## 17.4 Process All Data Changes  

```sql
-- ******* Process UPDATE,INSERT & DELETE simultaneously  ********                   
MERGE INTO SALES_FINAL_TABLE F      -- Target table to merge changes from source table
USING ( SELECT STRE.*,ST.location,ST.employees
        FROM SALES_STREAM STRE
        JOIN STORE_TABLE ST
        ON STRE.store_id = ST.store_id
       ) S
ON F.id = S.id
WHEN matched AND                        -- DELETE condition
    S.METADATA$ACTION ='DELETE' AND
    S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE                   
WHEN matched AND                        -- UPDATE condition
    S.METADATA$ACTION ='INSERT' AND
    S.METADATA$ISUPDATE  = 'TRUE'       
    THEN UPDATE
    SET F.product = S.product,
        F.price = S.price,
        F.amount= S.amount,
        F.store_id = S.store_id
WHEN matched AND
    S.METADATA$ACTION ='INSERT'
    THEN INSERT 
    (id,product,price,store_id,amount,employees,location)
    VALUES
    (S.id, S.product, S.price, S.store_id, S.amount, S.employees, S.location)

SELECT * FROM SALES_RAW_STAGING;     
        
SELECT * FROM SALES_STREAM;

SELECT * FROM SALES_FINAL_TABLE;

INSERT INTO SALES_RAW_STAGING VALUES (2,'Lemon',0.99,1,1);

UPDATE SALES_RAW_STAGING
SET PRODUCT = 'Lemonade'
WHERE PRODUCT ='Lemon'
   
DELETE FROM SALES_RAW_STAGING
WHERE PRODUCT = 'Lemonade';       

--- Example 2 ---
INSERT INTO SALES_RAW_STAGING VALUES (10,'Lemon Juice',2.99,1,1);

UPDATE SALES_RAW_STAGING
SET PRICE = 3
WHERE PRODUCT ='Mango';
       
DELETE FROM SALES_RAW_STAGING
WHERE PRODUCT = 'Potato';    


## 17.5 Combine Streams & Tasks

------- Automatate the updates using tasks --
CREATE OR REPLACE TASK all_data_changes
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('SALES_STREAM')
    AS 
merge into SALES_FINAL_TABLE F      -- Target table to merge changes from source table
USING ( SELECT STRE.*,ST.location,ST.employees
        FROM SALES_STREAM STRE
        JOIN STORE_TABLE ST
        ON STRE.store_id = ST.store_id
       ) S
ON F.id=S.id
WHEN matched AND                        -- DELETE condition
    S.METADATA$ACTION ='DELETE' AND
    S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE                   
when matched AND                        -- UPDATE condition
    S.METADATA$ACTION ='INSERT' AND 
    S.METADATA$ISUPDATE  = 'TRUE'       
    THEN UPDATE 
    set F.product = S.product,
        F.price = S.price,
        F.amount = S.amount,
        F.store_id = S.store_id
WHEN NOT matched AND 
    S.METADATA$ACTION ='INSERT'
    THEN INSERT 
    (id,product,price,store_id,amount,employees,location)
    VALUES
    (S.id, S.product, S.price, S.store_id, S.amount, S.employees, S.location)

ALTER TASK all_data_changes RESUME;
SHOW TASKS;

-- Change data

INSERT INTO SALES_RAW_STAGING VALUES (11,'Milk',1.99,1,2);
INSERT INTO SALES_RAW_STAGING VALUES (12,'Chocolate',4.49,1,2);
INSERT INTO SALES_RAW_STAGING VALUES (13,'Cheese',3.89,1,1);

UPDATE SALES_RAW_STAGING
SET PRODUCT = 'Chocolate bar'
WHERE PRODUCT ='Chocolate';
       
DELETE FROM SALES_RAW_STAGING
WHERE PRODUCT = 'Mango';    

-- Verify results
SELECT * FROM SALES_RAW_STAGING;     
        
SELECT * FROM SALES_STREAM;

SELECT * FROM SALES_FINAL_TABLE;

-- Verify the history
SELECT *
FROM table(information_schema.task_history())
ORDER BY name ASC,scheduled_time DESC;
```

## 17.6 Type of Streams

```sql
------- Append-only type ------
USE STREAMS_DB;
SHOW STREAMS;

SELECT * FROM SALES_RAW_STAGING;     

-- Create stream with default
CREATE OR REPLACE STREAM SALES_STREAM_DEFAULT
ON TABLE SALES_RAW_STAGING;

-- Create stream with append-only
CREATE OR REPLACE STREAM SALES_STREAM_APPEND
ON TABLE SALES_RAW_STAGING 
APPEND_ONLY = TRUE;

-- View streams
SHOW STREAMS;

-- Insert values
INSERT INTO SALES_RAW_STAGING VALUES (14,'Honey',4.99,1,1);
INSERT INTO SALES_RAW_STAGING VALUES (15,'Coffee',4.89,1,2);
INSERT INTO SALES_RAW_STAGING VALUES (15,'Coffee',4.89,1,2);

SELECT * FROM SALES_STREAM_APPEND;
SELECT * FROM SALES_STREAM_DEFAULT;

-- Delete values
SELECT * FROM SALES_RAW_STAGING

DELETE FROM SALES_RAW_STAGING WHERE ID=7;

SELECT * FROM SALES_STREAM_APPEND;
SELECT * FROM SALES_STREAM_DEFAULT;

-- Consume stream via "CREATE TABLE ... AS"
CREATE OR REPLACE TEMPORARY TABLE PRODUCT_TABLE
AS SELECT * FROM SALES_STREAM_DEFAULT;
CREATE OR REPLACE TEMPORARY TABLE PRODUCT_TABLE
AS SELECT * FROM SALES_STREAM_APPEND;

-- Update
UPDATE SALES_RAW_STAGING
SET PRODUCT = 'Coffee 200g'
WHERE PRODUCT ='Coffee';
       
SELECT * FROM SALES_STREAM_APPEND;
SELECT * FROM SALES_STREAM;
```

## 17.7 Change Clause

```sql
----- Change clause ------ 
--- Create example db & table ---
CREATE OR REPLACE DATABASE SALES_DB;

CREATE OR REPLACE table sales_raw(
	  id VARCHAR,
	  product VARCHAR,
	  price VARCHAR,
  	amount VARCHAR,
	  store_id VARCHAR
    );

-- insert values
INSERT INTO sales_raw
	VALUES
		(1, 'Eggs', 1.39, 1, 1),
		(2, 'Baking powder', 0.99, 1, 1),
		(3, 'Eggplants', 1.79, 1, 2),
		(4, 'Ice cream', 1.89, 1, 2),
		(5, 'Oats', 1.98, 2, 1);

ALTER TABLE sales_raw
SET CHANGE_TRACKING = TRUE;

SELECT * FROM SALES_RAW
CHANGES(information => default)
AT (offset => -0.5*60)

SELECT CURRENT_TIMESTAMP;

-- Insert values
INSERT INTO SALES_RAW VALUES (6, 'Bread', 2.99, 1, 2);
INSERT INTO SALES_RAW VALUES (7, 'Onions', 2.89, 1, 2);

SELECT * FROM SALES_RAW
CHANGES(information  => default)
AT (timestamp => 'your-timestamp'::timestamp_tz)

UPDATE SALES_RAW
SET PRODUCT = 'Toast2' WHERE ID=6;

-- information value

SELECT * FROM SALES_RAW
CHANGES(information  => default)
AT (timestamp => 'your-timestamp'::timestamp_tz)

SELECT * FROM SALES_RAW
CHANGES(information  => append_only)
AT (timestamp => 'your-timestamp'::timestamp_tz)

CREATE OR REPLACE TABLE PRODUCTS 
AS
SELECT * FROM SALES_RAW
CHANGES(information  => append_only)
AT (timestamp => 'your-timestamp'::timestamp_tz)

SELECT * FROM PRODUCTS;
```

# 18. Materializaed Views

## 18.1 Create Materialized View

```sql
-- Remove caching just to have a fair test -- Part 1
ALTER SESSION SET USE_CACHED_RESULT=FALSE; -- disable global caching
ALTER warehouse compute_wh suspend;
ALTER warehouse compute_wh resume;

-- Prepare table
CREATE OR REPLACE TRANSIENT DATABASE ORDERS;

CREATE OR REPLACE SCHEMA TPCH_SF100;

CREATE OR REPLACE TABLE TPCH_SF100.ORDERS AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;

SELECT * FROM ORDERS LIMIT 100

-- Example statement view -- 
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT) AS MAX_COMMENT,
    MIN(O_COMMENT) AS MIN_COMMENT,
    MAX(O_CLERK) AS MAX_CLERK,
    MIN(O_CLERK) AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);

-- Create materialized view
CREATE OR REPLACE MATERIALIZED VIEW ORDERS_MV
AS 
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT) AS MAX_COMMENT,
    MIN(O_COMMENT) AS MIN_COMMENT,
    MAX(O_CLERK) AS MAX_CLERK,
    MIN(O_CLERK) AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE);

SHOW MATERIALIZED VIEWS;

-- Query view
SELECT * FROM ORDERS_MV
ORDER BY YEAR;

-- UPDATE or DELETE values
UPDATE ORDERS
SET O_CLERK='Clerk#99900000' 
WHERE O_ORDERDATE='1992-01-01'

  -- Test updated data --
-- Example statement view -- 
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT) AS MAX_COMMENT,
    MIN(O_COMMENT) AS MIN_COMMENT,
    MAX(O_CLERK) AS MAX_CLERK,
    MIN(O_CLERK) AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);

-- Query view
SELECT * FROM ORDERS_MV
ORDER BY YEAR;

SHOW MATERIALIZED VIEWS;
```

## 18.2 Refresh in Materialized Views

```sql  
-- Remove caching just to have a fair test -- Part 2
ALTER SESSION SET USE_CACHED_RESULT=FALSE; -- disable global caching
ALTER warehouse compute_wh suspend;
ALTER warehouse compute_wh resume;

-- Prepare table
CREATE OR REPLACE TRANSIENT DATABASE ORDERS;

CREATE OR REPLACE SCHEMA TPCH_SF100;

CREATE OR REPLACE TABLE TPCH_SF100.ORDERS AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;

SELECT * FROM ORDERS LIMIT 100

-- Example statement view -- 
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT) AS MAX_COMMENT,
    MIN(O_COMMENT) AS MIN_COMMENT,
    MAX(O_CLERK) AS MAX_CLERK,
    MIN(O_CLERK) AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);

-- Create materialized view
CREATE OR REPLACE MATERIALIZED VIEW ORDERS_MV
  AS 
    SELECT
        YEAR(O_ORDERDATE) AS YEAR,
        MAX(O_COMMENT) AS MAX_COMMENT,
        MIN(O_COMMENT) AS MIN_COMMENT,
        MAX(O_CLERK) AS MAX_CLERK,
        MIN(O_CLERK) AS MIN_CLERK
    FROM ORDERS.TPCH_SF100.ORDERS
    GROUP BY YEAR(O_ORDERDATE);

SHOW MATERIALIZED VIEWS;

-- Query view
SELECT * FROM ORDERS_MV
ORDER BY YEAR;

-- UPDATE or DELETE values
UPDATE ORDERS
SET O_CLERK='Clerk#99900000' 
WHERE O_ORDERDATE='1992-01-01';

   -- Test updated data --
-- Example statement view -- 
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT) AS MAX_COMMENT,
    MIN(O_COMMENT) AS MIN_COMMENT,
    MAX(O_CLERK) AS MAX_CLERK,
    MIN(O_CLERK) AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);

-- Query view
SELECT * FROM ORDERS_MV
ORDER BY YEAR;

SHOW MATERIALIZED VIEWS;

SELECT * FROM table(information_schema.materialized_view_refresh_history());
```

## 18.3 Maintenance Cost

```sql
SHOW MATERIALIZED VIEWS;

SELECT * FROM table(information_schema.materialized_view_refresh_history());
```

#19. Data Masking

## 19.1 Creating Data Masking Policy

```sql
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
```

## 19.2 Unset & Replace Policy

```sql
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
```

## 19.3 Alter Existing Policy

```sql
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
```

## 19.4 Real-Life Examples

```sql
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
```

# 20. Access Management

## 20.1 Account Admin

```sql
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
```

## 20.2 Security Admin

  ```sql
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
```

## 20.3 System  Admin

```sql
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
```

## 20.4 Custom Role

```sql
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
```

## 20.5 User Admin

```sql
-- USERADMIN --
--- User 4 ---
CREATE USER ben PASSWORD = '123' 
DEFAULT_ROLE = ACCOUNTADMIN 
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE HR_ADMIN TO USER ben;

SHOW ROLES;

GRANT ROLE HR_ADMIN TO ROLE SYSADMIN;
```
