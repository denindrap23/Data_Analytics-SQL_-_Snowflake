-- 6.1 Create Storage Integration
  
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


-- 6.2 Load Data S3

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


-- 6.3 Handling JSON
  
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
