-- 8.1 Create Integration Object
  
-- create integration object that contains the access information
CREATE STORAGE INTEGRATION gcp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs:--bucket/path', 'gcs:--bucket/path2');

-- Describe integration object to provide access
DESC STORAGE integration gcp_integration;


-- 8.2 Query & Load Data

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


-- 8.2 Unload Data

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
