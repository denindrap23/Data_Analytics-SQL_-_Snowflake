-- 17.1 Insert Operation

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


-- 17.2 Update Operation
  
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


-- 17.3 Delete Operation
  
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


-- 17.4 Process All Data Changes  
  
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


-- 17.5 Combine Streams & Tasks

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


-- 17.6 Type of Streams
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


-- 17.7 Change Clause
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
