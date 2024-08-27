// S3 Integration
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::363750950044:role/snowflake-data-loading-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://fraud-detection-cleaned-data/fraud_detection_cleaned_data/');

// Database
CREATE OR REPLACE DATABASE fraud_detection_db;

// Staging
CREATE OR REPLACE STAGE fraud_stage
URL='s3://fraud-detection-cleaned-data/fraud_detection_cleaned_data/'
STORAGE_INTEGRATION = my_s3_integration;

// Table Creation
CREATE OR REPLACE TABLE fraud_detection_data (
    time_step STRING,               -- Represents the time step of the transaction
    transaction_id STRING,          -- Unique identifier for each transaction
    sender_id STRING,               -- Unique identifier for the sender
    sender_account STRING,          -- Account number of the sender
    sender_country STRING,          -- Country of the sender
    sender_sector BIGINT,           -- Sector code of the sender
    sender_lob STRING,              -- Line of business of the sender
    bene_id STRING,                 -- Unique identifier for the beneficiary
    bene_account STRING,            -- Account number of the beneficiary
    bene_country STRING,            -- Country of the beneficiary
    usd_amount DOUBLE,              -- Transaction amount in USD
    label BIGINT,                   -- Indicator of fraud (1 = Fraud, 0 = Non-Fraud)
    transaction_type STRING         -- Type of transaction (e.g., transfer, payment)
);

// CREATING DIMESION TABLES

// Stores details about countries involved in transactions.
CREATE OR REPLACE TABLE dim_country (
    country_code STRING PRIMARY KEY,  -- Country code as the primary key
    country_name STRING               -- Full name of the country
);

// Stores information about sectors, which could help in analyzing sector-based fraud patterns.
CREATE OR REPLACE TABLE dim_sector (
    sector_id BIGINT PRIMARY KEY,     -- Sector ID as the primary key
    sector_name STRING                -- Name of the sector
);

// Describes the different transaction types.
CREATE OR REPLACE TABLE dim_transaction_type (
    type_code STRING PRIMARY KEY,     -- Transaction type code
    description STRING                -- Description of the transaction type
);

//FACT TABLE
CREATE OR REPLACE TABLE fact_fraud_detection (
    transaction_id STRING PRIMARY KEY,  -- Unique identifier for each transaction
    time_step STRING,                   -- Represents the time step of the transaction
    sender_id STRING,                   -- Unique identifier for the sender
    bene_id STRING,                     -- Unique identifier for the beneficiary
    usd_amount DOUBLE,                  -- Transaction amount in USD
    label BIGINT,                       -- Indicator of fraud (1 = Fraud, 0 = Non-Fraud)
    country_code STRING REFERENCES dim_country(country_code),  -- Foreign key to `dim_country`
    sector_id BIGINT REFERENCES dim_sector(sector_id),         -- Foreign key to `dim_sector`
    type_code STRING REFERENCES dim_transaction_type(type_code) -- Foreign key to `dim_transaction_type`
);

// DATA LOADING
COPY INTO fraud_detection_data
FROM @fraud_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE'; -- or 'SKIP_FILE' to skip problematic files


TRUNCATE TABLE fact_fraud_detection;


-- Insert into fact_fraud_detection with data transformation
INSERT INTO fact_fraud_detection (
    transaction_id,
    time_step,
    sender_id,
    bene_id,
    usd_amount,
    label,
    country_code,
    sector_id,
    type_code
)
SELECT
    d.transaction_id,
    d.time_step,
    d.sender_id,
    d.bene_id,
    d.usd_amount,
    d.label,
    c.country_code,
    s.sector_id,
    t.type_code
FROM
    fraud_detection_data d
LEFT JOIN dim_country c
    ON d.sender_country = c.country_name
LEFT JOIN dim_sector s
    ON d.sender_sector = s.sector_id  -- Change this to match `sector_id` in `dim_sector`
LEFT JOIN dim_transaction_type t
    ON d.transaction_type = t.type_code;  -- Change this to match `type_code` in `dim_transaction_type`



-- Check count of records in fact table
SELECT COUNT(*) FROM fact_fraud_detection;

-- Sample data verification
SELECT * FROM fact_fraud_detection LIMIT 10;

-- Check for records with invalid country_code
SELECT * FROM fact_fraud_detection
WHERE country_code IS NULL;

-- Check for records with invalid sector_id
SELECT * FROM fact_fraud_detection
WHERE sector_id IS NULL;

-- Check for records with invalid type_code
SELECT * FROM fact_fraud_detection
WHERE type_code IS NULL;

-- Add missing entries to dimension tables if needed
-- Example for country
INSERT INTO dim_country (country_code, country_name)
SELECT DISTINCT sender_country AS country_name, sender_country AS country_code
FROM fraud_detection_data
WHERE sender_country NOT IN (SELECT country_code FROM dim_country);

INSERT INTO dim_sector (sector_id, sector_name)
SELECT DISTINCT sender_sector AS sector_id, 
       'Sector ' || sender_sector AS sector_name
FROM fraud_detection_data
WHERE sender_sector NOT IN (SELECT sector_id FROM dim_sector);

-- Add missing entries to the transaction type dimension table
INSERT INTO dim_transaction_type (type_code, description)
SELECT DISTINCT transaction_type AS type_code, 
       transaction_type AS description
FROM fraud_detection_data
WHERE transaction_type NOT IN (SELECT type_code FROM dim_transaction_type);

-- *********************************************************************************************
-- ** FEATURE ENGINEERING **

// Transaction Frequency per Sender
CREATE OR REPLACE VIEW sender_transaction_frequency AS
SELECT sender_id, COUNT(*) AS transaction_count
FROM fact_fraud_detection
GROUP BY sender_id;

// Average Transaction Amount per Sender
CREATE OR REPLACE VIEW sender_avg_transaction_amount AS
SELECT sender_id, AVG(usd_amount) AS avg_amount
FROM fact_fraud_detection
GROUP BY sender_id;

// Transaction Count per Country
CREATE OR REPLACE VIEW country_transaction_count AS
SELECT country_code, COUNT(*) AS transaction_count
FROM fact_fraud_detection
GROUP BY country_code;

// Transaction Amount by Type
CREATE OR REPLACE VIEW transaction_amount_by_type AS
SELECT type_code, AVG(usd_amount) AS avg_amount
FROM fact_fraud_detection
GROUP BY type_code;

DROP TABLE FRAUD_DETECTION_DATA;
DROP TABLE FACT_FRAUD_DETECTION;
DROP TABLE DIM_COUNTRY;
DROP TABLE DIM_SECTOR;
DROP TABLE DIM_TRANSACTION_TYPE;

DROP STAGE FRAUD_STAGE;

DROP VIEW COUNTRY_TRANSACTION_COUNT;
DROP VIEW SENDER_AVG_TRANSACTION_AMOUNT;
DROP VIEW SENDER_TRANSACTION_FREQUENCY;
DROP VIEW TRANSACTION_AMOUNT_BY_TYPE;

DROP DATABASE FRAUD_DETECTION_DB;





























