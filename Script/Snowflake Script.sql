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

SELECT * FROM fraud_detection_data;
