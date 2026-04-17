--BRONZE LAYER for ingesting S3 files into snowflake

-- Set UTC timezone to match python
ALTER SESSION SET TIMEZONE = 'UTC';
-- Set the folder date, which can be updated as more recent data is uploaded
SET file_date = TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD');

-- 1. First, create the database
CREATE DATABASE IF NOT EXISTS airbnb_project;

-- 2. Create a schema for raw (bronze) data
CREATE SCHEMA IF NOT EXISTS airbnb_project.raw;

-- 3. Tell current session which database/schema to use
USE DATABASE airbnb_project;
USE SCHEMA raw;


-- If you haven't already set up a connection with Amazon S3, which only has to be done once:
-- Run CREATE STORAGE INTEGRATION and CREATE STAGE statements

--Change all csv.gz file formats into csv
CREATE FILE FORMAT IF NOT EXISTS airbnb_csv_gz
  TYPE = 'CSV'
  COMPRESSION = 'GZIP'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  PARSE_HEADER = TRUE 
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL');
  
--Keep all csv file formats as csv
CREATE FILE FORMAT IF NOT EXISTS toronto_standard_csv
  TYPE = 'CSV'
  COMPRESSION = 'NONE'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL');  
  

--BRONZE LAYER: this layer contains the raw copy of the airbnb data from S3

-- Create listings_raw table (bronze layer) in snowflake
CREATE OR REPLACE TABLE listings_raw (
    --already an integer
    id NUMBER, 
    listing_url STRING,
    last_scraped STRING,
    name STRING,
    --already an integer
    host_id NUMBER, 
    host_name STRING,
    host_since DATE,
    host_response_time STRING,
    host_response_rate STRING,
    host_acceptance_rate STRING,
    host_is_superhost STRING,
    host_total_listings_count NUMBER,
    neighbourhood_cleansed STRING,
    latitude FLOAT,
    longitude FLOAT,
    property_type STRING,
    room_type STRING,
    accommodates NUMBER,
    bathrooms NUMBER,
    bedrooms NUMBER,
    beds NUMBER,
    -- keep this as STRING first because of the '$'
    price STRING, 
    minimum_nights NUMBER,
    maximum_nights NUMBER,
    availability_30 NUMBER,
    availability_60 NUMBER,
    availability_90 NUMBER,
    availability_365 NUMBER,
    number_of_reviews NUMBER,
    availability_eoy NUMBER,
    estimated_occupancy_l365d NUMBER,
    estimated_revenue_l365d NUMBER,
    review_scores_rating FLOAT,
    review_scores_cleanliness FLOAT,
    review_scores_communication FLOAT,
    review_scores_location FLOAT,
    license STRING
);

-- Build the path string (ensure it starts with the date)
SET listings_regex = CONCAT('.*', $file_date, '/listings.csv.gz');

-- Clear the table
TRUNCATE TABLE listings_raw;

-- Load contents into listings_raw table
COPY INTO listings_raw
FROM @airbnb_toronto_stage
PATTERN = $listings_regex
FILE_FORMAT = (FORMAT_NAME = 'airbnb_csv_gz')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE' --skip small formatting errors
FORCE = TRUE; --force reload of data, even if it has already been loaded before



-- Create reviews_raw (bronze layer) table in snowflake
CREATE OR REPLACE TABLE reviews_raw (
    listing_id NUMBER,
    id NUMBER,
    date STRING, 
    reviewer_id NUMBER,
    reviewer_name STRING,
    comments STRING
);

SET reviews_regex = CONCAT('.*', $file_date, '/reviews.csv.gz');

-- Clear the table
TRUNCATE TABLE reviews_raw;

--Load contents into reviews_raw table
COPY INTO reviews_raw
FROM @airbnb_toronto_stage
PATTERN = $reviews_regex
FILE_FORMAT = (FORMAT_NAME = 'airbnb_csv_gz')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE'
FORCE = TRUE;



--Create short term rental registraion raw (bronze layer) data table in snowflake
CREATE OR REPLACE TABLE str_reg_raw (
    _id NUMBER,
    operator_registration_number STRING,
    address STRING,
    unit STRING,
    postal_code STRING,
    property_type STRING,
    ward_number NUMBER,
    ward_name STRING
);

SET str_reg_regex = CONCAT('.*', $file_date, '/short-term-rental-registrations-data.csv');

--Clear the table
TRUNCATE TABLE str_reg_raw;

--Load contents into str_reg_raw table
COPY INTO str_reg_raw
FROM @airbnb_toronto_stage
PATTERN = $str_reg_regex
FILE_FORMAT = (FORMAT_NAME = 'toronto_standard_csv')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE'
FORCE = TRUE; 
