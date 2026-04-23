--SILVER LAYER: clean tables

-- Create the schema (silver layer) if it doesn't exist
CREATE SCHEMA IF NOT EXISTS airbnb_project.silver;

-- 3. Tell current session which database/schema to use
USE DATABASE airbnb_project;
USE SCHEMA silver;

-- Create silver listings table
CREATE OR REPLACE TABLE airbnb_project.silver.listings_silver AS
SELECT
    id AS listing_id,
    listing_url,
    TO_DATE(last_scraped) AS scraped_date,
    name,
    host_id,
    host_name,
    host_since,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    host_is_superhost,
    host_total_listings_count,
    neighbourhood_cleansed as neighbourhood,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bathrooms,
    bedrooms,
    beds,
    CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS DECIMAL(10,2)) AS price_per_night,
    minimum_nights,
    maximum_nights,
    availability_30,
    availability_60,
    availability_90,
    availability_365,
    number_of_reviews,
    availability_eoy,
    estimated_occupancy_l365d,
    estimated_revenue_l365d,
    review_scores_rating,
    review_scores_cleanliness,
    review_scores_communication,
    review_scores_location,
    UPPER(TRIM(license)) AS str_license,
    CASE 
        WHEN license ILIKE 'str-%' THEN 'Registered STR'
        WHEN license ILIKE 'exempt' THEN 'Exempt'
        WHEN license ILIKE '%government%' OR license ILIKE '%approved%' THEN 'Gov Approved'
        WHEN license IS NULL OR TRIM(license) = '' THEN 'Unlicensed/Missing'
        ELSE 'Other/Non-Standard'
    END AS license_type
FROM airbnb_project.raw.listings_raw
WHERE price IS NOT NULL 
  AND price != ''
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_scraped DESC) = 1;


-- Create Silver reviews table
CREATE OR REPLACE TABLE airbnb_project.silver.reviews_silver AS
SELECT
    listing_id,
    id AS review_id,
    TO_DATE(date) AS review_date, 
    reviewer_id,
    --comments,
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(comments, '<[^>]+>', ' '), -- 1. Remove HTML
                '[^[:ascii:]]', ''                        -- 2. Remove Emojis
            ), 
            ' {2,}', ' '                                  -- 3. Collapse Spaces
        )
    ) AS processed_comments
FROM airbnb_project.raw.reviews_raw
WHERE comments IS NOT NULL
  -- 1. Exclude "No Review" variations (Case Insensitive)
  AND processed_comments NOT ILIKE '%no review%'
  -- 2. Ensure the review has at least one Letter or Number
  AND processed_comments REGEXP '.*[A-Za-z0-9].*'
-- 3. Final Deduplication
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY review_date DESC) = 1;


--Create silver short_term_registrations table
CREATE OR REPLACE TABLE airbnb_project.silver.str_reg_silver AS
SELECT
    _id,
    UPPER(TRIM(operator_registration_number)) AS registration_number,
    address,
    unit,
    postal_code,
    property_type,
    ward_number,
    ward_name
FROM airbnb_project.raw.str_reg_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY registration_number ORDER BY _id DESC) = 1;