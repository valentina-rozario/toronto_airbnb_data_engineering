import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
from connection_config import params
import os

def run_gold_pipeline(session):
    print(f"Success! Connected to {session.get_current_database()}")

    # Environment Setup
    session.sql("ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60, AUTO_RESUME = TRUE;").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS airbnb_project.gold").collect()
    session.use_schema("GOLD")

    # Define the UDF (user-defined function)
    @F.udf(name="analyze_reviews_v4", is_permanent=False, replace=True, packages=["langdetect", "vaderSentiment"])
    def analyze_reviews_v4(text: str) -> dict:
        from langdetect import detect
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        
        analyzer = SentimentIntensityAnalyzer() # Worker-local initialization
        
        if text is None or len(text.strip()) < 3:
            return {"lang": "unknown", "score": 0.0}
        
        try:
            lang = detect(text)
        except:
            lang = "unknown"
        
        score = 0.0
        if lang == 'en':
            score = analyzer.polarity_scores(text)['compound']
        
        return {"lang": lang, "score": score}

    # Perform sentiment analysis on reviews not already in the previous reviews_gold table
    # check if the table exists first to avoid a crash on the very first run
    try:
        session.table("airbnb_project.gold.reviews_sentiment").limit(1).collect()
        df_reviews = session.table("airbnb_project.silver.reviews_silver").filter(
            ~F.col("review_id").in_(
                session.table("airbnb_project.gold.reviews_sentiment").select("review_id")
            )
        )
    except:
        print("Gold table doesn't exist yet. Performing full load.")
        df_reviews = session.table("airbnb_project.silver.reviews_silver")

    # Add columns for detected language, sentiment score, and review type
    df_processed = df_reviews.with_column("results", analyze_reviews_v4(F.col("processed_comments")))

    df_final = df_processed.select(
        "*",
        F.col("results")['lang'].as_("detected_lang"),
        F.col("results")['score'].as_("sentiment_score")
    ).with_column("sentiment_label", 
        F.when(F.col("detected_lang") != 'en', "Non-English")
         .when(F.col("sentiment_score") >= 0.05, "Positive")
         .when(F.col("sentiment_score") <= -0.05, "Negative")
         .otherwise("Neutral")
    )

    # Append new data to reviews_gold table
    df_final.write.mode("append").save_as_table("airbnb_project.gold.reviews_sentiment")
    print("Sentiment analysis updated.")

    # Create final listings_gold table used for BI reporting dashboard
    session.sql("""
    CREATE OR REPLACE TABLE airbnb_project.gold.listings_gold AS
    WITH sentiment_counts AS (
        SELECT 
            listing_id, 
            COUNT(*) AS total_reviews_processed,
            COUNT(CASE WHEN sentiment_label = 'Positive' THEN 1 END) AS count_positive,
            COUNT(CASE WHEN sentiment_label = 'Negative' THEN 1 END) AS count_negative,
            COUNT(CASE WHEN sentiment_label = 'Neutral' THEN 1 END) AS count_neutral,
            COUNT(CASE WHEN sentiment_label = 'Non-English' THEN 1 END) AS count_non_english
        FROM airbnb_project.gold.reviews_sentiment
        GROUP BY listing_id
    ),
    final_join AS (
        SELECT 
            l.LISTING_URL, l.HOST_ID, l.HOST_NAME, l.HOST_RESPONSE_TIME,
            l.HOST_ACCEPTANCE_RATE, l.HOST_IS_SUPERHOST, l.NEIGHBOURHOOD,
            l.LATITUDE, l.LONGITUDE, l.PROPERTY_TYPE, l.ROOM_TYPE,
            l.ACCOMMODATES, l.PRICE_PER_NIGHT, l.MINIMUM_NIGHTS,
            l.MAXIMUM_NIGHTS, l.AVAILABILITY_30, l.AVAILABILITY_365,
            l.NUMBER_OF_REVIEWS, l.ESTIMATED_OCCUPANCY_L365D,
            l.ESTIMATED_REVENUE_L365D, l.REVIEW_SCORES_RATING,
            l.STR_LICENSE, l.LICENSE_TYPE,
            CASE WHEN r.registration_number IS NOT NULL THEN 'Active' ELSE 'Inactive' END AS license_status,
            ((365 - l.availability_365) * l.price_per_night) AS my_est_annual_revenue,
            COALESCE(s.total_reviews_processed, 0) AS total_reviews,
            COALESCE(s.count_positive, 0) AS pos_reviews,
            COALESCE(s.count_negative, 0) AS neg_reviews
        FROM airbnb_project.silver.listings_silver l
        LEFT JOIN airbnb_project.silver.str_reg_silver r ON l.str_license = r.registration_number
        LEFT JOIN sentiment_counts s ON l.id = s.listing_id
    )
    SELECT * FROM final_join;
    """).collect()
    print("Gold Listings table refreshed.")


#Configure Snowflake credentials
def get_params():
    # Try to get credentials from GitHub environment variables first
    account = os.getenv('SNOW_ACCOUNT')
    user = os.getenv('SNOW_USER')
    password = os.getenv('SNOW_PASS')

    # If they are missing use the local file
    if not account or not password:
        from connection_config import params
        return params
    
    # If they ARE present, build the dictionary manually
    return {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": "COMPUTE_WH",
        "database": "AIRBNB_PROJECT",
        "schema": "GOLD"
    }



if __name__ == "__main__":
    # Create the session
    params = get_params()
    new_session = Session.builder.configs(params).create()
    
    try:
        run_gold_pipeline(new_session)
    finally:
        new_session.close()
        print("Snowflake session closed.")