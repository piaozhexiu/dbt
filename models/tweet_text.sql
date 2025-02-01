-- models/tweet_texts.sql

WITH tweet_data AS (
    SELECT 
        tweet->>'text' AS extracted_text
    FROM 
        {{ ref('stg_tweets') }}
)

SELECT 
    extracted_text
FROM 
    tweet_data

