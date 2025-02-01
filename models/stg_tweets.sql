-- models/stg_tweets.sql

WITH source_data AS (
    SELECT 
        'data'::json AS raw_data
    FROM 
        'fast_campus.default.tweet_search'
)

SELECT 
    json_array_elements(raw_data) AS tweet
FROM 
    source_data

