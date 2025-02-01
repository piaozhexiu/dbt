-- models/stg_tweets.sql

WITH source_data AS (
    SELECT 
        'data'::json AS raw_data  -- Replace 'json_data_field' with the actual field name containing the JSON.
    FROM 
        'fast_campus.default.tweet_search'  -- Replace 'source_table' with the actual table name containing the JSON data.
)

SELECT 
    json_array_elements(raw_data) AS tweet
FROM 
    source_data

