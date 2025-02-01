-- models/stg_tweets.sql

WITH source_data AS (
    SELECT 
        get_json_object(raw_data, '$.data') AS raw_data
    FROM 
        fast_campus.default.tweet_search
)

SELECT 
    explode(from_json(raw_data, 'array<struct<id:string,text:string>>')) AS tweet
FROM 
    source_data

