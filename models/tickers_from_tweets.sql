SELECT
    date,
    hour,
    explode(regexp_extract_all(tweet_text, '\\$([A-Z]+)')) as ticker,
    count(1) as count
FROM
    {{ ref('tweet_text') }}
GROUP BY 1, 2, 3
ORDER BY 4 DESC
