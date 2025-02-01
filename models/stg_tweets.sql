-- models/stg_tweets.sql
SELECT
  tweet.text
FROM
  (
    SELECT
      explode(
        from_json(data, 'array<struct<id:string, text:string, edit_history_tweet_ids:array<string>>>')
      ) AS tweet
    FROM
      fast_campus.default.tweet_search
  )

