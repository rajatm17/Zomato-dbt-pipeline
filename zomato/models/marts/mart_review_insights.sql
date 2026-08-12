{{config(tags=['ai'])}}


select 
rr.city,
e.topic,
e.SENTIMENT_LABEL,
count(*) as reviews,
round(avg(e.sentiment_score),3) as avg_sentiment_score,
round(avg(rr.rating),3) as avg_star_rating,
count_if(e.key_issue is not null) as flagged_issues
from {{source('ai','enriched_reviews')}} e
inner join {{ref('stg_reviews')}} rr using (review_id)
group by 1,2,3
