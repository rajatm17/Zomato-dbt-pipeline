select city,
hour(order_timestamp) as order_hour,
count_if(is_delivered) as delivered_orders,
round(median(delivery_time_min),1) as p50,
round(percentile_cont(0.95) within group (order by delivery_time_min),1) as p95
from {{ref('fact_orders')}}
where is_delivered
group by 1,2