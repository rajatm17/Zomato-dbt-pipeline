select
order_date,
city,
count(*) as orders,
count_if(is_delivered) as delivered_orders,
round(div0(count_if(order_status = 'Cancelled'), count(*)),4) as cancel_rate,
sum(case when is_delivered then sales_amount else 0 end) as gmv,
round(div0(sum(case when is_delivered then sales_amount else 0 end), count_if(is_delivered)),4) as aov
from {{ref('fact_orders')}}
group by 1,2