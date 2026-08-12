SELECT
    order_id,
    order_timestamp,
    order_date,
    user_id AS customer_id,
    r_id AS restaurant_id,
    TRIM(
        COALESCE(REGEXP_SUBSTR(restaurant_city, '[^,]+$'), restaurant_city)
    ) AS city,
    cuisine,
    items_count,
    sales_qty,
    subtotal,
    discount,
    delivery_fee,
    gst,
    sales_amount,
    currency,
    payment_method,
    order_status,
    (
        order_status = 'Delivered'
    ) AS is_delivered,
    customer_rating,
    delivery_time_min
FROM
    {{ source(
        'raw',
        'orders'
    ) }}
