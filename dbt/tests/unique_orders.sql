select order_id from {{ ref('gold_orders') }} group by order_id having count()!=1
