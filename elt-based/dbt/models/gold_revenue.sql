select toDate(event_time) day,segment,sum(amount) revenue,count() orders
from {{ ref('gold_orders') }} where not deleted and status='paid'
group by day,segment
