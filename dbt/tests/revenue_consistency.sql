select 1 where (select coalesce(sum(revenue),0) from {{ ref('gold_revenue') }}) != (select coalesce(sum(amount),0) from {{ ref('gold_orders') }} where not deleted and status='paid')
