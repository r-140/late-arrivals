-- Deliberately wrong: this excludes old business facts received today.
select * from {{ ref('gold_orders') }} where event_time>=now()-interval 2 day
