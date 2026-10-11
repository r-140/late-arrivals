{{ config(materialized='incremental',unique_key='order_id',incremental_strategy='legacy') }}
-- Small-lab correctness-first reconciliation: every known order key is re-emitted.
-- Tombstones and cancelled rows stay here; aggregates exclude them.
with orders as (
 select id as order_id,JSONExtractString(data,'customer_id') customer_id,
 JSONExtractFloat(data,'amount') amount,
 parseDateTime64BestEffort(JSONExtractString(data,'event_time'),3,'UTC') event_time,
 JSONExtractString(data,'status') status,op='d' as deleted
 from {{ ref('silver_state') }} where kind='order'
), customers as (
 select id from {{ ref('silver_state') }} where kind='customer' and op!='d'
)
select o.*,c.id!='' as customer_known,
 if(h.version_id='','unknown',h.segment) segment
from orders o left join customers c on o.customer_id=c.id
left join {{ ref('silver_customer_history') }} h
on o.customer_id=h.customer_id
and o.event_time>=h.valid_from and o.event_time<h.valid_to
