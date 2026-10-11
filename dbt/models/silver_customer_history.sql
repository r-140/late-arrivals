with versions as (
 select id as version_id, JSONExtractString(data,'customer_id') as customer_id,
 parseDateTime64BestEffort(JSONExtractString(data,'effective_from'),3,'UTC') as valid_from,
 JSONExtractString(data,'segment') as segment, revision
 from {{ ref('silver_state') }} where kind='customer_history' and op!='d'
), canonical as (
 select * from (select *,row_number() over(partition by customer_id,valid_from order by revision desc,version_id desc) rn from versions) where rn=1
)
select customer_id,version_id,segment,valid_from,
 leadInFrame(valid_from,1,toDateTime64('2100-01-01',3,'UTC')) over(
 partition by customer_id order by valid_from rows between unbounded preceding and unbounded following) as valid_to
from canonical
