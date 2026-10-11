-- Full-history reference resolver. No arrival-time assumption.
-- Retain tombstones until AFTER choosing the latest version.
select kind,id,revision,op,data,ingested_at from (
 select *, row_number() over(partition by kind,id order by revision desc,
 source_lsn desc, offset_id desc, event_id desc) as rn
 from {{ source('bronze','bronze_events') }}
) where rn=1
