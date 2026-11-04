-- Reconciliation, source to target: every distinct encounter id received in raw is in
-- fact_encounters exactly once, and nothing in the fact is missing from raw.
-- Catches incremental logic errors (for example a lookback that skips late data).
with source_ids as (
    select distinct id as encounter_id from {{ source('raw', 'encounters') }}
),

fact_ids as (
    select encounter_id from {{ ref('fact_encounters') }}
)

select coalesce(s.encounter_id, f.encounter_id) as encounter_id,
       case when f.encounter_id is null then 'missing from fact' else 'not in source' end as problem
from source_ids as s
full outer join fact_ids as f
    on f.encounter_id = s.encounter_id
where s.encounter_id is null or f.encounter_id is null
