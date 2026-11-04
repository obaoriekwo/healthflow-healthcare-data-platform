-- Corrections must win: the fact holds the same claim cost as the newest raw version.
select f.encounter_id, f.total_claim_cost, s.total_claim_cost as latest_total_claim_cost
from {{ ref('fact_encounters') }} as f
inner join {{ ref('stg_encounters') }} as s
    on s.encounter_id = f.encounter_id
where f.total_claim_cost is distinct from s.total_claim_cost
