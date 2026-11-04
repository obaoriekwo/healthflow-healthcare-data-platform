-- Reconciliation: every inpatient encounter in staging belongs to exactly one stay.
select s.encounter_id
from {{ ref('stg_encounters') }} as s
left join {{ ref('int_inpatient_encounters') }} as i
    on i.encounter_id = s.encounter_id
where s.encounter_class = 'inpatient'
  and i.encounter_id is null
