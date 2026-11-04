-- Grain: one row per hospital stay (inpatient episode, transfers merged).
--
-- Not incremental on purpose: a stay's readmission flag can change when a later
-- admission arrives, and whether it is an index stay changes as the follow-up window
-- becomes observable. Rebuilding is cheap at this size and always correct.
select
    r.episode_id                                                as hospital_stay_id,
    p.patient_key,
    r.patient_id,
    to_char(r.admission_date, 'YYYYMMDD')::int                  as admission_date_key,
    to_char(r.discharge_date, 'YYYYMMDD')::int                  as discharge_date_key,
    r.admitting_department_code                                 as admitting_department_key,
    r.discharging_department_code                               as discharging_department_key,
    r.organization_id,
    r.admitted_at,
    r.discharged_at,
    r.length_of_stay_days,
    r.transfer_count,
    r.included_critical_care,
    r.is_planned,
    r.planned_reason,
    r.age_at_admission,
    r.died_in_hospital,
    r.total_claim_cost,
    r.is_index_stay,
    r.index_exclusion_reason,
    r.is_readmitted_within_window,
    r.readmission_episode_id                                    as readmission_stay_id,
    r.days_to_readmission
from {{ ref('int_readmissions') }} as r
left join {{ ref('dim_patient') }} as p
    on  p.patient_id = r.patient_id
    and r.admitted_at >= p.valid_from
    and (p.valid_to is null or r.admitted_at < p.valid_to)
