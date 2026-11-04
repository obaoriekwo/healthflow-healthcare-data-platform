-- One row per hospital stay (episode): admission to final discharge, transfers merged.
-- The admitting encounter decides the department and whether the stay was planned.

with encounters as (

    select * from {{ ref('int_inpatient_encounters') }}

),

departments as (

    select * from {{ ref('departments') }}

),

episodes as (

    select
        e.episode_id,
        e.patient_id,
        min(e.started_at)                                                          as admitted_at,
        max(e.ended_at)                                                            as discharged_at,
        count(*)                                                                   as encounter_count,
        sum(e.total_claim_cost)                                                    as total_claim_cost,
        bool_or(d.is_critical_care)                                                as included_critical_care,
        (array_agg(e.organization_id order by e.sequence_in_episode))[1]          as organization_id,
        (array_agg(e.department_code order by e.sequence_in_episode))[1]          as admitting_department_code,
        (array_agg(e.department_code order by e.sequence_in_episode desc))[1]     as discharging_department_code,
        (array_agg(e.is_planned order by e.sequence_in_episode))[1]               as is_planned,
        (array_agg(e.planned_reason order by e.sequence_in_episode))[1]           as planned_reason
    from encounters as e
    inner join departments as d
        on d.department_code = e.department_code
    group by e.episode_id, e.patient_id

)

select
    ep.episode_id,
    ep.patient_id,
    ep.organization_id,
    ep.admitting_department_code,
    ep.discharging_department_code,
    ep.admitted_at,
    ep.discharged_at,
    (ep.admitted_at at time zone 'UTC')::date                                     as admission_date,
    (ep.discharged_at at time zone 'UTC')::date                                   as discharge_date,
    round(extract(epoch from ep.discharged_at - ep.admitted_at) / 86400.0, 2)     as length_of_stay_days,
    ep.encounter_count - 1                                                        as transfer_count,
    ep.included_critical_care,
    ep.is_planned,
    ep.planned_reason,
    ep.total_claim_cost,
    extract(year from age((ep.admitted_at at time zone 'UTC')::date, p.birth_date))::int
                                                                                  as age_at_admission,
    p.death_date,
    coalesce(
        p.death_date between (ep.admitted_at at time zone 'UTC')::date
                         and (ep.discharged_at at time zone 'UTC')::date,
        false
    )                                                                             as died_in_hospital
from episodes as ep
left join {{ ref('stg_patients') }} as p
    on p.patient_id = ep.patient_id
