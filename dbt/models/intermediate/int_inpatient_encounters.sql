-- Inpatient encounters, each assigned to a hospital stay (episode).
--
-- Synthea records a transfer (for example ward -> ICU) as a separate inpatient encounter
-- that overlaps or immediately follows the previous one. Counting those separately would
-- turn every transfer into a "readmission". Rule: an inpatient encounter that starts no
-- later than transfer_gap_hours after the latest discharge so far belongs to the same stay.
-- The running max handles nested and overlapping encounters.
--
-- Department and "planned" come from two seeds. The admission type (encounter code) is
-- the default; the admission reason overrides it, because Synthea uses the same generic
-- code ("Encounter for problem") for chemotherapy cycles and for appendicitis.

with inpatient as (

    select
        e.encounter_id,
        e.patient_id,
        e.organization_id,
        e.encounter_code,
        e.encounter_description,
        e.reason_code,
        e.started_at,
        e.ended_at,
        e.total_claim_cost,
        coalesce(r.department_code, a.department_code, 'UNMAPPED')  as department_code,
        coalesce(a.is_planned, false) or coalesce(r.is_planned, false)
                                                                    as is_planned,
        case
            when r.is_planned then r.planned_reason
            when a.is_planned then a.planned_reason
        end                                                         as planned_reason
    from {{ ref('stg_encounters') }} as e
    left join {{ ref('admission_types') }} as a
        on a.admission_code = e.encounter_code
    left join {{ ref('admission_reasons') }} as r
        on r.reason_code = e.reason_code
    where e.encounter_class = 'inpatient'

),

with_previous_end as (

    select
        *,
        max(ended_at) over (
            partition by patient_id
            order by started_at, encounter_id
            rows between unbounded preceding and 1 preceding
        ) as previous_max_ended_at
    from inpatient

),

numbered as (

    select
        *,
        sum(
            case
                when previous_max_ended_at is null then 1
                when started_at > previous_max_ended_at + interval '{{ var("transfer_gap_hours") }} hours' then 1
                else 0
            end
        ) over (partition by patient_id order by started_at, encounter_id) as episode_number
    from with_previous_end

)

select
    -- The episode is named after its first encounter, so it is stable and traceable.
    first_value(encounter_id) over (
        partition by patient_id, episode_number order by started_at, encounter_id
    )                                                                         as episode_id,
    encounter_id,
    patient_id,
    episode_number,
    row_number() over (
        partition by patient_id, episode_number order by started_at, encounter_id
    )                                                                         as sequence_in_episode,
    organization_id,
    encounter_code,
    encounter_description,
    department_code,
    is_planned,
    planned_reason,
    started_at,
    ended_at,
    total_claim_cost
from numbered
