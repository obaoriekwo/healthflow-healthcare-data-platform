-- 30-day readmission logic, one row per hospital stay. Full definition:
-- docs/readmission_definition.md. In short:
--
--   Index stay (denominator): an inpatient stay with a discharge, where the patient
--     - did not die in hospital,
--     - was at least min_index_age_years old at admission,
--     - was discharged early enough that the full follow-up window is observable
--       (discharge date + window <= the latest date the data covers).
--   Readmission (numerator): the index stay is followed by an UNPLANNED stay for the same
--     patient that starts after discharge and within readmission_window_days.
--     Transfers are already merged into one stay, so they never count.
--     A stay is counted once, however many readmissions follow it.
--     A readmission can itself be an index stay.

{% set window_days = var("readmission_window_days") %}

with episodes as (

    select * from {{ ref('int_inpatient_episodes') }}

),

data_coverage as (

    -- The latest day we have seen activity for. Discharges whose 30-day follow-up runs past
    -- it cannot be judged yet. This uses event dates, not delivery dates: late-arriving
    -- deliveries continue for days after the last event and would otherwise make the
    -- follow-up look complete when it is not.
    select max(start_date) as data_through
    from {{ ref('stg_encounters') }}

),

first_unplanned_readmission as (

    select distinct on (idx.episode_id)
        idx.episode_id,
        nxt.episode_id   as readmission_episode_id,
        nxt.admitted_at  as readmitted_at
    from episodes as idx
    inner join episodes as nxt
        on  nxt.patient_id = idx.patient_id
        and nxt.admitted_at > idx.discharged_at
        and nxt.admitted_at <= idx.discharged_at + interval '{{ window_days }} days'
        and not nxt.is_planned
    order by idx.episode_id, nxt.admitted_at

),

evaluated as (

    select
        e.*,
        r.readmission_episode_id,
        r.readmitted_at,
        round(extract(epoch from r.readmitted_at - e.discharged_at) / 86400.0, 1) as days_to_readmission,
        case
            when e.died_in_hospital
                then 'died_in_hospital'
            when e.age_at_admission < {{ var("min_index_age_years") }}
                then 'under_minimum_age'
            when e.discharge_date + {{ window_days }} > c.data_through
                then 'follow_up_window_incomplete'
        end as index_exclusion_reason
    from episodes as e
    cross join data_coverage as c
    left join first_unplanned_readmission as r
        on r.episode_id = e.episode_id

)

select
    *,
    index_exclusion_reason is null                                          as is_index_stay,
    index_exclusion_reason is null and readmission_episode_id is not null   as is_readmitted_within_window
from evaluated
