-- Daily hospital activity. Grain: one row per day.
-- Admissions and discharges count hospital stays (transfers merged), not encounters.

with days as (

    select date_key, calendar_date
    from {{ ref('dim_date') }}
    where calendar_date between (select min(start_date) from {{ ref('stg_encounters') }})
                            and (select max(latest_available_date) from {{ ref('stg_encounters') }})

),

encounters as (

    select
        start_date_key                                              as date_key,
        count(*)                                                    as encounters,
        count(distinct patient_id)                                  as patients_seen,
        count(*) filter (where encounter_class = 'emergency')       as emergency_visits,
        count(*) filter (where encounter_class = 'urgentcare')      as urgent_care_visits,
        count(*) filter (where encounter_class in ('ambulatory', 'outpatient', 'wellness'))
                                                                    as outpatient_visits,
        sum(total_claim_cost)                                       as total_claim_cost
    from {{ ref('fact_encounters') }}
    group by 1

),

admissions as (

    select
        admission_date_key                                          as date_key,
        count(*)                                                    as admissions,
        count(*) filter (where not is_planned)                      as unplanned_admissions
    from {{ ref('fact_hospital_stays') }}
    group by 1

),

discharges as (

    select
        discharge_date_key                                          as date_key,
        count(*)                                                    as discharges,
        sum(length_of_stay_days)                                    as discharged_stay_days
    from {{ ref('fact_hospital_stays') }}
    group by 1

),

occupied as (

    -- Patients in a bed at midnight (the standard daily census definition).
    select
        to_char(d.calendar_date, 'YYYYMMDD')::int                   as date_key,
        count(s.hospital_stay_id)                                   as midnight_census
    from days as d
    left join {{ ref('fact_hospital_stays') }} as s
        on  s.admitted_at < (d.calendar_date + 1)::timestamp at time zone 'UTC'
        and s.discharged_at >= (d.calendar_date + 1)::timestamp at time zone 'UTC'
    group by 1

)

select
    d.date_key,
    d.calendar_date,
    coalesce(e.encounters, 0)               as encounters,
    coalesce(e.patients_seen, 0)            as patients_seen,
    coalesce(e.emergency_visits, 0)         as emergency_visits,
    coalesce(e.urgent_care_visits, 0)       as urgent_care_visits,
    coalesce(e.outpatient_visits, 0)        as outpatient_visits,
    coalesce(a.admissions, 0)               as admissions,
    coalesce(a.unplanned_admissions, 0)     as unplanned_admissions,
    coalesce(x.discharges, 0)               as discharges,
    coalesce(x.discharged_stay_days, 0)     as discharged_stay_days,  -- sum / discharges = average length of stay
    o.midnight_census,
    coalesce(e.total_claim_cost, 0)         as total_claim_cost
from days as d
left join encounters as e on e.date_key = d.date_key
left join admissions as a on a.date_key = d.date_key
left join discharges as x on x.date_key = d.date_key
left join occupied as o on o.date_key = d.date_key
