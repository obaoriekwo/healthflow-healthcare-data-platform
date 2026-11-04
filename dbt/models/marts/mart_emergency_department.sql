-- Emergency department performance. Grain: one row per day with ED visits.
--
-- Time in department = encounter end minus start. The 4-hour measure follows the NHS
-- England A&E standard (share of patients admitted, transferred or discharged within
-- 4 hours of arrival). Synthea has no triage or waiting-room timestamps; those come
-- with the synthetic operational data planned for a later phase.

select
    start_date_key                                                          as date_key,
    count(*)                                                                as ed_visits,
    count(distinct patient_id)                                              as ed_patients,
    round(avg(duration_hours), 2)                                           as avg_hours_in_department,
    round((percentile_cont(0.5) within group (order by duration_hours))::numeric, 2)
                                                                            as median_hours_in_department,
    round((percentile_cont(0.95) within group (order by duration_hours))::numeric, 2)
                                                                            as p95_hours_in_department,
    count(*) filter (where duration_hours <= 4)                             as within_4_hours,
    count(*) filter (where duration_hours > 4)                              as over_4_hours,
    count(*) filter (where duration_hours > 12)                             as over_12_hours
from {{ ref('fact_encounters') }}
where encounter_class = 'emergency'
group by 1
