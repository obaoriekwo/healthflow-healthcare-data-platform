-- 30-day readmissions. Grain: one row per discharge month and admitting department.
--
-- Rates cannot be averaged across rows. Report by summing readmissions and index_stays
-- first, then dividing (in Power BI: DIVIDE(SUM(readmissions), SUM(index_stays))).
-- Definition: docs/readmission_definition.md.

select
    date_trunc('month', discharged_at at time zone 'UTC')::date             as discharge_month,
    admitting_department_key                                                as department_key,
    count(*)                                                                as discharges,
    count(*) filter (where is_index_stay)                                   as index_stays,
    count(*) filter (where is_readmitted_within_window)                     as readmissions,
    round(
        100.0 * count(*) filter (where is_readmitted_within_window)
        / nullif(count(*) filter (where is_index_stay), 0),
        1
    )                                                                       as readmission_rate_pct,
    count(*) filter (where index_exclusion_reason = 'died_in_hospital')     as excluded_died,
    count(*) filter (where index_exclusion_reason = 'under_minimum_age')    as excluded_under_age,
    count(*) filter (where index_exclusion_reason = 'follow_up_window_incomplete')
                                                                            as excluded_follow_up_incomplete,
    round(avg(days_to_readmission) filter (where is_readmitted_within_window), 1)
                                                                            as avg_days_to_readmission
from {{ ref('fact_hospital_stays') }}
group by 1, 2
