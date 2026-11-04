-- Department workload. Grain: one row per month and admitting department.
-- Stays are attributed to the month of discharge, the usual convention for
-- length-of-stay reporting (a stay's length is only known once it ends).

select
    date_trunc('month', s.discharged_at at time zone 'UTC')::date           as discharge_month,
    s.admitting_department_key                                              as department_key,
    d.department_name,
    d.service_line,
    count(*)                                                                as discharges,
    count(distinct s.patient_id)                                            as patients,
    count(*) filter (where s.is_planned)                                    as planned_stays,
    count(*) filter (where not s.is_planned)                                as unplanned_stays,
    sum(s.length_of_stay_days)                                              as total_stay_days,
    round(avg(s.length_of_stay_days), 2)                                    as avg_length_of_stay_days,
    round((percentile_cont(0.5) within group (order by s.length_of_stay_days))::numeric, 2)
                                                                            as median_length_of_stay_days,
    count(*) filter (where s.included_critical_care)                        as stays_with_critical_care,
    sum(s.transfer_count)                                                   as transfers,
    count(*) filter (where s.died_in_hospital)                              as in_hospital_deaths,
    sum(s.total_claim_cost)                                                 as total_claim_cost
from {{ ref('fact_hospital_stays') }} as s
inner join {{ ref('dim_department') }} as d
    on d.department_key = s.admitting_department_key
group by 1, 2, 3, 4
