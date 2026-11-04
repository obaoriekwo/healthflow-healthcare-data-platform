-- The mart must add up to the stay-level fact (no stays lost or double-counted by grouping).
with mart as (
    select sum(discharges) as discharges, sum(index_stays) as index_stays, sum(readmissions) as readmissions
    from {{ ref('mart_patient_readmissions') }}
),

stays as (
    select count(*) as discharges,
           count(*) filter (where is_index_stay) as index_stays,
           count(*) filter (where is_readmitted_within_window) as readmissions
    from {{ ref('fact_hospital_stays') }}
)

select *
from mart, stays
where (mart.discharges, mart.index_stays, mart.readmissions)
      is distinct from (stays.discharges, stays.index_stays, stays.readmissions)
