{{
    config(
        materialized='incremental',
        unique_key='encounter_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per encounter (latest version).
--
-- Incremental: each run reprocesses only encounters whose newest version was loaded
-- since the last run, minus a lookback for safety. A correction to an old encounter is
-- loaded recently, so it is picked up and replaces the old row (delete+insert on
-- encounter_id). A full rebuild (dbt build --full-refresh) gives the same result.

with encounters as (

    select *
    from {{ ref('stg_encounters') }}
    {% if is_incremental() %}
    where last_ingested_at > (
        select coalesce(max(last_ingested_at), '1900-01-01'::timestamptz)
               - interval '{{ var("fact_lookback_days") }} days'
        from {{ this }}
    )
    {% endif %}

)

select
    e.encounter_id,
    p.patient_key,
    e.patient_id,
    to_char(e.start_date, 'YYYYMMDD')::int                      as start_date_key,
    to_char(e.end_date, 'YYYYMMDD')::int                        as end_date_key,
    i.department_code                                           as department_key,  -- inpatient only
    i.episode_id                                                as hospital_stay_id, -- inpatient only
    e.organization_id,
    e.provider_id,
    e.payer_id,
    e.encounter_class,
    e.encounter_code,
    e.encounter_description,
    e.reason_code,
    e.reason_description,
    e.started_at,
    e.ended_at,
    e.duration_hours,
    extract(year from age(e.start_date, p.birth_date))::int    as patient_age_at_encounter,
    e.base_encounter_cost,
    e.total_claim_cost,
    e.payer_coverage,
    e.total_claim_cost - e.payer_coverage                       as patient_cost,
    e.version_count > 1                                         as was_corrected,
    e.first_available_date,
    e.last_ingested_at
from encounters as e
left join {{ ref('dim_patient') }} as p
    on  p.patient_id = e.patient_id
    and e.started_at >= p.valid_from
    and (p.valid_to is null or e.started_at < p.valid_to)
left join {{ ref('int_inpatient_encounters') }} as i
    on i.encounter_id = e.encounter_id
