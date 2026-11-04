-- Every distinct version of every patient, with the period it was valid for.
-- This is the history the source itself sent (for example "alive" then "deceased"),
-- so it does not depend on how often dbt runs. dim_patient is built from it (SCD type 2).

with deduplicated as (

    -- The same version can be delivered twice; keep one row per (patient, version time).
    select distinct on (id, source_updated_at::timestamptz)
        id,
        birthdate,
        deathdate,
        marital,
        race,
        ethnicity,
        gender,
        county,
        state,
        healthcare_expenses,
        healthcare_coverage,
        income,
        source_updated_at::timestamptz as source_updated_at,
        _available_date,
        _ingested_at
    from {{ source('raw', 'patients') }}
    order by id, source_updated_at::timestamptz, _available_date desc, _row_number desc

)

select
    id                                          as patient_id,
    birthdate::date                             as birth_date,
    nullif(deathdate, '')::date                 as death_date,
    case upper(gender) when 'M' then 'male' when 'F' then 'female' else 'unknown' end
                                                as gender,
    coalesce(lower(race), 'unknown')            as race,
    coalesce(lower(ethnicity), 'unknown')       as ethnicity,
    case upper(marital)
        when 'M' then 'married' when 'S' then 'single' when 'D' then 'divorced'
        when 'W' then 'widowed' else 'unknown'
    end                                         as marital_status,
    county,
    state,
    healthcare_expenses::numeric(14, 2)         as healthcare_expenses,
    healthcare_coverage::numeric(14, 2)         as healthcare_coverage,
    income::numeric(14, 2)                      as income,

    source_updated_at,
    -- The first version is treated as valid from the beginning of time: the patient
    -- existed before they were registered with this hospital.
    case
        when lag(source_updated_at) over w is null then '1900-01-01 00:00:00+00'::timestamptz
        else source_updated_at
    end                                         as valid_from,
    lead(source_updated_at) over w              as valid_to,
    lead(source_updated_at) over w is null      as is_current,
    row_number() over w                         as version_number,
    _available_date                             as available_date,
    _ingested_at                                as ingested_at
from deduplicated
window w as (partition by id order by source_updated_at)
