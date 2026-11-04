-- One row per encounter: the latest version received, with proper types.
-- Corrections arrive as new raw rows; the newest source_updated_at wins, then the
-- latest delivery, then the last row in the file.

with versions as (

    select
        *,
        row_number() over (
            partition by id
            order by source_updated_at::timestamptz desc, _available_date desc, _row_number desc
        ) as version_rank,
        count(*) over (partition by id) as version_count,
        min(_available_date) over (partition by id) as first_available_date,
        max(_ingested_at) over (partition by id) as last_ingested_at
    from {{ source('raw', 'encounters') }}

)

select
    id                                              as encounter_id,
    patient                                         as patient_id,
    organization                                    as organization_id,
    provider                                        as provider_id,
    payer                                           as payer_id,
    lower(encounterclass)                           as encounter_class,
    code                                            as encounter_code,
    description                                     as encounter_description,
    reasoncode                                      as reason_code,
    reasondescription                               as reason_description,

    start::timestamptz                              as started_at,
    stop::timestamptz                               as ended_at,
    -- Dates in UTC, so results never depend on the database session's time zone
    (start::timestamptz at time zone 'UTC')::date   as start_date,
    (stop::timestamptz at time zone 'UTC')::date    as end_date,
    round(extract(epoch from stop::timestamptz - start::timestamptz) / 3600.0, 2)
                                                    as duration_hours,

    base_encounter_cost::numeric(12, 2)             as base_encounter_cost,
    total_claim_cost::numeric(12, 2)                as total_claim_cost,
    payer_coverage::numeric(12, 2)                  as payer_coverage,

    source_updated_at::timestamptz                  as source_updated_at,
    first_available_date,
    _available_date                                 as latest_available_date,
    version_count,
    last_ingested_at
from versions
where version_rank = 1
