-- Patient dimension, slowly changing (type 2): one row per version of each patient.
-- Facts join on patient_key, the version valid when the event happened, so a report on
-- last year's admissions shows patients as they were then.
select
    md5(patient_id || '|' || version_number::text)  as patient_key,
    patient_id,
    version_number,
    valid_from,
    valid_to,
    is_current,
    birth_date,
    death_date,
    death_date is not null                          as is_deceased,
    gender,
    race,
    ethnicity,
    marital_status,
    county,
    state,
    income
from {{ ref('stg_patient_versions') }}
