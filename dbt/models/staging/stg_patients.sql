-- One row per patient: the current version.
select *
from {{ ref('stg_patient_versions') }}
where is_current
