-- Hospital departments (synthetic reference data, see seeds/departments.csv).
select
    department_code     as department_key,
    department_name,
    service_line,
    is_critical_care
from {{ ref('departments') }}
