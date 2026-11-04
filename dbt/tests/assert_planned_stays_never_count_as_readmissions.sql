-- Business rule: a planned admission (transplant, rehabilitation, elective) is never a readmission.
select r.episode_id, r.readmission_episode_id
from {{ ref('int_readmissions') }} as r
inner join {{ ref('int_inpatient_episodes') }} as readmission
    on readmission.episode_id = r.readmission_episode_id
where readmission.is_planned
