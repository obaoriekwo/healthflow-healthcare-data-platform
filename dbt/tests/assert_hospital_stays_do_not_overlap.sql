-- A patient cannot be in two hospital stays at once. If this fails, the transfer-merging
-- rule in int_inpatient_encounters is not working.
select a.episode_id, b.episode_id as overlapping_episode_id
from {{ ref('int_inpatient_episodes') }} as a
inner join {{ ref('int_inpatient_episodes') }} as b
    on  b.patient_id = a.patient_id
    and b.episode_id <> a.episode_id
    and b.admitted_at >= a.admitted_at
    and b.admitted_at < a.discharged_at
