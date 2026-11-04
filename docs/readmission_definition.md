# 30-day readmission: definition

This is the business rule behind `fact_hospital_stays.is_readmitted_within_window` and
`mart_patient_readmissions`. It is modelled on the general approach of the CMS hospital-wide
readmission measure and the NHS "emergency readmissions within 30 days" indicator, but it
is a simplified portfolio definition, not an implementation of either.

Parameters live in `dbt/dbt_project.yml` (`vars`), so they can be changed in one place.

## 1. What is a hospital stay?

A stay is one inpatient episode, from admission to final discharge.

Synthea records a transfer (ward → ICU, ICU → ward) as a separate inpatient encounter that
overlaps or immediately follows the previous one. **An inpatient encounter that starts no
more than `transfer_gap_hours` (1 hour) after the latest discharge so far belongs to the
same stay.** Without this rule every transfer would count as a readmission.
Implemented in `int_inpatient_encounters`; checked by `assert_hospital_stays_do_not_overlap`.

## 2. Index stays (the denominator)

A stay is an **index stay** unless:

| Exclusion | Reason |
| --- | --- |
| `died_in_hospital` | The patient's death date falls within the stay: they cannot be readmitted |
| `under_minimum_age` | Under `min_index_age_years` (18) at admission: paediatric care is measured separately |
| `follow_up_window_incomplete` | Discharged less than 30 days before the end of the data: we cannot yet know if they returned |

Planned stays **can** be index stays. A patient discharged after a planned operation who
comes back as an emergency is a readmission.

## 3. Readmissions (the numerator)

An index stay is **readmitted** if the same patient has another stay that:

- starts after the index discharge and **no more than `readmission_window_days` (30) days later**, and
- is **unplanned**.

A stay is planned if its admission type or its admission reason is marked planned in the seeds:

- `seeds/admission_types.csv`: transplant and rehabilitation admissions (always planned in the
  CMS approach), elective orthopaedic admissions, scheduled follow-ups.
- `seeds/admission_reasons.csv`: scheduled cancer treatment (chemotherapy cycles), elective
  sterilisation, sleep studies.

**Why the reason matters:** Synthea admits cancer patients every 3–4 weeks for treatment using
the generic code "Encounter for problem". In the first test with 1,000 patients, 18 of the 19
apparent readmissions were chemotherapy cycles for four patients, giving a misleading 35%
readmission rate. Marking those reasons as planned brought it to 1 of 54.

## 4. Counting

- An index stay counts **once**, however many readmissions follow it.
- A readmission can also be an index stay for a later readmission.
- Stays are reported in the **month of discharge** of the index stay.
- The rate is `readmissions / index stays`. Across rows, **sum the numerator and denominator
  first**, then divide. Averaging monthly or departmental rates gives the wrong answer.

## 5. Known limitations

- No discharge disposition in Synthea, so "discharged against medical advice" and
  "transferred to another hospital" cannot be excluded.
- Planned status is inferred from codes, not from a scheduling system.
- Synthea produces few unplanned inpatient stays (about 13 a year per 1,000 patients),
  so rates are unstable at small scale. Use 10,000+ patients for meaningful monthly figures.
- Deaths after discharge do not remove a stay from the denominator (as in the CMS measure).
