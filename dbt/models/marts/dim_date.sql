-- Calendar dimension covering all activity plus one year ahead.
with bounds as (

    select
        date_trunc('year', min(start_date))::date                               as first_day,
        (date_trunc('year', max(coalesce(end_date, start_date))) + interval '2 years - 1 day')::date
                                                                                as last_day
    from {{ ref('stg_encounters') }}

)

select
    to_char(d, 'YYYYMMDD')::int                 as date_key,
    d::date                                     as calendar_date,
    extract(isoyear from d)::int                as iso_year,
    extract(week from d)::int                   as iso_week,
    extract(year from d)::int                   as year,
    extract(quarter from d)::int                as quarter,
    extract(month from d)::int                  as month,
    to_char(d, 'FMMonth')                       as month_name,
    to_char(d, 'YYYY-MM')                       as year_month,
    date_trunc('month', d)::date                as month_start,
    extract(isodow from d)::int                 as day_of_week,  -- 1 = Monday
    to_char(d, 'FMDay')                         as day_name,
    extract(isodow from d) in (6, 7)            as is_weekend
from bounds,
    generate_series(bounds.first_day, bounds.last_day, interval '1 day') as d
