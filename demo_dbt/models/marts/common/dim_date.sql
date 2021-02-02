{{
    config(
        materialized = 'ephemeral'
    )
}}
with dates as (

    -- we arbitrarily start on 1/1/2016 and end 53 weeks from now:
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2016', 'mm/dd/yyyy')",
        end_date="dateadd(week, 53, current_date)"
       )
    }}

)
select
    d.calendar_date,
    date_trunc('week', d.calendar_date)::date as week_start_date,
    case
        when day_of_week = 7 then d.calendar_date
        else dateadd('day', -1, week_start_date)
    end as week_start_date_sun,
    dateadd('day', 6, date_trunc('week', d.calendar_date))::date as week_end_date,
    dateadd('day', 6, week_start_date_sun) as week_end_date_sat,
    date_part('month', d.calendar_date)::int as month_of_year,
    date_trunc('month', d.calendar_date)::date as month_start_date,
    {{ dbt_utils.last_day('d.calendar_date', 'month') }} as month_end_date,
    date_part('year', d.calendar_date)::int as year_number
from
    dates d
order by 1