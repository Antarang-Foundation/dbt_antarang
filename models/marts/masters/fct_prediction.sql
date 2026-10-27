with

    holiday_calendar_raw as (

        -- Rajasthan: Udaipur and Dungarpur

        select 'Rajasthan' as geography, holiday_date

        from unnest([

            date '2026-04-03', date '2026-04-11',

            date '2026-04-14', date '2026-04-19',

            date '2026-05-28', date '2026-06-17',

            date '2026-06-26', date '2026-08-09',

            date '2026-08-15', date '2026-08-26',

            date '2026-08-28', date '2026-09-04',

            date '2026-08-30', date '2026-10-02',

            date '2026-10-11', date '2026-10-19',

            date '2026-10-20', date '2026-11-24',

            date '2026-12-25', date '2027-01-26',

            date '2027-03-06', date '2027-03-10',

            date '2027-03-21', date '2027-03-22',

            date '2027-03-26'

        ]) as holiday_date

        union all

        select 'Rajasthan', holiday_date

        from unnest(generate_date_array(

            date '2026-05-15', date '2026-06-20'

        )) as holiday_date

        union all

        select 'Rajasthan', holiday_date

        from unnest(generate_date_array(

            date '2026-11-04', date '2026-11-14'

        )) as holiday_date

        union all

        select 'Rajasthan', holiday_date

        from unnest(generate_date_array(

            date '2026-12-26', date '2027-01-05'

        )) as holiday_date

        -- Maharashtra

        union all

        select 'Maharashtra', holiday_date

        from unnest([

            date '2026-07-25', date '2026-08-15',

            date '2026-08-26', date '2026-08-28',

            date '2026-09-04', date '2026-09-05',

            date '2026-09-25', date '2026-10-02',

            date '2026-10-11', date '2026-10-20',

            date '2026-11-24', date '2026-12-04',

            date '2026-12-25', date '2027-01-01',

            date '2027-01-15', date '2027-01-26',

            date '2027-02-19', date '2027-03-06',

            date '2027-03-10', date '2027-03-22',

            date '2027-03-26'

        ]) as holiday_date

        union all

        select 'Maharashtra', holiday_date

        from unnest(generate_date_array(

            date '2026-09-14', date '2026-09-19'

        )) as holiday_date

        union all

        select 'Maharashtra', holiday_date

        from unnest(generate_date_array(

            date '2026-11-06', date '2026-11-18'

        )) as holiday_date

        union all

        select 'Maharashtra', holiday_date

        from unnest(generate_date_array(

            date '2026-12-25', date '2027-01-04'

        )) as holiday_date

        -- Haryana

        union all

        select 'Haryana', holiday_date

        from unnest([

            date '2026-08-15', date '2026-08-28',

            date '2026-09-04', date '2026-09-23',

            date '2026-10-02', date '2026-10-20',

            date '2026-10-26', date '2026-11-24',

            date '2026-12-25', date '2027-01-26',

            date '2027-02-26'

        ]) as holiday_date

        union all

        select 'Haryana', holiday_date

        from unnest(generate_date_array(

            date '2026-11-07', date '2026-11-13'

        )) as holiday_date

        union all

        select 'Haryana', holiday_date

        from unnest(generate_date_array(

            date '2027-01-01', date '2027-01-17'

        )) as holiday_date

        -- Goa

        union all

        select 'Goa', holiday_date

        from unnest([

            date '2026-08-15', date '2026-08-26',

            date '2026-10-02', date '2026-10-20',

            date '2026-12-03', date '2026-12-19',

            date '2027-01-26', date '2027-03-10',

            date '2027-03-22'

        ]) as holiday_date

        union all

        select 'Goa', holiday_date

        from unnest(generate_date_array(

            date '2026-09-14', date '2026-09-19'

        )) as holiday_date

        union all

        select 'Goa', holiday_date

        from unnest(generate_date_array(

            date '2026-11-02', date '2026-11-21'

        )) as holiday_date

        union all

        select 'Goa', holiday_date

        from unnest(generate_date_array(

            date '2026-12-24', date '2027-01-02'

        )) as holiday_date

        -- Nagaland

        union all

        select 'Nagaland', holiday_date

        from unnest([

            date '2026-01-01', date '2026-01-26',

            date '2026-03-04', date '2026-03-21',

            date '2026-05-28', date '2026-08-15',

            date '2026-08-26', date '2026-09-04',

            date '2026-10-02', date '2026-11-08',

            date '2026-11-24', date '2026-12-01'

        ]) as holiday_date

        union all

        select 'Nagaland', holiday_date

        from unnest(generate_date_array(

            date '2026-04-03', date '2026-04-05'

        )) as holiday_date

        union all

        select 'Nagaland', holiday_date

        from unnest(generate_date_array(

            date '2026-10-19', date '2026-10-21'

        )) as holiday_date

        union all

        select 'Nagaland', holiday_date

        from unnest(generate_date_array(

            date '2026-12-22', date '2026-12-31'

        )) as holiday_date

    ),

    holiday_calendar as (

        select distinct geography, holiday_date

        from holiday_calendar_raw

    ),

    base as (

        select

            facilitator_name,

            facilitator_email,

            school_name,

            batch_academic_year,

            batch_no,

            batch_grade,

            batch_donor,

            fac_start_date,

            fac_end_date,

            session_date,

            session_name,

            session_code,

            school_state,

            school_district,

            school_partner,

            school_taluka,

            school_ward,

            school_area,

            total_student_present,

            batch_expected_sessions,

            case

                when session_date is not null

                    and total_student_present is not null

                then session_code

            end as session_completion

        from {{ ref("int_global_session") }}
        where session_type = 'Student'

    ),

    summary as (

        select

            facilitator_name,

            facilitator_email,

            school_name,

            batch_academic_year,

            batch_no,

            batch_grade,

            fac_start_date,

            fac_end_date,

            school_state,

            school_district,

            school_taluka,

            school_ward,

            school_area,

            school_partner,

            batch_donor,

            count(distinct session_code) as total_sessions,
            count(distinct session_completion) as completed_sessions,
            max(safe_cast(batch_expected_sessions as int64)) as batch_expected_sessions

        from base

        group by

            facilitator_name,

            facilitator_email,

            school_name,

            batch_academic_year,

            batch_no,

            batch_grade,

            fac_start_date,

            fac_end_date,

            school_state,

            school_district,

            school_taluka,

            school_ward,

            school_area,

            school_partner,

            batch_donor

    ),

    

tat_base AS (
    SELECT
        *,
        total_sessions - completed_sessions AS pending_sessions,

        COALESCE(
            SAFE_CAST(fac_start_date AS DATE),
            CURRENT_DATE("Asia/Kolkata")
        ) AS tat_start_date

    FROM summary
),

final AS (
    SELECT
        *,

        -- 5 working days per pending session
        (
            SELECT ARRAY_AGG(d ORDER BY d)
                [SAFE_OFFSET(pending_sessions * 5 - 1)]
            FROM UNNEST(
                GENERATE_DATE_ARRAY(
                    DATE_ADD(tat_start_date, INTERVAL 1 DAY),
                    DATE_ADD(
                        tat_start_date,
                        INTERVAL (pending_sessions * 10 + 14) DAY
                    )
                )
            ) AS d
            WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)
        ) AS five_days_tat,

        -- 15 working days per pending session
        (
            SELECT ARRAY_AGG(d ORDER BY d)
                [SAFE_OFFSET(pending_sessions * 15 - 1)]
            FROM UNNEST(
                GENERATE_DATE_ARRAY(
                    DATE_ADD(tat_start_date, INTERVAL 1 DAY),
                    DATE_ADD(
                        tat_start_date,
                        INTERVAL (pending_sessions * 30 + 14) DAY
                    )
                )
            ) AS d
            WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)
        ) AS fifteen_days_tat

    FROM tat_base
),

prediction_dates AS (

    SELECT

        *,

        five_days_tat AS predicted_date_tat

    FROM final

),

prediction AS (

        select

            *,

            concat(

                case

                    when extract(month from predicted_date_tat) = 1 then 'Jan'

                    when extract(month from predicted_date_tat) = 2 then 'Feb'

                    when extract(month from predicted_date_tat) = 3 then 'Mar'

                    when extract(month from predicted_date_tat) = 4 then 'Apr'

                    when extract(month from predicted_date_tat) = 5 then 'May'

                    when extract(month from predicted_date_tat) = 6 then 'Jun'

                    when extract(month from predicted_date_tat) = 7 then 'Jul'

                    when extract(month from predicted_date_tat) = 8 then 'Aug'

                    when extract(month from predicted_date_tat) = 9 then 'Sep'

                    when extract(month from predicted_date_tat) = 10 then 'Oct'

                    when extract(month from predicted_date_tat) = 11 then 'Nov'

                    else 'Dec'

                end,

                ' Week ',

                cast(

                    floor(

                        (extract(day from predicted_date_tat) - 1) / 7

                    ) + 1 as string

                )

            ) as predicted_days_tat,

            concat(

                format_date('%a', predicted_date_tat),

                ', ',

                format_date('%b', predicted_date_tat),

                ' Week ',

                cast(

                    floor(

                        (extract(day from predicted_date_tat) - 1) / 7

                    ) + 1 as string

                )

            ) as formula_prediction_week

        from prediction_dates

        where batch_academic_year is not null

    ),

    t1 as (

        select

            p.school_state,

            p.school_district,

            p.school_taluka,

            p.school_ward,

            p.school_area,

            p.school_partner,

            p.school_name,

            p.batch_no,

            p.batch_grade,

            p.facilitator_name,

            p.facilitator_email,

            p.batch_donor,

            p.batch_academic_year,

            p.fac_start_date,

            p.fac_end_date,

            p.total_sessions,

            p.completed_sessions,

            p.pending_sessions,

            p.predicted_date_tat,

            p.five_days_tat,
            p.fifteen_days_tat,

            -- School working status for predicted date:

            -- 1 = School Day

            -- 0 = Sunday or applicable holiday

            -- NULL = Predicted date is NULL

            case

                when p.predicted_date_tat is null then null

                when extract(

                    dayofweek from p.predicted_date_tat

                ) = 1 then 0

                when exists (

                    select 1

                    from holiday_calendar h

                    where h.holiday_date = p.predicted_date_tat

                      and (

                          (

                              h.geography = 'Rajasthan'

                              and p.school_district in (

                                  'Udaipur', 'Dungarpur'

                              )

                          )

                          or (

                              h.geography = 'Maharashtra'

                              and p.school_district in (

                                  'Mumbai', 'Pune', 'Thane',

                                  'Dharashiv', 'Osmanabad'

                              )

                          )

                          or (

                              h.geography = p.school_state

                              and p.school_state in (

                                  'Haryana', 'Goa', 'Nagaland'

                              )

                          )

                      )

                ) then 0

                else 1

            end as school_working_status,

            p.formula_prediction_week as predicted_days_tat

        from prediction p

    )

select *

from t1