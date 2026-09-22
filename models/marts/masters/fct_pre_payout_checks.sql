with

    source as (
        select *
        from {{ ref("fct_global_session") }}
        where session_date is not null
    ),

    holiday_calendar_raw as (

        -- Rajasthan: Udaipur + Dungarpur
        select 'Rajasthan' as geography, holiday_date
        from
            unnest(
                [
                    date '2026-04-03',
                    date '2026-04-11',
                    date '2026-04-14',
                    date '2026-04-19',
                    date '2026-05-28',
                    date '2026-06-17',
                    date '2026-06-26',
                    date '2026-08-09',
                    date '2026-08-15',
                    date '2026-08-26',
                    date '2026-08-28',
                    date '2026-09-04',
                    date '2026-08-30',
                    date '2026-10-02',
                    date '2026-10-11',
                    date '2026-10-19',
                    date '2026-10-20',
                    date '2026-11-24',
                    date '2026-12-25',
                    date '2027-01-26',
                    date '2027-03-06',
                    date '2027-03-10',
                    date '2027-03-21',
                    date '2027-03-22',
                    date '2027-03-26'
                ]
            ) as holiday_date

        union all

        select 'Rajasthan', holiday_date
        from
            unnest(
                generate_date_array(date '2026-05-15', date '2026-06-20')
            ) as holiday_date

        union all

        select 'Rajasthan', holiday_date
        from
            unnest(
                generate_date_array(date '2026-11-04', date '2026-11-14')
            ) as holiday_date

        union all

        select 'Rajasthan', holiday_date
        from
            unnest(
                generate_date_array(date '2026-12-26', date '2027-01-05')
            ) as holiday_date

        -- Maharashtra
        union all

        select 'Maharashtra', holiday_date
        from
            unnest(
                [
                    date '2026-07-25',
                    date '2026-08-15',
                    date '2026-08-26',
                    date '2026-08-28',
                    date '2026-09-04',
                    date '2026-09-05',
                    date '2026-09-25',
                    date '2026-10-02',
                    date '2026-10-11',
                    date '2026-10-20',
                    date '2026-11-24',
                    date '2026-12-04',
                    date '2026-12-25',
                    date '2027-01-01',
                    date '2027-01-15',
                    date '2027-01-26',
                    date '2027-02-19',
                    date '2027-03-06',
                    date '2027-03-10',
                    date '2027-03-22',
                    date '2027-03-26'
                ]
            ) as holiday_date

        union all

        select 'Maharashtra', holiday_date
        from
            unnest(
                generate_date_array(date '2026-09-14', date '2026-09-19')
            ) as holiday_date

        union all

        select 'Maharashtra', holiday_date
        from
            unnest(
                generate_date_array(date '2026-11-06', date '2026-11-18')
            ) as holiday_date

        union all

        select 'Maharashtra', holiday_date
        from
            unnest(
                generate_date_array(date '2026-12-25', date '2027-01-04')
            ) as holiday_date

        -- Haryana
        union all

        select 'Haryana', holiday_date
        from
            unnest(
                [
                    date '2026-08-15',
                    date '2026-08-28',
                    date '2026-09-04',
                    date '2026-09-23',
                    date '2026-10-02',
                    date '2026-10-20',
                    date '2026-10-26',
                    date '2026-11-24',
                    date '2026-12-25',
                    date '2027-01-26',
                    date '2027-02-26'
                ]
            ) as holiday_date

        union all

        select 'Haryana', holiday_date
        from
            unnest(
                generate_date_array(date '2026-11-07', date '2026-11-13')
            ) as holiday_date

        union all

        select 'Haryana', holiday_date
        from
            unnest(
                generate_date_array(date '2027-01-01', date '2027-01-17')
            ) as holiday_date

        -- Goa
        union all

        select 'Goa', holiday_date
        from
            unnest(
                [
                    date '2026-08-15',
                    date '2026-08-26',
                    date '2026-10-02',
                    date '2026-10-20',
                    date '2026-12-03',
                    date '2026-12-19',
                    date '2027-01-26',
                    date '2027-03-10',
                    date '2027-03-22'
                ]
            ) as holiday_date

        union all

        select 'Goa', holiday_date
        from
            unnest(
                generate_date_array(date '2026-09-14', date '2026-09-19')
            ) as holiday_date

        union all

        select 'Goa', holiday_date
        from
            unnest(
                generate_date_array(date '2026-11-02', date '2026-11-21')
            ) as holiday_date

        union all

        select 'Goa', holiday_date
        from
            unnest(
                generate_date_array(date '2026-12-24', date '2027-01-02')
            ) as holiday_date

        -- Nagaland
        union all

        select 'Nagaland', holiday_date
        from
            unnest(
                [
                    date '2026-01-01',
                    date '2026-01-26',
                    date '2026-03-04',
                    date '2026-03-21',
                    date '2026-05-28',
                    date '2026-08-15',
                    date '2026-08-26',
                    date '2026-09-04',
                    date '2026-10-02',
                    date '2026-11-08',
                    date '2026-11-24',
                    date '2026-12-01'
                ]
            ) as holiday_date

        union all

        select 'Nagaland', holiday_date
        from
            unnest(
                generate_date_array(date '2026-04-03', date '2026-04-05')
            ) as holiday_date

        union all

        select 'Nagaland', holiday_date
        from
            unnest(
                generate_date_array(date '2026-10-19', date '2026-10-21')
            ) as holiday_date

        union all

        select 'Nagaland', holiday_date
        from
            unnest(
                generate_date_array(date '2026-12-22', date '2026-12-31')
            ) as holiday_date

    ),

    holiday_calendar as (

        select distinct geography, holiday_date from holiday_calendar_raw

    ),

    school_timing_calculations as (

        select
            source.*,

            /*
        1 = session is within geography-wise threshold working hours
        0 = session is outside geography-wise threshold working hours
        */
            case
                when session_start_time is null
                then null

                -- Nagaland: 08:00 AM - 03:30 PM
                when
                    school_state = 'Nagaland'
                    and safe.parse_time(
                        '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                    )
                    between time '08:00:00' and time '15:30:00'
                then 1

                -- Udaipur / Dungarpur: 07:30 AM - 04:00 PM
                when
                    school_district in ('Udaipur', 'Dungarpur')
                    and safe.parse_time(
                        '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                    )
                    between time '07:30:00' and time '16:00:00'
                then 1

                -- Goa: 08:00 AM - 01:45 PM
                when
                    school_state = 'Goa'
                    and safe.parse_time(
                        '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                    )
                    between time '08:00:00' and time '13:45:00'
                then 1

                -- Yamunanagar: 08:00 AM - 03:30 PM
                when
                    school_district = 'Yamunanagar'
                    and safe.parse_time(
                        '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                    )
                    between time '08:00:00' and time '15:30:00'
                then 1

                -- Mumbai / Thane / Pune / Dharashiv / Osmanabad:
                -- 07:00 AM - 06:00 PM
                when
                    school_district
                    in ('Mumbai', 'Thane', 'Pune', 'Dharashiv', 'Osmanabad')
                    and safe.parse_time(
                        '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                    )
                    between time '07:00:00' and time '18:00:00'
                then 1

                else 0
            end as school_timing,

            /*
        1 = session is outside working hours
        0 = session is within working hours
        */
            case
                when session_start_time is null
                then null

                when
                    school_state = 'Nagaland'
                    and (
                        safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        < time '08:00:00'
                        or safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        > time '15:30:00'
                    )
                then 1

                when
                    school_district in ('Udaipur', 'Dungarpur')
                    and (
                        safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        < time '07:30:00'
                        or safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        > time '16:00:00'
                    )
                then 1

                when
                    school_state = 'Goa'
                    and (
                        safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        < time '08:00:00'
                        or safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        > time '13:45:00'
                    )
                then 1

                when
                    school_district = 'Yamunanagar'
                    and (
                        safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        < time '08:00:00'
                        or safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        > time '15:30:00'
                    )
                then 1

                when
                    school_district
                    in ('Mumbai', 'Thane', 'Pune', 'Dharashiv', 'Osmanabad')
                    and (
                        safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        < time '07:00:00'
                        or safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        )
                        > time '18:00:00'
                    )
                then 1

                else 0
            end as beyond_working_hours_flag,

            /*
        School working status

        1 = Holiday
        0 = School Day
        NULL = session date is NULL

        Holiday is determined from the geography-wise
        holiday_calendar created above.
        */
            case
                when source.session_date is null
                then null
                when h.holiday_date is not null
                then 0
                else 1
            end as school_working_status

        from source

        left join
            holiday_calendar h
            on h.holiday_date = safe_cast(source.session_date as date)
            and (
                (
                    h.geography = 'Rajasthan'
                    and source.school_district in ('Udaipur', 'Dungarpur')
                )
                or (
                    h.geography = 'Maharashtra'
                    and source.school_district
                    in ('Mumbai', 'Pune', 'Thane', 'Dharashiv', 'Osmanabad')
                )
                or (
                    h.geography = source.school_state
                    and source.school_state in ('Haryana', 'Goa', 'Nagaland')
                )
            )

    ),

    /*
Unique session-level data for calculating the
same-day facilitator session counter.

This prevents multiple source rows belonging to
the same session_id from increasing the counter.
*/
    session_counter_base as (

    select
        session_id,
        any_value(facilitator_email) as facilitator_email,
        safe_cast(session_date as date) as session_date,
        any_value(session_start_time) as session_start_time

    from school_timing_calculations

    group by session_id, session_date

),

    session_counter_calculations as (

    select
        session_id,

        case
            when session_date is null then null
            else row_number() over (
                partition by facilitator_email, session_date
                order by
                    safe.parse_time(
                        '%H:%M:%E*S',
                        regexp_replace(session_start_time, r'Z$', '')
                    ),
                    session_id
            )
        end as session_counter,

        count(*) over (
            partition by facilitator_email, session_date
        ) as daily_session_count

    from session_counter_base

),

    session_calculations as (

        select
            st.*,

            format_date(
                '%B %Y', safe_cast(st.session_date as date)
            ) as session_month_year,

            format_date('%A', safe_cast(st.session_date as date)) as session_day,

            format_time(
                '%I:%M %p',
                safe.parse_time(
                    '%H:%M:%E*S', regexp_replace(st.session_start_time, r'Z$', '')
                )
            ) as session_time,

            /*
        Previous session for the same PSO
        on the same date.
        */
            lag(
                safe.parse_time(
                    '%H:%M:%E*S', regexp_replace(st.session_start_time, r'Z$', '')
                )
            ) over (
                partition by st.facilitator_email, safe_cast(st.session_date as date)
                order by
                    safe.parse_time(
                        '%H:%M:%E*S', regexp_replace(st.session_start_time, r'Z$', '')
                    ),
                    st.session_id
            ) as previous_session_time,

            sc.session_counter,

            sc.daily_session_count

        from school_timing_calculations st

        left join session_counter_calculations sc
    on st.session_id = sc.session_id

    ),

    attendance_calculations as (

        select
            * except (session_status),

            case
                when session_type = 'Parent'
                then total_parent_present
                else total_student_present
            end as total_attendance,

            case
                when session_type = 'Parent'
                then parent_present_count
                else present_count
            end as individual_attendance,

            case
                when session_type = 'Student'
                then total_student_present - present_count

                when session_type = 'Parent'
                then total_parent_present - parent_present_count

                when session_type = 'Counseling'
                then total_student_present - present_count

                when session_type = 'Flexible'
                then total_student_present - present_count

                else null
            end as gap_in_attendance,

           case
    when session_date is not null
         and (
             total_student_present is not null
             or total_parent_present is not null
         )
        then 1

    when session_date is not null
         and total_student_present is null
         and total_parent_present is null
        then 0

    else null
end as session_status,
case
    when session_date is not null
         and (
             total_student_present is not null
             or total_parent_present is not null
         )
        then 1
    else 0
end as session_completed_flag

        from session_calculations

    ),

    attendance_tat_calculations as (

        select
            *,

            /*
        Attendance TAT:

        TAT is calculated only when:
        - Total Attendance > 0
        - Individual Attendance = 0

        TAT = number of days from the session date to today.
        */
            case
                when
                    total_attendance > 0
                    and individual_attendance = 0
                    and session_date is not null
                then date_diff(current_date(), safe_cast(session_date as date), day)
                else null
            end as attendance_tat, --Attendace daal diya hai tho kya hoga we dont have that tat

            case
                when
                    total_attendance > 0
                    and individual_attendance = 0
                    and session_date is not null
                    and date_diff(current_date(), safe_cast(session_date as date), day)
                    >= 7
                then 1
                else 0
            end as attendance_tat_7_days_flag

        from attendance_calculations

    ),

    gap_calculations as (

        select
            *,

            /*
        Difference between consecutive sessions
        for the same PSO on the same date.
        */
            case
                when previous_session_time is not null
                then
                    time_diff(
                        safe.parse_time(
                            '%H:%M:%E*S', regexp_replace(session_start_time, r'Z$', '')
                        ),
                        previous_session_time,
                        minute
                    )
                else null
            end as gap_minutes

        from attendance_tat_calculations

    ),

    final_calculations as (

        select
            *,

            /*
        Gap displayed as HH:MM
        */
            case
                when gap_minutes is not null
                then
                    format(
                        '%02d:%02d',
                        cast(floor(gap_minutes / 60) as int64),
                        mod(gap_minutes, 60)
                    )
                else null
            end as gap_between_sessions,

            /*
        Session gap label
        */
            case
                when gap_minutes = 0
                then 'Same day Same time'

                when gap_minutes > 0 and gap_minutes < 45
                then 'less than 45 mins'

                when gap_minutes >= 45 and gap_minutes <= 60
                then '45-60 mins'

                when gap_minutes > 60
                then 'more than 60 mins'

                else null
            end as session_gap_label,

            /*
        Control label
        */
            case
                when gap_minutes = 0
                then 'Same day Same time'

                when gap_minutes > 0 and gap_minutes < 45
                then '<45 mins'

                when gap_minutes >= 45 and gap_minutes <= 60
                then '<45-60 mins'

                when gap_minutes > 60
                then '>60 mins'

                when daily_session_count >= 4
                then 'Session counter greater than 4'

                when beyond_working_hours_flag = 1
                then 'Session beyond working hour'

                when session_completed_flag = 0
                then 'Session not completed'

                when total_attendance > 0 and coalesce(individual_attendance, 0) = 0
                then 'IA missing'

                else null
            end as control_label --concatenate blank ke saath 45 min hai tho that, its basically , , or, Blank ko NULL

        from gap_calculations

    ),

    final as (

        select

            school_state,

            school_district,

            school_taluka,
            school_ward,

            batch_donor,

            school_partner,

            batch_language,

            batch_academic_year,

            school_name,

            batch_no,

            batch_grade,

            facilitator_name,

            facilitator_email,

            session_name,

            session_date,

            session_month_year,

            session_day,

            session_time,

            school_timing,

            session_status,

            school_working_status,

            gap_between_sessions,

            session_gap_label,

            session_counter,

            total_attendance,

            individual_attendance,

            gap_in_attendance,

            attendance_tat,

            control_label

        from final_calculations
        --where daily_session_count >= 4
  where session_status is not null
        order by facilitator_name, session_date

        /*
    Show all sessions for days where
    the facilitator conducted >= 4 sessions.
    */

    )

select * from final
