with

    t1 as (select * from {{ ref("int_global") }}),
    t2 as (select * from {{ ref("stg_session") }}),
    t3 as (
        select *
        from t1
        full outer join t2 on t1.batch_id = t2.session_batch_id
        order by batch_id, session_id
    ),
    t4 as (
        select t3.*,(
                case
                    when t3.session_mode = 'Any' and t3.school_district = 'RJ Model B'
                    then 'HW Session'
                    else t3.session_type
                end
            ) updated_session_type,
            (
                case
                    when
                        lower(session_name) like '%endline%'
                        and total_student_present is not null
                        and session_date is not null
                    then "Batch Completed"  -- above queries are processed first so baseline and endline do not clash
                    when
                        lower(session_name) like '%01.%'
                        and total_student_present is not null
                        and session_date is not null
                    then "Batch Started"
                end
            ) batch_status
        from t3
    )

    select
    batch_id, batch_no, batch_academic_year, batch_grade, batch_language,
    no_of_students_facilitated, fac_start_date, fac_end_date,
    allocation_email_sent, batch_facilitator_id, facilitator_id, facilitator_name, facilitator_email,
    batch_session_type_based_avg_overall_attendance,
    batch_max_overall_attendance,
    total_reached_parents, school_district, school_state, session_no, total_student_present,
    session_type as previous_session_type,
    updated_session_type as session_type,
    from t4