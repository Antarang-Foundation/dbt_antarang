with
    t1 as (select batch_id, student_barcode, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated,
    fac_start_date, fac_end_date, allocation_email_sent, batch_facilitator_id, facilitator_id, facilitator_name, 
    facilitator_email, batch_school_id, school_id, school_name, school_taluka, school_district, school_state,
    school_academic_year, school_language, enrolled_g9, enrolled_g10, enrolled_g11, enrolled_g12,
    tagged_for_counselling, school_partner, school_area, batch_donor_id, donor_id, batch_donor, student_name, first_barcode, gender,
    current_barcode, current_batch_no, birth_year, birth_date, g9_whatsapp_no, g10_whatsapp_no, g11_whatsapp_no, g12_whatsapp_no, religion, caste, father_education, father_occupation, mother_education, mother_occupation,
    student_details_2_submitted, school_ward  from {{ ref("dev_int_global_dcp") }}),

    t2 as (select session_id, session_batch_id, session_facilitator_id, session_code, session_name, session_type,
    session_date, session_start_time, session_grade, session_no, omr_required, omrs_received, total_student_present,
    total_parent_present, log_reason, attendance_submitted, present_count, attendance_count, payment_status,
    parent_present_count, session_mode, batch_expected_sessions, batch_expected_student_type_session, batch_expected_parent_type_session, 
    batch_expected_flexible_type_session, batch_expected_counseling_type_session, batch_scheduled_sessions, 
    batch_student_type_scheduled_sessions, batch_parent_type_scheduled_sessions, batch_flexible_type_scheduled_sessions, 
    batch_counseling_type_scheduled_sessions, batch_completed_sessions, batch_student_type_completed_sessions, 
    batch_parent_type_completed_sessions, batch_flexible_type_completed_sessions, batch_Counseling_type_completed_sessions, 
    batch_max_student_session_attendance, batch_max_session_parent_attendance, batch_max_session_flexible_attendance, 
    batch_max_session_counseling_attendance, batch_indi_stud_attendance, batch_indi_parent_attendance, batch_indi_flexible_attendance, 
    batch_indi_counseling_attendance, batch_session_type_based_avg_overall_attendance, batch_max_overall_attendance, total_reached_parents,
    session_type as previous_session_type
    from {{ ref("dev_stg_session") }}),

    t3 as (
        select *
        from t1
        left join t2 on t1.batch_id = t2.session_batch_id
    ),

    t4 as (
        select
            t3.*,
            (
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
                    then 'Batch Completed'  -- above queries are processed first so baseline and endline do not clash
                    when
                        lower(session_name) like '%01.%'
                        and total_student_present is not null
                        and session_date is not null
                    then 'Batch Started'
                end
            ) batch_status
        from t3
    )

select *
from t4 --5070423


