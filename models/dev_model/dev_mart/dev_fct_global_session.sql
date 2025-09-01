with
stg_batch as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language
from {{ref('dev_stg_batch')}}),

int_global_session as 
(select 
no_of_students_facilitated, session_batch_id,
fac_start_date, fac_end_date, allocation_email_sent, facilitator_name, facilitator_email, school_name, school_state, school_district,
school_taluka, enrolled_g9, enrolled_g10, enrolled_g11, enrolled_g12, tagged_for_counselling, school_partner, school_area,
batch_donor, session_id, session_code, session_name, session_date, session_no, omr_required, omrs_received, total_student_present,
total_parent_present, log_reason, attendance_submitted, present_count, attendance_count, payment_status, parent_present_count,
session_mode, batch_expected_sessions, batch_expected_student_type_session, batch_expected_parent_type_session, batch_expected_flexible_type_session,
batch_expected_counseling_type_session, batch_scheduled_sessions, batch_student_type_scheduled_sessions, batch_parent_type_scheduled_sessions,
batch_flexible_type_scheduled_sessions, batch_counseling_type_scheduled_sessions, batch_completed_sessions, batch_student_type_completed_sessions,
batch_parent_type_completed_sessions, batch_flexible_type_completed_sessions, batch_Counseling_type_completed_sessions,
batch_max_student_session_attendance, batch_max_session_parent_attendance, batch_max_session_flexible_attendance, batch_max_session_counseling_attendance,
batch_indi_stud_attendance, batch_indi_parent_attendance, batch_indi_flexible_attendance, batch_indi_counseling_attendance,
batch_max_overall_attendance, total_reached_parents, session_type, batch_status
from {{ref('dev_int_global_session')}}
),

t3 as (select *
        from stg_batch
        left join int_global_session t2 on stg_batch.batch_id = t2.session_batch_id
    )

select distinct * from t3
where batch_academic_year >= 2023 

