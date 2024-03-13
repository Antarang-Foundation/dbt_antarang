with

t0 as (select * from {{ ref('int_student_global_session_attendance')}}),
t1 as (select distinct global_batch_id, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, facilitator_name, 
school, school_taluka_id, school_ward_id, school_district_id, school_state_id, school_partner, batch_donor, session_id, session_batch_id, 
session_facilitator_id, session_code, session_name, session_type, session_date, session_grade, session_no, omr_required, omrs_received, 
total_student_present, total_parent_present, log_reason, attendance_submitted, present_count, attendance_count, payment_status, deferred_reason, 
invoice_date, session_amount, no_of_sessions_no_of_units, total_amount, attendance_id, attendance_student_id, attendance_session_id, attendance_no, 
attendance_status, attendance_date, attendance_time, guardian_attendance
from t0  where global_batch_id is not null order by global_batch_id, session_id, attendance_student_id)
select * from t1 
