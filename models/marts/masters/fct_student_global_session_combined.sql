select 
student_barcode, student_name, school_name, school_area, school_district, school_partner, school_state, school_taluka, 
batch_academic_year, batch_donor, batch_grade, batch_language, batch_no, fac_start_date, facilitator_email, facilitator_name, gender, 
attendance_status, guardian_attendance,
omr_type, session_type, session_code, session_no, session_name, session_date, 
batch_expected_sessions, batch_expected_student_type_session, batch_expected_parent_type_session, batch_expected_flexible_type_session, batch_expected_counseling_type_session, 
batch_scheduled_sessions, batch_flexible_type_scheduled_sessions,
batch_flexible_type_completed_sessions, no_of_students_facilitated, total_student_present, present_count, 
batch_indi_stud_attendance, batch_indi_counseling_attendance, batch_indi_flexible_attendance, batch_indi_parent_attendance,
batch_max_overall_attendance, batch_max_student_session_attendance, batch_max_session_counseling_attendance, batch_max_session_flexible_attendance, batch_max_session_parent_attendance
from {{ref('int_student_global_session_combined')}}


/* select 
*
from {{ref('int_student_global_session_combined')}}
*/