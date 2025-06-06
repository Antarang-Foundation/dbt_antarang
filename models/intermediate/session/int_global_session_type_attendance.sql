with 

t1 as (SELECT batch_no, batch_donor, school_partner, school_state, school_district, school_taluka, school_name, school_area, facilitator_name, facilitator_email,
batch_academic_year, batch_grade, session_grade, batch_language, fac_start_date, fac_end_date, no_of_students_facilitated, batch_max_student_session_attendance, 
batch_max_session_parent_attendance, batch_max_session_counseling_attendance, batch_max_session_flexible_attendance, batch_indi_stud_attendance, batch_indi_parent_attendance, batch_indi_flexible_attendance, batch_indi_counseling_attendance,
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions, session_type, 

max(batch_session_type_based_avg_overall_attendance) `batch_session_type_based_avg_overall_attendance` --this is a max overall attendance

FROM {{ref('int_global_session')}}

group by batch_no, batch_donor, school_partner, school_state, school_district, school_taluka, school_name, school_area, facilitator_name, facilitator_email,
batch_academic_year, batch_grade, session_grade, batch_language, fac_start_date, fac_end_date, no_of_students_facilitated, batch_max_student_session_attendance, batch_max_session_parent_attendance,batch_max_session_counseling_attendance, batch_max_session_flexible_attendance,
batch_indi_stud_attendance, batch_indi_parent_attendance, batch_indi_flexible_attendance, batch_indi_counseling_attendance,
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions, session_type)

select * from t1 