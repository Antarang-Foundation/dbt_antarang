with 

t1 as (SELECT batch_no, batch_donor, school_partner, school_state, school_district, school_ward, school_taluka, school_name, facilitator_name, 
batch_academic_year, batch_grade, session_grade, batch_language, fac_start_date, fac_end_date, no_of_students_facilitated, batch_max_session_attendance, 
batch_max_session_parent_attendance,
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions, session_type, 

max(batch_session_type_based_avg_overall_attendance) `batch_session_type_based_avg_overall_attendance`

FROM {{ref('int_global_session')}}

group by batch_no, batch_donor, school_partner, school_state, school_district, school_ward, school_taluka, school_name, facilitator_name, 
batch_academic_year, batch_grade, session_grade, batch_language, fac_start_date, fac_end_date, no_of_students_facilitated, batch_max_session_attendance, batch_max_session_parent_attendance,
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions, session_type)

select * from t1 