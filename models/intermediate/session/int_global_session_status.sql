with 

t1 as (SELECT batch_no, batch_donor, school_partner, school_state, school_district, school_ward, school_taluka, school_name, school_area, facilitator_name, 
batch_academic_year, batch_grade, session_grade, batch_language, fac_start_date, fac_end_date, no_of_students_facilitated, batch_max_session_attendance, 
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions,

round(100*safe_divide(batch_scheduled_sessions, batch_expected_sessions), 1) `batch_pct_scheduled_vs_expected`,

round(100*safe_divide(batch_completed_sessions, batch_scheduled_sessions), 1) `batch_pct_completed_vs_scheduled`,

round(100*safe_divide(batch_completed_sessions, batch_expected_sessions), 1) `batch_pct_completed_vs_expected`

FROM {{ref('int_global_session')}} 

group by batch_no, batch_donor, school_partner, school_state, school_district, school_ward, school_taluka, school_name, school_area, facilitator_name, 
batch_academic_year, batch_grade, session_grade, batch_language, fac_start_date, fac_end_date, no_of_students_facilitated, batch_max_session_attendance, 
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions)

select * from t1  
