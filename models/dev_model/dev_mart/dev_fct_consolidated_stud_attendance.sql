select 
    student_id, student_barcode, gender, batch_no, 
    batch_academic_year, batch_grade, batch_language, batch_donor, facilitator_id, facilitator_name,
    school_name, school_id, school_state, school_district, school_taluka, school_partner, school_area,
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions,
total_student_session_attended as total_session_attended, batch_expected_student_type_session, total_attended_student_type_session,
total_attended_parent_type_session, total_attended_counseling_type_session, 
total_parent_attended_parent_type_session, total_parent_attended_Counseling_type_session, student_completed_program, category_of_percentage_of_session_attended
from {{ref('dev_int_studentwise_reach')}}
group by  
student_id, student_barcode, gender, batch_no, 
batch_academic_year, batch_grade, batch_language, batch_donor, facilitator_id, facilitator_name,
school_name, school_id, school_state, school_district, school_taluka, school_partner, school_area,
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions,
total_session_attended, batch_expected_student_type_session, total_attended_student_type_session,
total_attended_parent_type_session, total_attended_counseling_type_session, 
total_parent_attended_parent_type_session, total_parent_attended_Counseling_type_session, student_completed_program, category_of_percentage_of_session_attended
 --453162 -453289

/*2023 2341006190 2301043188 2304000382
2024 2403005686 2401072084 2401047330
2025 2503010002 2503004455 2503005562*/


/*select * from t1
WHERE batch_academic_year IN (2023, 2024, 2025) AND student_barcode IN ('2341006190', '2301043188', '2304000382', '2403005686',
'2401072084', '2401047330', '2503010002', '2503004455', '2503005562') 
order by batch_academic_year, student_id*/

