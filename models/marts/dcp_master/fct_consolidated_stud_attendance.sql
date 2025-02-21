with t1 as (
    select 
    student_id, student_barcode, gender, batch_no, 
    batch_academic_year, batch_grade, batch_language, batch_donor, facilitator_id, facilitator_name,
    school_name, school_id, school_state, school_district, school_taluka, school_partner, school_area,
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions,
total_student_session_attended as total_session_attended, batch_expected_student_type_session, total_attended_student_type_session,
total_attended_parent_type_session, total_attended_counseling_type_session, 
total_parent_attended_parent_type_session, total_parent_attended_Counseling_type_session, student_completed_program, category_of_percentage_of_session_attended
    from {{ref('int_attendancewise_reach')}}
group by  
student_id, student_barcode, gender, batch_no, 
batch_academic_year, batch_grade, batch_language, batch_donor, facilitator_id, facilitator_name,
school_name, school_id, school_state, school_district, school_taluka, school_partner, school_area,
batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions,
total_session_attended, batch_expected_student_type_session, total_attended_student_type_session,
total_attended_parent_type_session, total_attended_counseling_type_session, 
total_parent_attended_parent_type_session, total_parent_attended_Counseling_type_session, student_completed_program, category_of_percentage_of_session_attended
)

select * from t1