with 

t1 as (select * from {{ref('int_student_global')}}),

t2 as (select session_id, session_batch_id, session_facilitator_id, session_code, session_name, session_type, session_date, session_no, session_grade, total_student_present, total_parent_present,
attendance_submitted, present_count from {{ref('stg_session')}}),

t3 as (select * from {{ref('stg_attendance')}}),

t4 as (select * from t1 full outer join t2 on t1.batch_id = t2.session_batch_id),
t5 as (select * from t4 inner join t3 on t4.student_id = t3.attendance_student_id)

select * from t5

/* student_id, student_name, first_barcode, student_barcode, student_batch_id, gender,
batch_id, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, fac_start_date, allocation_email_sent,
facilitator_name, school_name, school_taluka, school_district, school_state, school_academic_year, enrolled_g9, enrolled_g10, enrolled_g11, enrolled_g12,
tagged_for_counselling, school_partner, school_area, batch_donor, session_batch_id, session_code, session_name, session_type, 
session_date, session_no, session_grade, total_student_present, total_parent_present, attendance_submitted, present_count, 
attendance_id, attendance_student_id, attendance_session_id, attendance_no, attendance_status, attendance_date, attendance_time, guardian_attendance */




