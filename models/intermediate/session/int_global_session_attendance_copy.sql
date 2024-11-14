with t1 as (
    select * from {{ref('int_global_session')}}
),

t2 as (
    select * from {{ref('stg_attendance')}}
),

t3 as (
    select * from t1 inner join t2 on t1.session_id = t2.attendance_session_id
),

t4 as (
select 
batch_no, batch_academic_year, batch_language, batch_donor,
facilitator_name, school_name, school_state, school_district, school_taluka, school_partner, 
session_name, session_type, session_id, session_date, session_grade, session_no, total_student_present, total_parent_present, 
attendance_submitted, present_count, parent_present_count, session_mode, batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions, 
attendance_no, attendance_student_id, attendance_status, guardian_attendance 
from t3
),

t5 AS (
    SELECT *,
    COUNT(CASE 
            WHEN attendance_status = 'Present' AND session_type = 'Student' 
            THEN 1 END) 
            OVER (PARTITION BY attendance_student_id) AS indi_student_attendance,
    COUNT(CASE 
            WHEN guardian_attendance = 'Present' AND session_type = 'Parent' 
            THEN 1 END) 
            OVER (PARTITION BY attendance_student_id) AS indi_parent_attendance,
    COUNT(CASE 
            WHEN attendance_status = 'Present' AND session_type = 'Counseling' 
            THEN 1 END) 
            OVER (PARTITION BY attendance_student_id) AS indi_counseling_attendance
    FROM t4
)

select * from t5
