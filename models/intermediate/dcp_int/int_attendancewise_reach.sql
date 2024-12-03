with t1 as (
    select student_id, student_barcode, gender, batch_no
    from {{ref('fct_studentwise_reach')}}
),

t2 as (select 
    session_batch_id, 
    batch_no as session_batch_no,
    batch_academic_year, batch_grade, batch_language, batch_donor, facilitator_id, facilitator_name,
    school_name, school_id, school_state, school_district, school_taluka, school_partner, school_area,
    session_id, session_type, session_no, attendance_submitted, 
    batch_expected_sessions, batch_scheduled_sessions, batch_completed_sessions
    from {{ref('int_global_session')}}
),

t3 as (
    select * from t1 full outer join t2 on t1.batch_no = t2.session_batch_no
),

t4 as (
    select attendance_student_id, attendance_session_id, attendance_no, 
    attendance_status, guardian_attendance
    from {{ref('stg_attendance')}}
),

t5 as (
    select * from t3 full outer join t4 on t3.student_id = t4.attendance_student_id and t3.session_id = t4.attendance_session_id
),

t6 AS (
    SELECT *,
        COUNT(CASE WHEN attendance_status = 'Present' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_student_session_attended`,
        COUNT(CASE WHEN attendance_status = 'Present' AND session_type = 'Student' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_attended_student_type_session`,
        COUNT(CASE WHEN attendance_status = 'Present' AND session_type = 'Parent' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_attended_parent_type_session`,
        COUNT(CASE WHEN attendance_status = 'Present' AND session_type = 'Counseling' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_attended_counseling_type_session`,
        COUNT(CASE WHEN guardian_attendance = 'Present' AND session_type = 'Parent' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_parent_attended_parent_type_session`,
         COUNT(CASE WHEN guardian_attendance = 'Present' AND session_type = 'Counseling' THEN 1 END) OVER (PARTITION BY student_barcode, batch_no) AS `total_parent_attended_Counseling_type_session`, 
    FROM t5
)

select * from t6

