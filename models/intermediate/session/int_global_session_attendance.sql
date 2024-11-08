WITH t1 AS (
    SELECT * 
    FROM {{ref('int_student_global')}}
),

t2 AS (
    SELECT 
        session_id, 
        session_batch_id, 
        session_facilitator_id, 
        session_code, 
        session_name, 
        session_type, 
        updated_session_type,
        session_date, 
        session_no, 
        session_grade, 
        total_student_present, 
        total_parent_present,
        attendance_submitted, 
        present_count 
    FROM {{ref('int_global_session')}}
),

t3 AS (
    SELECT * 
    FROM {{ref('stg_attendance')}}
),

t4 AS (
    SELECT * 
    FROM t1 
    INNER JOIN t2 
    ON t1.batch_id = t2.session_batch_id
),

t5 AS (
    SELECT * 
    FROM t4 
    INNER JOIN t3 
    ON t4.student_id = t3.attendance_student_id
),

t6 AS (
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
    FROM t5
)

SELECT 
    batch_academic_year,
    batch_grade,
    no_of_students_facilitated,
    school_state,
    school_district,
    school_taluka,
    school_area,
    batch_donor,
    school_partner,
    facilitator_name,
    session_batch_id, 
    session_type,
    updated_session_type, 
    session_name,
    student_id, 
    student_name,
    attendance_status,
    indi_student_attendance, 
    indi_parent_attendance, 
    indi_counseling_attendance 
FROM t6

