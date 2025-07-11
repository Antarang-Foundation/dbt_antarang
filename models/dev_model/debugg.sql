WITH 
stg_session AS (
    SELECT 
        session_type, session_code, session_no, session_name, session_date, total_student_present, 
        session_id, session_batch_id, batch_expected_sessions, batch_expected_student_type_session, 
        batch_expected_parent_type_session, batch_expected_flexible_type_session, 
        batch_expected_counseling_type_session, batch_scheduled_sessions, 
        batch_flexible_type_completed_sessions, present_count, batch_indi_stud_attendance, 
        batch_indi_counseling_attendance, batch_indi_flexible_attendance, batch_indi_parent_attendance, 
        batch_max_overall_attendance, batch_max_student_session_attendance, 
        batch_max_session_counseling_attendance, batch_max_session_flexible_attendance, 
        batch_max_session_parent_attendance 
    FROM {{ ref('dev_stg_session') }}
),  

stg_somrt AS (
    SELECT 
        omr_type, somrt_session_id, somrt_batch_id 
    FROM {{ ref('dev_stg_somrt') }}
),

stg_attendance AS (
    SELECT DISTINCT 
        attendance_status, guardian_attendance, attendance_session_id 
    FROM {{ ref('dev_stg_attendance') }}
),

dev_int_global_dcp AS (
    SELECT DISTINCT 
        student_barcode, student_name, caste, school_name, school_area, school_district, 
        school_partner, school_state, school_taluka, batch_academic_year, batch_donor, 
        batch_grade, batch_language, batch_no, fac_start_date, facilitator_email, 
        facilitator_name, gender, no_of_students_facilitated, student_batch_id, batch_id 
    FROM {{ ref('dev_int_global_dcp') }}
    WHERE batch_academic_year >= 2023
),

t5 AS (
    SELECT 
        t1.session_name,
        t1.session_date,
        t1.session_type,
        t2.omr_type,
        t3.attendance_status,
        t3.guardian_attendance,

        -- Merge student info from either join
        COALESCE(t4a.student_barcode, t4b.student_barcode) AS student_barcode,
        COALESCE(t4a.student_name, t4b.student_name) AS student_name,
        COALESCE(t4a.school_name, t4b.school_name) AS school_name,
        COALESCE(t4a.batch_grade, t4b.batch_grade) AS batch_grade,
        COALESCE(t4a.batch_language, t4b.batch_language) AS batch_language

    FROM stg_session t1
    INNER JOIN stg_somrt t2 ON t1.session_id = t2.somrt_session_id
    LEFT JOIN stg_attendance t3 ON t1.session_id = t3.attendance_session_id

    -- Two LEFT JOINs to fetch from both identifiers
    LEFT JOIN dev_int_global_dcp t4a ON t4a.student_batch_id = t1.session_batch_id
    LEFT JOIN dev_int_global_dcp t4b ON t4b.batch_id = t2.somrt_batch_id
)

-- Filter for your student here
SELECT *
FROM t5
WHERE COALESCE(student_barcode, '') = 'YOUR_STUDENT_BARCODE_HERE'
ORDER BY session_date
