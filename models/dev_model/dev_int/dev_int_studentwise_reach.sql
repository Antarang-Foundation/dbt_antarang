WITH t1 AS (
    SELECT 
        student_id, student_barcode, gender, batch_no
    FROM {{ ref('dev_int_studentwise_demographic') }}
),

t2 AS (
    SELECT 
        session_batch_id, 
        batch_no AS session_batch_no,
        batch_academic_year, batch_grade, batch_language, batch_donor, facilitator_id, facilitator_name,
        school_name, school_id, school_state, school_district, school_taluka, school_partner, school_area,
        session_id, session_type, session_no, attendance_submitted, 
        batch_expected_sessions, batch_expected_student_type_session, 
        batch_scheduled_sessions, batch_completed_sessions
    FROM {{ ref('dev_int_global_session') }}
),

t3 AS (
    SELECT * 
    FROM t1 
    LEFT JOIN t2 ON t1.batch_no = t2.session_batch_no
),

t4 AS (
    SELECT
        attendance_student_id, attendance_session_id, attendance_no, 
        attendance_status, guardian_attendance
    FROM {{ ref('dev_stg_attendance') }}
),

t5 AS (
    SELECT DISTINCT * 
    FROM t3 
    LEFT JOIN t4 
        ON t3.student_id = t4.attendance_student_id 
        AND t3.session_id = t4.attendance_session_id
),

t6 AS (
    SELECT student_id,
        student_barcode,
        batch_no,

        MAX(gender) AS gender,
        MAX(batch_academic_year) AS batch_academic_year,
        MAX(batch_grade) AS batch_grade,
        MAX(batch_language) AS batch_language,
        MAX(batch_donor) AS batch_donor,
        MAX(facilitator_id) AS facilitator_id,
        MAX(facilitator_name) AS facilitator_name,
        MAX(school_name) AS school_name,
        MAX(school_id) AS school_id,
        MAX(school_state) AS school_state,
        MAX(school_district) AS school_district,
        MAX(school_taluka) AS school_taluka,
        MAX(school_partner) AS school_partner,
        MAX(school_area) AS school_area,

        MAX(batch_expected_sessions) AS batch_expected_sessions,
        MAX(batch_expected_student_type_session) AS batch_expected_student_type_session,
        MAX(batch_scheduled_sessions) AS batch_scheduled_sessions,
        MAX(batch_completed_sessions) AS batch_completed_sessions,

        COUNTIF(attendance_status = 'Present') AS total_student_session_attended,
        COUNTIF(attendance_status = 'Present' AND session_type = 'Student') AS total_attended_student_type_session,
        COUNTIF(attendance_status = 'Present' AND session_type = 'Parent') AS total_attended_parent_type_session,
        COUNTIF(attendance_status = 'Present' AND session_type = 'Counseling') AS total_attended_counseling_type_session,
        COUNTIF(guardian_attendance = 'Present' AND session_type = 'Parent') AS total_parent_attended_parent_type_session,
        COUNTIF(guardian_attendance = 'Present' AND session_type = 'Counseling') AS total_parent_attended_Counseling_type_session
    FROM t5
    GROUP BY student_id, student_barcode, batch_no
),

t7 AS (
    SELECT *,
        CASE 
            WHEN batch_expected_student_type_session = total_attended_student_type_session THEN 1 
            ELSE 0 
        END AS student_completed_program,

        CASE 
            WHEN batch_expected_student_type_session = 0 THEN 'No Expected Sessions'
            WHEN (total_attended_student_type_session / NULLIF(batch_expected_student_type_session, 0)) < 0.26 THEN '0% to 25% Attended'
            WHEN (total_attended_student_type_session / NULLIF(batch_expected_student_type_session, 0)) < 0.51 THEN '26% to 50% Attended'
            WHEN (total_attended_student_type_session / NULLIF(batch_expected_student_type_session, 0)) < 0.76 THEN '51% to 75% Attended'
            WHEN (total_attended_student_type_session / NULLIF(batch_expected_student_type_session, 0)) <= 1 THEN '75% to 100% Attended'
        END AS category_of_percentage_of_session_attended
    FROM t6
)

SELECT * FROM t7 --5072487 --5073974 -454649 -454776

