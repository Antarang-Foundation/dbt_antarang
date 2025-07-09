WITH 
stg_session AS (
    SELECT session_type, session_code, session_no, session_name, session_date, total_student_present, 
session_id, session_batch_id, batch_expected_sessions, batch_expected_student_type_session, batch_expected_parent_type_session,
batch_expected_flexible_type_session, batch_expected_counseling_type_session, batch_scheduled_sessions, batch_flexible_type_completed_sessions,
present_count, batch_indi_stud_attendance, batch_indi_counseling_attendance, batch_indi_flexible_attendance, batch_indi_parent_attendance, 
batch_max_overall_attendance, batch_max_student_session_attendance, batch_max_session_counseling_attendance, 
batch_max_session_flexible_attendance, batch_max_session_parent_attendance FROM {{ ref('dev_stg_session') }} where session_date >= '2023-01-01'
),  

stg_somrt AS (
    SELECT omr_type, somrt_session_id, somrt_batch_id FROM {{ ref('dev_stg_somrt') }}
),

stg_attendance AS (
    SELECT DISTINCT
        attendance_status, guardian_attendance, attendance_session_id 
    FROM {{ ref('dev_stg_attendance') }}
),

-- Flattened student data to avoid cross join
dcp_students AS (
    SELECT DISTINCT 
        student_barcode, student_name, caste, school_name, school_area, school_district, school_partner, school_state,
        school_taluka, batch_academic_year, batch_donor, batch_grade, batch_language, batch_no, fac_start_date, facilitator_email,
        facilitator_name, gender, no_of_students_facilitated, student_batch_id, batch_id
    FROM {{ ref('dev_int_global_dcp') }}
    WHERE batch_academic_year >= 2023
),

t5 AS (
    SELECT
    t1.session_type, t1.session_code, t1.session_no, t1.session_name, t1.session_date, t1.total_student_present, t1.session_id, 
    t1.session_batch_id, t1.batch_expected_sessions, t1.batch_expected_student_type_session, t1.batch_expected_parent_type_session,
    t1.batch_expected_flexible_type_session, t1.batch_expected_counseling_type_session, t1.batch_scheduled_sessions, t1.batch_flexible_type_completed_sessions,
    t1.present_count, t1.batch_indi_stud_attendance, t1.batch_indi_counseling_attendance, t1.batch_indi_flexible_attendance, t1.batch_indi_parent_attendance, 
    t1.batch_max_overall_attendance, t1.batch_max_student_session_attendance, t1.batch_max_session_counseling_attendance, 
    t1.batch_max_session_flexible_attendance, t1.batch_max_session_parent_attendance, t2.omr_type, t2.somrt_session_id, t2.somrt_batch_id,
    t3.attendance_status, t3.guardian_attendance, t3.attendance_session_id,
    t4.student_barcode, t4.student_name, t4.caste, t4.school_name, t4.school_area, t4.school_district,
    t4.school_partner, t4.school_state, t4.school_taluka, t4.batch_academic_year, t4.batch_donor, 
    t4.batch_grade, t4.batch_language, t4.batch_no, t4.fac_start_date, t4.facilitator_email,
    t4.facilitator_name, t4.gender, t4.no_of_students_facilitated, t4.student_batch_id, t4.batch_id

    FROM stg_session t1 --105473
    LEFT JOIN stg_somrt t2 ON t1.session_id = t2.somrt_session_id --146591
    LEFT JOIN stg_attendance t3 ON coalesce(t1.session_id, t2.somrt_session_id) = t3.attendance_session_id --203994
    INNER JOIN dcp_students t4 ON t4.student_batch_id = t1.session_batch_id --5603157
)

SELECT *
FROM t5