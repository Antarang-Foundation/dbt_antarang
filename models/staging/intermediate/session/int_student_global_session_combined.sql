WITH

-- 🔹 1. STUDENTS (base)
t0 AS (
    SELECT *
    FROM {{ ref('int_student_global') }}
),

-- 🔹 2. SESSIONS (1 row per session)
t_sessions AS (
    SELECT DISTINCT
        session_id,
        session_batch_id,
        session_status,
        somrt_batch_id,
        session_facilitator_id,
        session_code,
        session_name,
        session_type,
        session_date,
        session_start_time,
        session_grade,
        session_no,
        omr_required,
        omrs_received,
        total_student_present,
        total_parent_present,
        log_reason,
        attendance_submitted,
        present_count,
        attendance_count,
        somrt_id, somrt_no, somrt_session_id, omr_type,	omr_assessment_object, omr_assessment_count,	
            omr_assessment_record_type,	somrt_batch_no,	first_omr_upload_date, omr_received_count, 
        payment_status,
        deferred_reason,
        invoice_date,
        session_amount,
        no_of_sessions_no_of_units,
        total_amount,
        parent_present_count,
        session_mode,
        batch_indi_stud_attendance,
        batch_indi_parent_attendance,
        batch_indi_flexible_attendance,
        batch_indi_counseling_attendance,
        batch_session_type_based_avg_overall_attendance,
        batch_max_overall_attendance,
        total_reached_parents,

        -- batch metrics
        batch_expected_student_type_session,
        batch_expected_parent_type_session,
        batch_expected_flexible_type_session,
        batch_expected_counseling_type_session,
        batch_expected_sessions,
        batch_scheduled_sessions,
        batch_student_type_scheduled_sessions,
        batch_parent_type_scheduled_sessions,
        batch_flexible_type_scheduled_sessions,
        batch_counseling_type_scheduled_sessions,
        batch_completed_sessions,
        batch_student_type_completed_sessions,
        batch_parent_type_completed_sessions,
        batch_flexible_type_completed_sessions,
        batch_counseling_type_completed_sessions,
        batch_max_student_session_attendance,
        batch_max_session_parent_attendance,
        batch_max_session_flexible_attendance,
        batch_max_session_counseling_attendance

    FROM {{ ref('int_session_combined') }}
    WHERE session_batch_id IS NOT NULL
),

-- 🔹 3. ATTENDANCE (DEDUPED → 1 row per student + session)
t_attendance AS (
    SELECT *
    FROM (
        SELECT
            attendance_id,
            attendance_student_id,
            attendance_session_id,
            attendance_no,
            attendance_status,
            attendance_date,
            attendance_time,
            guardian_attendance,
            session_att_grade,
            somrt_id, somrt_no, somrt_session_id, omr_type,	omr_assessment_object, omr_assessment_count,	
            omr_assessment_record_type,	somrt_batch_no,	first_omr_upload_date, omr_received_count, 
            omr_received_date, omr_received_by,	number_of_students_in_batch,


            ROW_NUMBER() OVER (
                PARTITION BY attendance_student_id, attendance_session_id
                ORDER BY attendance_date DESC, attendance_time DESC
            ) AS rn

        FROM {{ ref('int_session_combined') }}
        WHERE attendance_student_id IS NOT NULL
    )
    WHERE rn = 1
),

-- 🔹 4. FINAL JOIN (student × session grain)
final AS (

SELECT
    s.*,

    -- session
    sess.*,

    -- attendance
    att.attendance_id,
    att.attendance_student_id,
    att.attendance_session_id,
    att.attendance_no,
    att.attendance_status,
    att.attendance_date,
    att.attendance_time,
    att.guardian_attendance,
    att.session_att_grade

FROM t0 s

-- ✅ all sessions for student's batch
LEFT JOIN t_sessions sess
    ON COALESCE(s.student_batch_id, s.batch_id) = sess.session_batch_id

-- ✅ one attendance per student per session
LEFT JOIN t_attendance att
    ON s.student_id = att.attendance_student_id
   AND sess.session_id = att.attendance_session_id

)

SELECT *  FROM final