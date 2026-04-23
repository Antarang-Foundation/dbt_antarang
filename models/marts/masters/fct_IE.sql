WITH t1 AS (
    SELECT *
    FROM {{ ref('int_student_global_assessment') }}
),

t2 AS (
    SELECT *
    FROM {{ ref('int_student_global_session_status') }}
),

t3 AS (
    SELECT 
        t1.*,
        CASE 
            WHEN t2.stud_present_flexible_type_sessions = 0 THEN 0 -- correct
            WHEN t2.stud_present_flexible_type_sessions > 0 THEN 1 -- correct
        END AS stud_present_flexible_sessions_type,
        CASE 
            WHEN t2.flexible_type_completed_sessions = 0 THEN 0 -- correct
            WHEN t2.flexible_type_completed_sessions > 0 THEN 1 -- correct
        END AS flexible_type_session_completed
    FROM t1
LEFT JOIN t2
ON t1.student_barcode = t2.student_barcode
)

SELECT *
FROM t3







