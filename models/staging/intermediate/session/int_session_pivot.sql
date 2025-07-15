WITH t0 AS (
    SELECT 
        batch_no, 
        session_name, 
        session_no, 
        session_type, 
        total_student_present, 
        total_parent_present, 
        present_count,
        omr_required
    FROM {{ ref('int_global_session') }}
),

t1 AS (
    SELECT 
       * 
    FROM t0
    PIVOT (
        SUM(total_student_present) 
        FOR session_no IN (1,3,4,5,6,11,14)
    ) AS pvt
)

SELECT 
batch_no, total_parent_present, present_count, _1, _3, _4, _5, _6, _11, _14
FROM t1
ORDER BY session_type
