WITH 
t1 AS (SELECT * FROM {{ref('stg_session')}}),  
t2 AS (SELECT * FROM {{ref('stg_somrt')}}),

t3 AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY attendance_student_id, attendance_session_id
                ORDER BY attendance_date DESC, attendance_time DESC
            ) AS rn
        FROM {{ ref('stg_attendance') }} 
    )
    WHERE rn = 1
),

t4 AS (
    SELECT *
    FROM t1
    LEFT JOIN t3  
        ON t1.session_id = t3.attendance_session_id
    LEFT JOIN t2 
        ON t1.session_id = t2.somrt_session_id
)

SELECT * 
FROM t4