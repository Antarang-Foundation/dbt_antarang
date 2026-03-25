WITH 
t1 AS (SELECT * FROM {{ref('stg_session')}}),  
t2 AS (SELECT * FROM {{ref('stg_somrt')}}),
t3 AS (SELECT * FROM {{ref('stg_attendance')}}),

t4 AS (
    SELECT *
    FROM t3
    LEFT JOIN t1 
        ON t3.attendance_session_id = t1.session_id
    LEFT JOIN t2 
        ON t1.session_id = t2.somrt_session_id
)

SELECT * 
FROM t4




