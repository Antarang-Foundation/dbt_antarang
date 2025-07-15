WITH
t0 AS (
    SELECT * FROM {{ ref('int_student_global') }}
),
t1 AS (
    SELECT * FROM {{ ref('int_session_combined') }}
),
t2 AS (
    SELECT *
    FROM t0
    INNER JOIN t1
    ON COALESCE(t0.student_batch_id, t0.batch_id) = COALESCE(t1.session_batch_id, t1.somrt_batch_id)
)

SELECT *
FROM t2 
