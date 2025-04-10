with 

t1 as (select * from {{ref('int_student_global')}}),
t2 as (select * from {{ref('stg_session')}}),
t3 as (select * from t1 full outer join t2 on t1.batch_id = t2.session_batch_id),

--select * from t3

t4 AS (
    SELECT t3.*,
        (CASE
            WHEN t3.session_mode = 'Any' AND t3.school_district = 'RJ Model B' 
            THEN 'HW Session'
            ELSE t3.session_type
        END)updated_session_type
    FROM t3
)

SELECT * FROM t4 where batch_academic_year >= 2024 and facilitator_name is not null
