with

    t0 as (select * from {{ ref("int_student") }}),
    
    t1 as (select * from {{ ref("int_global") }}),

    t2 as (select * from t0 full outer join t1 on t0.student_batch_id = t1.batch_id)

    select * from t2 order by student_id, student_grade

