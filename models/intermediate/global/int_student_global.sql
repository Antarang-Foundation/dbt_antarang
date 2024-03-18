with

    t0 as (select * from {{ ref("int_student") }}),
    
    t1 as (select * from {{ ref("int_global") }}),

    t2 as (select t0.*, t1.batch_id as global_batch_id, t1.* except(batch_id) from t0 full outer join t1 on t0.contact_batch_id = t1.batch_id)

    select * from t2 order by student_id, contact_grade

