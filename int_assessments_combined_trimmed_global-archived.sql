with

    t0 as (select * from {{ ref("int_students_global") }}),
    t1 as (select * from {{ ref("int_assessments_combined_trimmed") }}),
    t2 as (select * from t0 full outer join t1 on assessment_batch_id = batch_id)

select *
from t2
order by uid
