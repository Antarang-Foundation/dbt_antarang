with t1 as (select * from {{ref('stg_student')}})

select * from t1 where g9_batch_id is null and g10_batch_id is null and g11_batch_id is null and g12_batch_id is null 

order by student_id