with t1 as (select * from {{ref('stg_student')}})

select * from t1 where g9_batch_id = g10_batch_id or g10_batch_id = g11_batch_id or g11_batch_id = g12_batch_id

order by student_id