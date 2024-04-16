with t1 as (select * from {{ref('int_global')}}),
t2 as (select * from {{ref('int_session')}})

select * from t1 full outer join t2 on t1.batch_id = t2.session_batch_id order by batch_id, session_id, somrt_id, attendance_id