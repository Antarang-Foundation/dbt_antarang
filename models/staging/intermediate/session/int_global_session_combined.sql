with t1 as (select * from {{ref('int_global')}}),
t2 as (select * from {{ref('int_session_combined')}}),

t3 as (select * from t1 full outer join t2 on t1.batch_id = coalesce(t2.session_batch_id, t2.somrt_batch_id) )

select * from t3 where batch_academic_year >= 2023 and facilitator_name is not null