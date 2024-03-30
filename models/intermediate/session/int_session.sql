with t1 as (select * from {{ref('stg_session')}}), 
t2 as (select * from {{ref('stg_somrt')}}),
t3 as (select * from {{ref('stg_attendance')}}),
t4 as (select * from t1 

full outer join t2 on t1.session_id = t2.somrt_session_id
full outer join t3 on coalesce(t1.session_id, t2.somrt_session_id) = t3.attendance_session_id order by session_id, somrt_session_id, attendance_session_id)

select * from t4