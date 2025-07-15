with 
t1 as (select * from {{ref('stg_session')}}),  
t2 as (select * from {{ref('stg_somrt')}}),
t3 as (select * from {{ref('stg_attendance')}}),
t4 as (select * from t1 
left join t2 on t1.session_id = t2.somrt_session_id 
left join t3 on coalesce(t1.session_id, t2.somrt_session_id) = t3.attendance_session_id 
order by session_id, somrt_session_id, attendance_session_id
)

select * from t4

/*
with 
t1 as (select * from {{ref('stg_session')}}),  
t2 as (select * from {{ref('stg_somrt')}}),
t3 as (select * from {{ref('stg_attendance')}}),
t4 as (select * from t1 left join t2 on t1.session_id = t2.somrt_session_id),
t5 as (select * from t4 left join t3 on t4.session_id = t3.attendance_session_id)

select * from t5
*/
