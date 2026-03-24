with 
t1 as (select * from {{ref('stg_session')}}),  
t2 as (select * from {{ref('stg_somrt')}}),
t3 as (select * from {{ref('stg_attendance')}}),
t5 as (select * from {{ref('int_student_global')}}),
t4 as (select * from t5 
left join t3 on t5.student_id = t3.attendance_student_id
left join t1 on  t3.attendance_session_id = t1.session_id
left join t2 on t1.session_id = t2.somrt_session_id
)

select * from t4



