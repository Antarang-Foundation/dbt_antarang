with

t0 as (select * from {{ ref('int_student_global')}}),
t1 as (select * from {{ ref('stg_session')}}),
t2 as (select * from t0 full outer join t1 on t0.global_batch_id = t1.session_batch_id),
t3 as (select * from {{ ref('stg_attendance')}}),
t4 as (select * from t2 full outer join t3 on t2.session_id = t3.attendance_session_id and t2.student_id = t3.attendance_student_id 
order by student_id, contact_grade, session_code)

select * from t4