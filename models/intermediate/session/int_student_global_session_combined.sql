with

t0 as (select * from {{ ref('int_student_global')}}),
t1 as (select * from {{ ref('int_session_combined')}}),
t2 as (select * from t0 full outer join t1 on coalesce(t0.student_batch_id, t0.batch_id) = coalesce(t1.session_batch_id, t1.somrt_batch_id) 
and t0.student_id = t1.attendance_student_id)

select * from t2 

-- where student_id is not null and session_id is not null and somrt_id is not null and attendance_student_id is not null