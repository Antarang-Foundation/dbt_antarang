with 
t1 as (select * from {{ref('fct_global_somrt_upload_form')}}),
t2 as (select batch_no, session_name,session_type, total_student_present, total_parent_present, present_count, attendance_count from {{ref('fct_global_session')}}),

t3 as (select * from t1 full outer join t2 on t1.batch_no = t2.batch_no)

select * from t3

