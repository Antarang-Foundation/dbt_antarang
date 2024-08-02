with t1 as (select * from {{ref('int_global_session_attendance')}}),

t2 as(select batch_no, batch_max_session_attendance, batch_max_session_parent_attendance, batch_max_session_counseling_attendance, batch_expected_sessions, 
batch_scheduled_sessions, batch_completed_sessions, batch_session_type_based_avg_overall_attendance from {{ref('fct_global_session_type_attendance')}}),

t3 as (select * from t1 full outer join t2 on t1.batch_no = t2.batch_no)

select * from t3