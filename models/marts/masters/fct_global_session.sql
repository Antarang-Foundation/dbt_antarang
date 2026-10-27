
select session_name, session_type from {{ref('int_global_session')}}
where batch_academic_year>=2023 


