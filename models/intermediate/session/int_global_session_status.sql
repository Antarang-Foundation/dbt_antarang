with 

t1 as (SELECT batch_no, batch_donor, school_partner, school_state, school_district, school_ward, school_taluka, school_name, facilitator_name, 
batch_academic_year, batch_grade, session_grade, batch_language, 

count(distinct session_no) `expected_sessions`, count(distinct case when session_date is not null then session_id end) `scheduled_sessions`,
count(distinct case when session_date is not null and total_student_present > 0 then session_id end) `conducted_sessions`, 

max(no_of_students_facilitated) `batch_strength`, max(total_student_present) `max_overall_attendance` 

FROM {{ref('int_global_session')}} 

group by batch_no, batch_donor, school_partner, school_state, school_district, school_ward, school_taluka, school_name, facilitator_name, 
batch_academic_year, batch_grade, session_grade, batch_language),

t2 as (SELECT *, ROW_NUMBER() OVER (PARTITION BY batch_no) AS `dup_rank` FROM t1),

t3 as (select * except (dup_rank) from t2 where dup_rank=1)

select * from t3  where batch_no is not null order by batch_no
