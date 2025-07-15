with t1 as (
    select distinct school_name, batch_no, batch_academic_year, school_district,
    case when total_student_present is not null and session_date is not null then batch_no end as S1_Completed   
from {{ref('fct_global_session')}}
where session_name like '%01%' and batch_academic_year is not null
group by school_name, batch_no,total_student_present, session_date,batch_academic_year,school_district
), 

t2 as (
   select distinct school_name,
                    batch_academic_year,
                    school_district,
                    count(distinct batch_no) as Total_batches ,
                    count(distinct S1_Completed) as Completed_batches,
    case when count(distinct batch_no) = count(distinct S1_Completed) then 'Yes' end as All_S1_Completed 
    from t1
    group by school_name,batch_academic_year,school_district
)

select 
school_name,
batch_academic_year, school_district, Total_batches,
Completed_batches as all_s1_completed,
All_S1_Completed as all_s1_completed_status
from t2

    
    
