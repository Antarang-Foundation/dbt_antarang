with t1 as (
    select * from {{ref('fct_bot_contact_global')}}
),

t2 as (
    select 		
student_barcode as stud_barcode,		
flow_name,	
profile_id,	
inserted_at,	
updated_at,	
contact_name,	
flow_version,	
contact_phone,	
bq_inserted_at,	
flow_context_id
from {{ref('stg_bot_flow_results')}}
),

t3 as (
    select * from t1 
    full outer join t2 on
    t1.student_barcode = t2.stud_barcode
),

t4 as (
     SELECT 
        *,
        COUNT(DISTINCT contact_phone) OVER (
            PARTITION BY batch_academic_year, batch_donor, batch_grade, school_name
        ) AS chatbot_reach,
        CASE 
            WHEN no_of_students_facilitated IS NOT NULL AND no_of_students_facilitated != 0 
            THEN COUNT(DISTINCT contact_phone) OVER (
                PARTITION BY batch_academic_year, batch_donor, batch_grade, school_name
            ) / no_of_students_facilitated * 100
            ELSE 0
        END AS adoption_percentage
    FROM 
        t3
)

select
batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, 
school_name, school_state, school_district, batch_donor, chatbot_reach, adoption_percentage
from t4
group by batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, 
school_name, school_state, school_district, batch_donor, chatbot_reach, adoption_percentage

