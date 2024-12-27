with flow_result as (
    select id, name, profile_id, inserted_at, updated_at, contact_name, flow_version,
    contact_phone, bq_inserted_at, flow_context_id
    from {{source ('salesforce', 'flow_results')}}
),


t1 as (
    select * from {{ref('fct_bot_contact_global')}}
),

t2 as (
    select * from t1 
    left join flow_result as f
    ON t1.student_contact = f.contact_phone
    OR RIGHT(t1.student_contact, 10) = RIGHT(f.contact_phone, 10)
)

select 
student_id,
student_name,	
first_barcode,	
student_grade,	
student_barcode,	
student_batch_id,	
student_contact,	
gender,	
phone,	
student_on_chatbot,	
batch_id,	
batch_no,	
batch_academic_year,	
batch_grade,	
batch_language,	
no_of_students_facilitated,	
facilitator_name,	
school_name,	
school_state,	
school_district,	
school_taluka,	
batch_donor,	
school_area,	
id,	
name as flow_name,	
profile_id,	
inserted_at,	
updated_at,	
contact_name,	
flow_version,	
contact_phone,	
bq_inserted_at,	
flow_context_id
from t2

