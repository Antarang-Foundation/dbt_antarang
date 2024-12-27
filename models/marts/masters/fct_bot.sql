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
)

select * from t3