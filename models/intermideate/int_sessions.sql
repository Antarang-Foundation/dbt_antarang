with
    session_attendance as (select * from {{ ref('stg_session_attendance') }}),
    sessions as (select * from {{ ref('stg_sessions') }}),
    batches as (select * from {{ ref('stg_batches') }}),
    
    

int_sessions as (
    
    select *
    from 
        session_attendance
        left join sessions using (record_type_id)

)
select *
from int_cdm1_recordtypes