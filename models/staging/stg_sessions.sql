

with source as (

    select * from {{ source('salesforce', 'src_Session__c') }}

),

renamed as (

    select
        id as sessions_id,
        session_code__c as sessions_code,
        name as sessions_name,
        session_type__c as sessions_type,
        sessiondate__c as sessions_date,
        session_grade__c as sessions_grade,
        session_number__c as sessions_number,
        assigned_facilitator__c as assigned_facilitator_id,
        batch__c as batches_id

    from source

)

select * from renamed
