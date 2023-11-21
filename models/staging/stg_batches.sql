

with source as (

    select * from {{ source('salesforce', 'Batch__c') }}

),

renamed as (

    select
        id as batches_id,
        batch_number__c as batches_number,
        school_name__c as school_id,
        academic_year__c as academic_year,
        grade__c as batches_grade,
        batch_completed__c as batch_completed,
        batch_id__c,
        school_state__c,

    from source

)

select * from renamed
