with source as (
    select * from {{ source('salesforce', 'Contact') }}
),

renamed as (
    select
        Id as contact_id,
        Full_Name__c as full_name,

        RecordTypeId as record_type_id,
        Academic_Year__c as academic_year
    from source 
)

select * from renamed
