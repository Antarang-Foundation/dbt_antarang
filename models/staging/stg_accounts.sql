with source as (
    select * from {{ source('salesforce', 'Account') }}
),

renamed as (
    select
        id as account_id,
        name as account_name,
        RecordTypeId as record_type_id,
        District__c as account_district,
        Medium_Language_of_Instruction__c as account_medium_language,
        CAST(Academic_Year__c as STRING) as academic_year
    from source
)

select * from renamed