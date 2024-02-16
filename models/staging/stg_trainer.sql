with source as (
    select * from {{ source('salesforce', 'Contact') }}
),

renamed as (
    select
        Id as contact_id,
        Full_Name__c as full_name,
        Language__c as language,
        Facilitator_Work_Status__c as facilitator_work_status,
        Payment_Type__c as payment_type,
        Area_of_Operation__c as area_of_operation,
        Cities_of_Operation__c as cities_of_operation,
        Trainer_Status__c as trainer_status,
        npe01__WorkEmail__c as work_email,
        MobilePhone as facilitator_mobile,
        RecordTypeId as record_type_id,
        Academic_Year__c as academic_year
    from source 
)

select * from renamed
