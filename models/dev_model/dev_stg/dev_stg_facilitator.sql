with source as (
    select 
        Id as facilitator_id,
        Name as facilitator_name,
        Language__c as facilitator_language,
        Facilitator_Work_Status__c as facilitator_work_status,
        Payment_Type__c as payment_type,
        Area_of_Operation__c as facilitator_area,
        Cities_of_Operation__c as facilitator_city,
        Trainer_Status__c as facilitator_shift,
        npe01__WorkEmail__c as facilitator_email,
        MobilePhone as facilitator_mobile,
        RecordTypeId as record_type_id,
        Academic_Year__c as facilitator_academic_year 
    from {{ source('salesforce', 'Contact') }} 
    where IsDeleted = false and lower(name) not like '%test%'
),

recordtypes as (
    select record_type_id, record_type 
    from {{ ref('seed_recordtype') }} 
    where record_type = 'CA Trainer'
),

dev_stg_facilitator as (
    select 
        a.facilitator_id,
        a.facilitator_name, 
        a.facilitator_language, 
        a.facilitator_work_status, 
        a.payment_type, 
        a.facilitator_area, 
        a.facilitator_city, 
        a.facilitator_shift, 
        a.facilitator_email, 
        a.facilitator_mobile, 
        a.facilitator_academic_year, 
        b.record_type
    from source a
    inner join recordtypes b ON a.record_type_id = b.record_type_id
)

select *
from dev_stg_facilitator