with source as (
     select * from {{ source('salesforce', 'Contact') }} where IsDeleted = false
),

renamed as (
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
    from source 
),

recordtypes as (select record_type_id,record_type from {{ ref('seed_recordtype') }}),
    stg_facilitator as (
        select * except (record_type_id)
        from 
            renamed
            left join recordtypes using (record_type_id) where record_type = 'CA Trainer'
    )

select *
from stg_facilitator where facilitator_name not like '%test%' or facilitator_name not like '%Test%' 
