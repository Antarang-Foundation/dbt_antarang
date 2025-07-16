with source as (
     select * from {{ source('salesforce', 'Contact') }} where IsDeleted = false
),

renamed as (
    select
        Id as facilitator_id,
        Name as facilitator_name,
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
