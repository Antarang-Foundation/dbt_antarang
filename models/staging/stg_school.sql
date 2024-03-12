with source as (
    select * from {{ source('salesforce', 'Account') }}
),

renamed as (
    select
        id as school_id,
        name as school_name,
        RecordTypeId as record_type_id,
        /* District__c as account_district, */
        Medium_Language_of_Instruction__c as school_language,
        CAST(Academic_Year__c as STRING) as school_academic_year,
        School_Timing__c as school_timing,
        Partner__c as school_partner,
        State__c as school_state_id,
        District__c as school_district_id, 
        City__c as school_taluka_id,
        Ward__c as school_ward_id,
        Enrolled_Grade_9__c as enrolled_g9,
        Enrolled_Grade_10__c as enrolled_g10,
        Enrolled_Grade_11__c as enrolled_g11,
        Enrolled_Grade_12__c as enrolled_g12,
        Tagged_for_Counselling__c as tagged_for_counselling
        --Tagged_for_Experiential_Learning__c as Tagged_for_Experiential_Learning,
        --Tagged_for_Digital_Learning__c as Tagged_for_Digital_Learning__c

    from source
),

recordtypes as (select record_type_id, record_type from {{ ref('stg_recordtypes') }}),
    
    stg_school as (
        select * except (record_type_id)
        from 
            renamed
            left join recordtypes using (record_type_id) where record_type = 'School'
    )
    
select *
from stg_school