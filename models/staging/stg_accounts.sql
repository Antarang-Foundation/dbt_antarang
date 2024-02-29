with source as (
    select * from {{ source('salesforce', 'Account') }}
),

renamed as (
    select
        id as account_id,
        name as account_name,
        RecordTypeId as record_type_id,
        /* District__c as account_district, */
        Medium_Language_of_Instruction__c as account_language,
        CAST(Academic_Year__c as STRING) as account_academic_year,
        School_Timing__c as school_timing,
        Partner__c as account_partner,
        /* State__c as State,
        District__c as District, */
        City__c as taluka,
        Ward__c as ward,
        Enrolled_Grade_9__c as enrolled_g9,
        Enrolled_Grade_10__c as enrolled_g10,
        Enrolled_Grade_11__c as enrolled_g11,
        Enrolled_Grade_12__c as enrolled_g12,
        Tagged_for_Counselling__c as tagged_for_counselling
        --Tagged_for_Experiential_Learning__c as Tagged_for_Experiential_Learning,
        --Tagged_for_Digital_Learning__c as Tagged_for_Digital_Learning__c

    from source
)

select * from renamed