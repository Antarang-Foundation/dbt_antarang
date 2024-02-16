with source as (
    select * from {{ source('salesforce', 'Account') }}
),

renamed as (
    select
        id as school_id,
        name as account_name,
        RecordTypeId as record_type_id,
        /* District__c as account_district, */
        Medium_Language_of_Instruction__c as account_medium_language,
        CAST(Academic_Year__c as STRING) as school_academic_year,
        School_Timing__c as School_Timing,
        Partner__c as Partner,
        /* State__c as State,
        District__c as District, */
        City__c as Taluka,
        Ward__c as Ward,
        Enrolled_Grade_9__c as Enrolled_Grade_9,
        Enrolled_Grade_10__c as Enrolled_Grade_10,
        Enrolled_Grade_11__c as Enrolled_Grade_11,
        Enrolled_Grade_12__c as Enrolled_Grade_12__c,
        Tagged_for_Counselling__c as Tagged_for_Counselling
        --Tagged_for_Experiential_Learning__c as Tagged_for_Experiential_Learning,
        --Tagged_for_Digital_Learning__c as Tagged_for_Digital_Learning__c

    from source
)

select * from renamed