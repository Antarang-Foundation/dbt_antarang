with t0 as (
    select
        id as school_id,
        name as school_name,
        RecordTypeId as record_type_id,
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
        Tagged_for_Counselling__c as tagged_for_counselling,
        School_Area__c as school_area
        --Tagged_for_Experiential_Learning__c as Tagged_for_Experiential_Learning,
        --Tagged_for_Digital_Learning__c as Tagged_for_Digital_Learning__c
    from {{ source('salesforce', 'Account') }} 
    where IsDeleted = false AND lower(name) not like '%test%'
),

t1 as (
    select record_type_id, record_type 
    from {{ ref('seed_recordtype') }} 
    where record_type = 'School'
),
    
t2 as (
    select 
        t0.school_id, 
        t0.school_name, 
        t0.school_language, 
        t0.school_academic_year, 
        t0.school_timing, 
        t0.school_partner, 
        t0.enrolled_g9, 
        t0.enrolled_g10, 
        t0.enrolled_g11, 
        t0.enrolled_g12, 
        t0.tagged_for_counselling, 
        t0.school_area, 
        t1.record_type,
        t0.school_state_id,
        t0.school_district_id,
        t0.school_ward_id,
        t0.school_taluka_id
    from t0
    inner join t1 
    on t0.record_type_id = t1.record_type_id
),

t3 as (select state_name, state_id from {{ ref('seed_state') }}),
t4 as (select district_name, district_id from {{ ref('seed_district') }}),
t5 as (select ward_name, ward_id from {{ ref('seed_ward') }}),
t6 as (select taluka_name, taluka_id from {{ ref('seed_taluka') }}),

t7 as (
    select 
        t2.*, 
        t3.state_name as school_state, 
        t4.district_name as school_district, 
        t5.ward_name as school_ward, 
        t6.taluka_name as school_taluka
    from t2 
    left join t3 on t2.school_state_id = t3.state_id 
    left join t4 on t2.school_district_id = t4.district_id 
    left join t5 on t2.school_ward_id = t5.ward_id 
    left join t6 on t2.school_taluka_id = t6.taluka_id
)

select *
from t7
    