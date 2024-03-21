with t0 as (
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

    from {{ source('salesforce', 'Account') }}
),

t1 as (select record_type_id, record_type from {{ ref('stg_recordtypes') }}),
    
    t2 as (
        select * except (record_type_id)
        from 
            t0
            left join t1 using (record_type_id) where record_type = 'School'
    ),

t3 as (select * from {{ ref('seed_state') }}),
t4 as (select * from {{ ref('seed_district') }}),
t5 as (select * from {{ ref('seed_ward') }}),
t6 as (select * from {{ ref('seed_taluka') }}),

t7 as (select * except (school_state_id, state_id, state_name, state_code, school_district_id, district_id, district_name, school_ward_id, ward_id, ward_name, school_taluka_id, taluka_id, taluka_name), state_name as school_state, district_name as school_district, ward_name as school_ward, taluka_name as school_taluka

from t2 

left join t3 on t2.school_state_id = t3.state_id 
left join t4 on t2.school_district_id = t4.district_id 
left join t5 on t2.school_ward_id = t5.ward_id 
left join t6 on t2.school_taluka_id = t6.taluka_id 

order by school_id)
select * from t7
    