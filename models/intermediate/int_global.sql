with
    batches as (select * from {{ ref('stg_batches') }}),
    schools as (select * from {{ ref('stg_accounts') }} where record_type_id = '0127F000000Hqg2QAC'),
    trainers as (select * from {{ ref('stg_contacts') }} where record_type_id in ('0127F000000Hqg5QAC', '0127F000000BfaOQAS')),
    donors as (select * from {{ ref('stg_accounts') }} where record_type_id = '0127F000000Hqg1QAC'),
    int_global as (
        select batches_id as batch_id, batches_number as batch_no, batch_academic_year, batches_grade as batch_grade, 
        Medium_Language_of_Instruction as batch_language, Number_of_students_facilitated, Date_of_facilitation_starting as facilitation_start, 
        Date_of_facilitation_completion as facilitation_end, Allocation_Email_Sent, schools.account_name as school, 
        schools.account_medium_language as school_language, schools.school_academic_year, schools.Enrolled_Grade_9, schools.Enrolled_Grade_10, 
        schools.Enrolled_Grade_11, schools.Enrolled_Grade_12__c as Enrolled_Grade_12, school_district, School_State as school_state, 
        School_Partner as school_partner, schools.Partner as partner, schools.Taluka as taluka, schools.Ward as ward, schools.Tagged_for_Counselling, 
        full_name as batch_facilitator, trainers.contact_academic_year as contacts_academic_year, donors.account_name as donor
        from 
            batches
            left join schools on batches.school_id = schools.school_id
            left join trainers on batches.Batch_Trainer_Name = trainers.contact_id
            left join donors on batches.Donor_Name = donors.school_id)
    
select *

from int_global