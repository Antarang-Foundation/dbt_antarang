with
    batches as (select * from {{ ref('stg_batches') }}),
    schools as (select * from {{ ref('stg_accounts') }} where record_type_id = '0127F000000Hqg2QAC'),
    trainers as (select * from {{ ref('stg_contacts') }} where record_type_id in ('0127F000000Hqg5QAC', '0127F000000BfaOQAS')),
    donors as (select * from {{ ref('stg_accounts') }} where record_type_id = '0127F000000Hqg1QAC'),
    int_global as (
        select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, fac_start_date, fac_end_date, 
        allocation_email_sent, schools.tagged_for_counselling, full_name as batch_trainer_name,
        schools.account_name as school, schools.taluka as taluka, schools.ward as ward, school_district, school_state, school_partner, 
        donors.account_name as donor
        from 
            batches
            left join schools on batches.school_id = schools.account_id
            left join trainers on batches.batch_trainer_id = trainers.contact_id
            left join donors on batches.donor_id = donors.account_id)
    
select *

from int_global