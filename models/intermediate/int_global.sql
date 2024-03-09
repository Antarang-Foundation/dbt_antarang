with
    batches as (select * from {{ ref('stg_batch') }}),
    schools as (select * from {{ ref('stg_school') }}),
    trainers as (select * from {{ ref('stg_trainer') }}),
    donors as (select * from {{ ref('stg_donor') }}),
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