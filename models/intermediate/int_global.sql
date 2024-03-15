with
    batches as (select * from {{ ref('stg_batch') }}),
    schools as (select * from {{ ref('stg_school') }}),
    facilitators as (select * from {{ ref('stg_facilitator') }}),
    donors as (select * from {{ ref('stg_donor') }}),
    int_global as (
        select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, fac_start_date, fac_end_date, 
        allocation_email_sent, facilitator_name,
        
        school_name, school_taluka, school_ward, school_district, school_state, school_academic_year, school_language, enrolled_g9, 
        enrolled_g10, enrolled_g11, enrolled_g12, tagged_for_counselling, school_partner, donor_name as batch_donor
        from 
            batches
            left join schools on batches.batch_school_id = schools.school_id
            left join facilitators on batches.batch_facilitator_id = facilitators.facilitator_id
            left join donors on batches.batch_donor_id = donors.donor_id)
    
select *

from int_global order by batch_id