with source as (
    select * from {{ source('salesforce', 'Batch__c') }} where IsDeleted = false and lower(Name) not like '%test%'
),

a as (
    select
        id as batch_id,
        batch_number__c as batch_no,
        Name as batch_name,
        School_Name__c as batch_school_id,
        academic_year__c as batch_academic_year,
        grade__c as batch_grade,
        batch_completed__c as batch_completed,
        Medium_Language_of_Instruction__c as batch_language,
        Trainer_Name__c as batch_facilitator_id,
        Date_of_facilitation_starting__c as fac_start_date,
        Date_of_facilitation_completion__c as fac_end_date,
        Allocation_Email_Sent__c as allocation_email_sent,
        Donor_Name__c as batch_donor_id,
        Number_of_students_facilitated__c as no_of_students_facilitated

    from source
     
)

select * from a 
--where batch_no = '32416'
-- where batch_name like '%Dummy%' or batch_name like '%dummy%'