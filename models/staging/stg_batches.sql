with source as (
    select * from {{ source('salesforce', 'Batch__c') }}
),

renamed as (
    select
        id as batches_id,
        batch_number__c as batches_number,
        Name as Batch_Name,
        School_Name__c as school_id,
        academic_year__c as batch_academic_year,
        grade__c as batches_grade,
        batch_completed__c as batch_completed,
        batch_id__c as batch_id,
        Medium_Language_of_Instruction__c as Medium_Language_of_Instruction,
        Trainer_Name__c as Batch_Trainer_Name,
        Date_of_facilitation_starting__c as Date_of_facilitation_starting,
        Date_of_facilitation_completion__c as Date_of_facilitation_completion,
        Allocation_Email_Sent__c as Allocation_Email_Sent,
        Donor_Name__c as Donor_Name,
        School_State__c as School_State,
        school_district__c as school_district,
        School_Partner__c as School_Partner,
        Number_of_students_facilitated__c as Number_of_students_facilitated

    from source
)

select * from renamed
