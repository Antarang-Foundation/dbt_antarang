with source as (
    select * from {{ source('salesforce', 'Session_OMR_Type__c') }}
),

renamed as (
    select
        Id as somrt_id,
        Name as somrt_no,
        Session__c as somrt_session_id,
        Session_Batch_Id__c as somrt_batch_id,
        OMR_Type__c as omr_type,
        OMR_Assessment_Object__c as omr_assessment_object,
        OMR_Assessment_Count__c as omr_assessment_count,
        OMR_Assessment_Record_Type__c as omr_assessment_record_type,
        Session_Batch_Number__c as somrt_batch_no,
        First_OMR_Uploaded_Date__c as first_omr_uploaded_date,
        OMRs_Received_Count__c as omr_received_count,
        OMR_Received_Date__c as omr_received_date,
        OMR_Received_By__c as omr_received_by,
        Number_of_Students_in_Batch__c as number_of_students_in_batch
         
    from source
)

select * from renamed
