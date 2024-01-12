with source as (
    select * from {{ source('salesforce', 'Session_OMR_Type__c') }}
),

renamed as (
    select
        id as Session_OMR_Type_id,
        Name as SOMRT_number,
        Session__c as Session_id,
        Session_Batch_Id__c as Session_Batch_Id,
        OMR_Type__c as OMR_Type,
        OMR_Assessment_Object__c as OMR_Assessment_Object,
        OMR_Assessment_Count__c as OMR_Assessment_Count,
        OMR_Assessment_Record_Type__c as OMR_Assessment_Record_Type,
        Session_Batch_Number__c as Session_Batch_Number,
        First_OMR_Uploaded_Date__c as First_OMR_Uploaded_Date,
        OMRs_Received_Count__c as OMRs_Received_Count,
        OMR_Received_Date__c as OMR_Received_Date,
        OMR_Received_By__c as OMR_Received_By,
        Number_of_Students_in_Batch__c as Number_of_Students_in_Batch
        
    from source
)

select * from renamed
