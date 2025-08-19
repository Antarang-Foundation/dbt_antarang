WITH source AS (
  SELECT 
    Id AS hm_id,
    School__c AS hm_school_id,
    Assigned_Facilitator__c AS assigned_facilitator_id,
    Date__c AS hm_session_date,
    LastActivityDate AS last_activity_date,
    LastReferencedDate AS last_referenced_date,
    LastViewedDate AS last_viewed_date,
    CreatedDate AS created_date,
    LastModifiedDate AS last_modified_date,
    SystemModstamp AS system_modified_stamp,
    CAST(Academic_Year__c AS STRING) AS session_academic_year,
    Rescheduled_counter__c AS rescheduled_counter,
    Session_Number__c AS session_no,
    HM_Attended__c AS hm_attended,
    Late_Scedule__c AS late_scedule,
    Scheduling_Type__c AS scheduling_type,
    Session_Lead__c AS session_lead,
    Session_Status__c AS session_status,
    Session_Name__c AS hm_session_name,
    Name AS hm_facilitator_name,
    Start_Time__c AS start_time
  FROM {{ source('salesforce', 'HM_Session__c') }}
  WHERE IsDeleted = FALSE
)

SELECT * FROM source