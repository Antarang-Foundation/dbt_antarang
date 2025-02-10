WITH source AS (
    SELECT * 
    FROM {{ source('salesforce', 'Assessment_Marks__c') }}
),

t1 AS (
    SELECT 
        Of_Student_CA_Track__c AS percentage_student_ca_track,
        Of_Student_CP_Track__c AS percentage_student_cp_track,
        Of_Student_SA_Track__c AS percentage_student_sa_track,
        Name AS assessment_marks_number,
        RecordTypeId AS assessment_record_type,
        Student__c AS assessment_student_id,
        Total_Marks__c AS total_marks
    FROM source
)

select * from t1