WITH source AS (
    SELECT 
        Id AS cdm1_id,
        Barcode__c AS assessment_barcode,
        RecordTypeId AS record_type_id,
        CreatedDate AS created_on,
        Created_from_Form__c AS created_from_form,                       
        Name AS cdm1_no,
        Grade__c AS assessment_grade,
        CAST(Academic_Year__c AS STRING) AS assessment_academic_year,
        Batch_Id__c AS assessment_batch_id,
        Q_1__c AS q1,
        X1_A_good_career_plan_has_the_following__c AS q1_marks,
        Interest_1__c AS q2_1, 
        Interest_2__c AS q2_2,  
        Interest_Marks__c AS q2_marks,
        Aptitude_1__c AS q3_1, 
        Aptitude_2__c AS q3_2,
        Aptitude_Marks__c AS q3_marks,
        Career_Choice_1__c AS q4_1,
        Career_Choice_1_Marks__c AS q4_1_marks,
        Career_Choice_2__c AS q4_2,
        Career_Choice_2_Marks__c AS q4_2_marks,
        Career_Choice_Total_Marks__c AS q4_marks,
        (X1_A_good_career_plan_has_the_following__c + Interest_Marks__c + Aptitude_Marks__c + Career_Choice_Total_Marks__c) AS cdm1_total_marks,
        Error_Status__c AS error_status, 
        Data_Clean_up__c AS data_cleanup,
        Marks_Recalculated__c AS marks_recalculated,
        Student_Linked__c AS student_linked 
    FROM {{ source('salesforce', 'OMR_Assessment__c') }} 
    WHERE IsDeleted = false
),

recordtype AS (
    SELECT 
        record_type_id, 
        record_type 
    FROM {{ ref('seed_recordtype') }}
),

joined_source AS (
    SELECT 
        s.*,
        rt.record_type
    FROM source s
    INNER JOIN recordtype rt 
        ON s.record_type_id = rt.record_type_id
),

t2 AS (
    SELECT 
        cdm1_id, 
        assessment_barcode, 
        record_type, 
        created_on, 
        created_from_form, 
        cdm1_no,

        CASE 
            WHEN cdm1_no IS NOT NULL AND (q1 IS NOT NULL OR q2_1 IS NOT NULL OR q2_2 IS NOT NULL OR q3_1 IS NOT NULL OR q3_2 IS NOT NULL OR q4_1 IS NOT NULL OR q4_2 IS NOT NULL) 
                THEN 1 
            WHEN cdm1_no IS NOT NULL AND (q1 IS NULL AND q2_1 IS NULL AND q2_2 IS NULL AND q3_1 IS NULL AND q3_2 IS NULL AND q4_1 IS NULL AND q4_2 IS NULL) 
                THEN 0 
        END AS is_non_null,

       assessment_grade, assessment_academic_year, assessment_batch_id, q1,	q1_marks, q2_1,	q2_2, q2_marks, q3_1, q3_2, q3_marks,
       q4_1, q4_1_marks, q4_2, q4_2_marks, q4_marks, cdm1_total_marks, error_status, data_cleanup, marks_recalculated, student_linked
    FROM joined_source
)

SELECT * 
FROM t2