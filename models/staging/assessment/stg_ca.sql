WITH source AS (
    SELECT 
        cdm3_id, 
        cdm3_name, 
        form_name,
        Q6_2, Q6_3, 
        Q6_6, Q6_7, Q6_8, Q6_9, Q6_10, Q6_11, Q6_12, Q6_13, 
        Q6_2_Marks, Q6_3_Marks,	
        Q6_6_Marks, Q6_7_Marks, Q6_8_Marks, Q6_9_Marks, 
        Q6_10_Marks, Q6_11_Marks, Q6_12_Marks, Q6_13_Marks,	
        error_status, 
        created_by_id, 
        form_language, 
        record_type_id as record_type_id_ca,	
        created_by_date, 
        assessment_grade,	
        created_from_form,	
        assessment_barcode,	
        last_modified_date,	
        marks_recalculated,	
        assessment_batch_id, 
        assessment_student_id,	
        assessment_academic_year
    FROM {{ source('salesforce', 'CA_Stage') }}
),

record_type AS (
    SELECT 
        record_type_id, 
        record_type 
    FROM {{ ref('seed_recordtype') }}
)

SELECT *
FROM source s
LEFT JOIN record_type r
    ON s.record_type_id_ca = r.record_type_id -- This Join condition will change after orignal source comes

