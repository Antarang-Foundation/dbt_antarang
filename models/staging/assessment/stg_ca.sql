WITH source AS (

    SELECT
        Id AS ca1_id,
        Name AS ca1_no,
        OwnerId AS owner_id,

        Grade__c AS assessment_grade,

        Q_2_A__c AS q2_a,
        Q_2_B__c AS q2_b,

        SAFE_CAST(Q_2_A_Marks__c AS NUMERIC) AS q2_a_marks,
        SAFE_CAST(Q_2_B_Marks__c AS NUMERIC) AS q2_b_marks,

        Barcode__c AS assessment_barcode,
        Student__c AS assessment_student_id,
        Batch_Id__c AS assessment_batch_id,

        CreatedDate AS created_on,

        RecordTypeId AS record_type_id,
        Error_Status__c AS error_status,

        CAST(Academic_Year__c AS STRING) AS assessment_academic_year,

        Data_Clean_up__c AS data_cleanup,
        Student_Linked__c AS student_linked,
        Created_from_Form__c AS created_from_form

    FROM {{ source('salesforce', 'CDM3__c') }}

    WHERE IsDeleted = FALSE

),

record_type AS (

    SELECT
        record_type_id,
        record_type
    FROM {{ ref('seed_recordtype') }}

),

final AS (

    SELECT
        s.ca1_id,
        s.ca1_no,

        rt.record_type,
        rt.record_type_id,

        s.owner_id,

        s.assessment_grade,
        s.assessment_academic_year,

        s.assessment_barcode,
        s.assessment_student_id,
        s.assessment_batch_id,

        s.created_on,
        s.created_from_form,

        s.q2_a,
        s.q2_a_marks,

        s.q2_b,
        s.q2_b_marks,

        s.error_status,
        s.data_cleanup,
        s.student_linked

    FROM source s
    LEFT JOIN record_type rt
        ON s.record_type_id = rt.record_type_id

)

SELECT *
FROM final