WITH source AS (

    SELECT 
        Id AS diagnostics_id,
        Name AS sd_no,
        Grade__c AS assessment_grade,

        A_Q_1_1__c AS a_q1,
        A_Q_1_2__c AS a_q2,
        A_Q_1_3__c AS a_q3,
        A_Q_1_4__c AS a_q4,
        A_Q_1_5__c AS a_q5,
        A_Q_1_6__c AS a_q6,
        A_Q_1_7__c AS a_q7,
        A_Q_1_8__c AS a_q8,
        A_Q_1_9__c AS a_q9,
        A_Q_1_10__c AS a_q10,

        A_Q_1_1_Marks__c AS a_q1_marks,
        A_Q_1_2_Marks__c AS a_q2_marks,
        A_Q_1_3_Marks__c AS a_q3_marks,
        A_Q_1_4_Marks__c AS a_q4_marks,
        A_Q_1_5_Marks__c AS a_q5_marks,
        A_Q_1_6_Marks__c AS a_q6_marks,
        A_Q_1_7_Marks__c AS a_q7_marks,
        A_Q_1_8_Marks__c AS a_q8_marks,
        A_Q_1_9_Marks__c AS a_q9_marks,
        A_Q_1_10_Marks__c AS a_q10_marks,

        B_Q_1_1__c AS b_q1,
        B_Q_1_2__c AS b_q2,
        B_Q_1_3__c AS b_q3,
        B_Q_1_4__c AS b_q4,
        B_Q_1_5__c AS b_q5,
        B_Q_1_6__c AS b_q6,
        B_Q_1_7__c AS b_q7,
        B_Q_1_8__c AS b_q8,
        B_Q_1_9__c AS b_q9,
        B_Q_1_10__c AS b_q10,

        B_Q_1_1_Marks__c AS b_q1_marks,
        B_Q_1_2_Marks__c AS b_q2_marks,
        B_Q_1_3_Marks__c AS b_q3_marks,
        B_Q_1_4_Marks__c AS b_q4_marks,
        B_Q_1_5_Marks__c AS b_q5_marks,
        B_Q_1_6_Marks__c AS b_q6_marks,
        B_Q_1_7_Marks__c AS b_q7_marks,
        B_Q_1_8_Marks__c AS b_q8_marks,
        B_Q_1_9_Marks__c AS b_q9_marks,
        B_Q_1_10_Marks__c AS b_q10_marks,

        Barcode__c AS assessment_barcode,
        Student__c AS assessment_student_id,
        Batch_Id__c AS assessment_batch_id,
        CreatedById AS created_by_id,
        CreatedDate AS created_by_date,
        Form_Name__c AS form_name,
        RecordTypeId AS record_type_id,

        Error_Status__c AS error_status,

        Academic_Year__c AS assessment_academic_year,
        Form_Language__c AS form_language,
        LastModifiedDate AS last_modified_date,
        Created_from_Form__c AS created_from_form,
        Marks_Recalculated__c AS marks_recalculated,
        Form_submitted_Set_A__c AS form_submitted_set_a,
        Form_submitted_Set_B__c AS form_submitted_set_b

    FROM {{ source('salesforce', 'Student_Diagnostic__c') }}
    
    WHERE IsDeleted = FALSE
),

t2 AS (

    SELECT 
        record_type_id,
        record_type
    FROM {{ ref('seed_recordtype') }}

),

final as (SELECT 
    s.*,
    t2.record_type

FROM source s
LEFT JOIN t2
    ON s.record_type_id = t2.record_type_id
)

select *  from final