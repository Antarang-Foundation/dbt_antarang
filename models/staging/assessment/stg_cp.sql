WITH source AS (
    SELECT
        Id AS cp_id,
        Barcode__c AS assessment_barcode,
        RecordTypeId AS record_type_id,
        CreatedDate AS created_on,
        Created_from_Form__c AS created_from_form,
        Name AS cp_no,
        Grade__c AS assessment_grade,
        CAST(Academic_Year__c AS STRING) AS assessment_academic_year,
        Batch_Id__c AS assessment_batch_id,

        Q_7__c AS q7,
        Q_7_Marks__c AS q7_marks,
        Q_8__c AS q8,
        Q_8_Marks__c AS q8_marks,
        Q_9_1__c AS q9_1,
        Q_9_1_Marks__c AS q9_1_marks,
        Q_9_2__c AS q9_2,
        Q_9_2_Marks__c AS q9_2_marks,
        Q_9_3__c AS q9_3,
        Q_9_3_Marks__c AS q9_3_marks,
        Q_9_4__c AS q9_4,
        Q_9_4_Marks__c AS q9_4_marks,
        Q_9_5__c AS q9_5,
        Q_9_5_Marks__c AS q9_5_marks,
        Q_9_6__c AS q9_6,
        Q_9_6_Marks__c AS q9_6_marks,
        Q_9_7__c AS q9_7,
        Q_9_7_Marks__c AS q9_7_marks,
        Q_10__c AS q10,
        Q_10_Marks__c AS q10_marks,

        COALESCE(SAFE_CAST(Q_7_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_8_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_9_1_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_9_2_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_9_3_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_9_4_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_9_5_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_9_6_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_9_7_Marks__c AS NUMERIC), 0)
        + COALESCE(SAFE_CAST(Q_10_Marks__c AS NUMERIC), 0)
        AS cp_total_marks,

        Error_Status__c AS error_status,
        Data_Clean_up__c AS data_cleanup,
        Marks_Recalculated__c AS marks_recalculated,
        Student_Linked__c AS student_linked,

        Q1_C1__c AS q1_c1,
        Q1_C1_Ans__c AS q1_c1_ans,
        Q1_C1_Faci__c AS q1_c1_faci,
        Q1_C2__c AS q1_c2,
        Q1_C2_Ans__c AS q1_c2_ans,
        Q1_C2_Faci__c AS q1_c2_faci,

        A_Q2_C1__c AS a_q2_c1,
        A_Q2_C2__c AS a_q2_c2,

        A_Q3_C1__c AS a_q3_c1,
        A_Q3_C1_Ans__c AS a_q3_c1_ans,
        A_Q3_C2__c AS a_q3_c2,
        A_Q3_C2_Ans__c AS a_q3_c2_ans,

        A_Q4_C1__c AS a_q4_c1,
        A_Q4_C1_Ans__c AS a_q4_c1_ans,
        A_Q4_C2__c AS a_q4_c2,
        A_Q4_C2_Ans__c AS a_q4_c2_ans,

        A_Q5_C1__c AS a_q5_c1,
        A_Q5_C2__c AS a_q5_c2,

        A_Q6_C1__c AS a_q6_c1,
        A_Q6_C1_Ans__c AS a_q6_c1_ans,
        A_Q6_C2__c AS a_q6_c2,
        A_Q6_C2_Ans__c AS a_q6_c2_ans,

        A_Q7_C1__c AS a_q7_c1,
        A_Q7_C1_Ans__c AS a_q7_c1_ans,
        A_Q7_C2__c AS a_q7_c2,
        A_Q7_C2_Ans__c AS a_q7_c2_ans,

        A_Q8_C1__c AS a_q8_c1,
        A_Q8_C1_Ans__c AS a_q8_c1_ans,
        A_Q8_C2__c AS a_q8_c2,
        A_Q8_C2_Ans__c AS a_q8_c2_ans,

        B_Q2_C1__c AS b_q2_c1,
        B_Q2_C1_Ans__c AS b_q2_c1_ans,
        B_Q2_C2__c AS b_q2_c2,
        B_Q2_C2_Ans__c AS b_q2_c2_ans,

        B_Q3_C1__c AS b_q3_c1,
        B_Q3_C2__c AS b_q3_c2,

        B_Q4_C1__c AS b_q4_c1,
        B_Q4_C1_Ans__c AS b_q4_c1_ans,
        B_Q4_C2__c AS b_q4_c2,
        B_Q4_C2_Ans__c AS b_q4_c2_ans,

        B_Q5_C1__c AS b_q5_c1,
        B_Q5_C1_Ans__c AS b_q5_c1_ans,
        B_Q5_C2__c AS b_q5_c2,
        B_Q5_C2_Ans__c AS b_q5_c2_ans,

        B_Q6_C1__c AS b_q6_c1,
        B_Q6_C1_Ans__c AS b_q6_c1_ans,
        B_Q6_C2__c AS b_q6_c2,
        B_Q6_C2_Ans__c AS b_q6_c2_ans,

        B_Q7_C1__c AS b_q7_c1,
        B_Q7_C2__c AS b_q7_c2,

        Q2__c AS q2,
        Q2_Other__c AS q2_other,
        Q3__c AS q3,
        Q4__c AS q4,
        Q4_Ans__c AS q4_ans

    FROM {{ source('salesforce', 'Career_Planning__c') }}
    WHERE IsDeleted = FALSE
),

record_type AS (
    SELECT
        record_type_id,
        record_type
    FROM {{ ref('seed_recordtype') }}
),

joined_source AS (
    SELECT
        s.*,
        rt.record_type
    FROM source AS s
    INNER JOIN record_type AS rt
        ON s.record_type_id = rt.record_type_id
),


t2 AS (
    SELECT
        joined_source.*,

        CASE
            WHEN cp_no IS NOT NULL
                AND (
                    q7 IS NOT NULL
                    OR q8 IS NOT NULL
                    OR q9_1 IS NOT NULL
                    OR q9_2 IS NOT NULL
                    OR q9_3 IS NOT NULL
                    OR q9_4 IS NOT NULL
                    OR q9_5 IS NOT NULL
                    OR q9_6 IS NOT NULL
                    OR q9_7 IS NOT NULL
                    OR q10 IS NOT NULL
                    OR q1_c1 IS NOT NULL
                    OR q1_c1_ans IS NOT NULL
                    OR q1_c1_faci IS NOT NULL
                    OR q1_c2 IS NOT NULL
                    OR q1_c2_ans IS NOT NULL
                    OR q1_c2_faci IS NOT NULL
                    OR a_q2_c1 IS NOT NULL
                    OR a_q2_c2 IS NOT NULL
                    OR a_q3_c1 IS NOT NULL
                    OR a_q3_c1_ans IS NOT NULL
                    OR a_q3_c2 IS NOT NULL
                    OR a_q3_c2_ans IS NOT NULL
                    OR a_q4_c1 IS NOT NULL
                    OR a_q4_c1_ans IS NOT NULL
                    OR a_q4_c2 IS NOT NULL
                    OR a_q4_c2_ans IS NOT NULL
                    OR a_q5_c1 IS NOT NULL
                    OR a_q5_c2 IS NOT NULL
                    OR b_q2_c1 IS NOT NULL
                    OR b_q2_c1_ans IS NOT NULL
                    OR b_q2_c2 IS NOT NULL
                    OR b_q2_c2_ans IS NOT NULL
                    OR b_q3_c1 IS NOT NULL
                    OR b_q3_c2 IS NOT NULL
                    OR b_q4_c1 IS NOT NULL
                    OR b_q4_c1_ans IS NOT NULL
                    OR b_q4_c2 IS NOT NULL
                    OR b_q4_c2_ans IS NOT NULL
                    OR b_q5_c1 IS NOT NULL
                    OR b_q5_c1_ans IS NOT NULL
                    OR b_q5_c2 IS NOT NULL
                    OR b_q5_c2_ans IS NOT NULL
                    OR a_q6_c1 IS NOT NULL
                    OR a_q6_c1_ans IS NOT NULL
                    OR a_q6_c2 IS NOT NULL
                    OR a_q6_c2_ans IS NOT NULL
                    OR b_q6_c1 IS NOT NULL
                    OR b_q6_c1_ans IS NOT NULL
                    OR b_q6_c2 IS NOT NULL
                    OR b_q6_c2_ans IS NOT NULL
                    OR a_q7_c1 IS NOT NULL
                    OR a_q7_c1_ans IS NOT NULL
                    OR a_q7_c2 IS NOT NULL
                    OR a_q7_c2_ans IS NOT NULL
                    OR b_q7_c1 IS NOT NULL
                    OR b_q7_c2 IS NOT NULL
                    OR a_q8_c1 IS NOT NULL
                    OR a_q8_c1_ans IS NOT NULL
                    OR a_q8_c2 IS NOT NULL
                    OR a_q8_c2_ans IS NOT NULL
                    OR q2 IS NOT NULL
                    OR q2_other IS NOT NULL
                    OR q3 IS NOT NULL
                    OR q4 IS NOT NULL
                    OR q4_ans IS NOT NULL
                )
                THEN 1
            WHEN cp_no IS NOT NULL THEN 0
            ELSE NULL
        END AS is_non_null

    FROM joined_source
),

t3 AS (
    SELECT
        t2.*,

        CASE
            WHEN q8 IS NOT NULL AND q8 != '*'
                THEN SAFE_CAST(q8_marks AS NUMERIC)
            ELSE CAST(3 AS NUMERIC)
        END AS q8_null,

        CASE
            WHEN (
                CASE
                    WHEN q8 IS NOT NULL AND q8 != '*'
                        THEN SAFE_CAST(q8_marks AS NUMERIC)
                    ELSE CAST(3 AS NUMERIC)
                END
            ) = 1 THEN '1. Yes'

            WHEN (
                CASE
                    WHEN q8 IS NOT NULL AND q8 != '*'
                        THEN SAFE_CAST(q8_marks AS NUMERIC)
                    ELSE CAST(3 AS NUMERIC)
                END
            ) = 0.5 THEN '3. Other'

            WHEN (
                CASE
                    WHEN q8 IS NOT NULL AND q8 != '*'
                        THEN SAFE_CAST(q8_marks AS NUMERIC)
                    ELSE CAST(3 AS NUMERIC)
                END
            ) = 0 THEN '2. No'

            ELSE '4. DNA'
        END AS q8_bucket

    FROM t2
)

SELECT *
FROM t3
