
WITH source AS (
    SELECT
        Id AS fp_id,
        Barcode__c AS assessment_barcode,
        RecordTypeId AS record_type_id,
        CreatedDate AS created_on,
        Created_from_Form__c AS created_from_form,
        Name AS fp_no,
        Grade__c AS assessment_grade,
        CAST(Academic_Year__c AS STRING) AS assessment_academic_year,
        Batch_Id__c AS assessment_batch_id,

        Q_17__c AS q17,
        Q_17_Ans__c AS q17_marks,

        Q_18_1__c AS q18_1,
        Q_18_2__c AS q18_2,
        Q_18_3__c AS q18_3,
        Q_18_4__c AS q18_4,
        Q_18_5__c AS q18_5,
        Q_18_6__c AS q18_6,
        Q_18_7__c AS q18_7,
        Q_18_8__c AS q18_8,
        Q_18_9__c AS q18_9,
        Q_18_10__c AS q18_10,
        Q_18_11__c AS q18_11,
        Q_18_Ans__c AS q18_marks,

        Q_19__c AS q19,
        Q_19_Ans__c AS q19_marks,

        Q_20__c AS q20,
        Q_20_Ans__c AS q20_marks,

        Q_21__c AS q21,
        Q_21_Ans__c AS q21_marks,

        Q_22__c AS q22,
        Q_22_Ans__c AS q22_marks,

        (
            COALESCE(SAFE_CAST(Q_17_Ans__c AS NUMERIC), 0)
            + COALESCE(SAFE_CAST(Q_18_Ans__c AS NUMERIC), 0)
            + COALESCE(SAFE_CAST(Q_19_Ans__c AS NUMERIC), 0)
            + COALESCE(SAFE_CAST(Q_20_Ans__c AS NUMERIC), 0)
            + COALESCE(SAFE_CAST(Q_21_Ans__c AS NUMERIC), 0)
            + COALESCE(SAFE_CAST(Q_22_Ans__c AS NUMERIC), 0)
        ) AS fp_total_marks,

        F_1__c AS f1,
        F_2__c AS f2,
        F_3__c AS f3,
        F_4__c AS f4,
        F_5__c AS f5,
        F_6__c AS f6,
        F_7__c AS f7,
        F_8__c AS f8,
        F_9__c AS f9,
        F_10__c AS f10,
        F_11__c AS f11,
        F_12__c AS f12,

        Error_Status__c AS error_status,
        Data_Clean_up__c AS data_cleanup,
        Marks_Recalculated__c AS marks_recalculated,
        Student_Linked__c AS student_linked

    FROM {{ source('salesforce', 'Future_Planning__c') }}
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
        fp_id,
        assessment_barcode,
        record_type,
        created_on,
        created_from_form,
        fp_no,
        assessment_grade,
        assessment_academic_year,
        assessment_batch_id,

        CASE
            WHEN fp_no IS NOT NULL
                AND (
                    q17 IS NOT NULL
                    OR q18_1 IS NOT NULL
                    OR q18_2 IS NOT NULL
                    OR q18_3 IS NOT NULL
                    OR q18_4 IS NOT NULL
                    OR q18_5 IS NOT NULL
                    OR q18_6 IS NOT NULL
                    OR q18_7 IS NOT NULL
                    OR q18_8 IS NOT NULL
                    OR q18_9 IS NOT NULL
                    OR q18_10 IS NOT NULL
                    OR q18_11 IS NOT NULL
                    OR q19 IS NOT NULL
                    OR q20 IS NOT NULL
                    OR q21 IS NOT NULL
                    OR q22 IS NOT NULL
                    OR f1 IS NOT NULL
                    OR f2 IS NOT NULL
                    OR f3 IS NOT NULL
                    OR f4 IS NOT NULL
                    OR f5 IS NOT NULL
                    OR f6 IS NOT NULL
                    OR f7 IS NOT NULL
                    OR f8 IS NOT NULL
                    OR f9 IS NOT NULL
                    OR f10 IS NOT NULL
                    OR f11 IS NOT NULL
                    OR f12 IS NOT NULL
                )
                THEN 1

            WHEN fp_no IS NOT NULL
                AND (
                    q17 IS NULL
                    AND q18_1 IS NULL
                    AND q18_2 IS NULL
                    AND q18_3 IS NULL
                    AND q18_4 IS NULL
                    AND q18_5 IS NULL
                    AND q18_6 IS NULL
                    AND q18_7 IS NULL
                    AND q18_8 IS NULL
                    AND q18_9 IS NULL
                    AND q18_10 IS NULL
                    AND q18_11 IS NULL
                    AND q19 IS NULL
                    AND q20 IS NULL
                    AND q21 IS NULL
                    AND q22 IS NULL
                    AND f1 IS NULL
                    AND f2 IS NULL
                    AND f3 IS NULL
                    AND f4 IS NULL
                    AND f5 IS NULL
                    AND f6 IS NULL
                    AND f7 IS NULL
                    AND f8 IS NULL
                    AND f9 IS NULL
                    AND f10 IS NULL
                    AND f11 IS NULL
                    AND f12 IS NULL
                )
                THEN 0

            ELSE NULL
        END AS is_non_null,

        q17,
        q18_1,
        q18_2,
        q18_3,
        q18_4,
        q18_5,
        q18_6,
        q18_7,
        q18_8,
        q18_9,
        q18_10,
        q18_11,
        q19,
        q20,
        q21,
        q22,

        f1,
        f2,
        f3,
        f4,
        f5,
        f6,
        f7,
        f8,
        f9,
        f10,
        f11,
        f12,

        q17_marks,
        q18_marks,
        q19_marks,
        q20_marks,
        q21_marks,
        q22_marks,
        fp_total_marks,
        error_status,
        data_cleanup,
        marks_recalculated,
        student_linked

    FROM joined_source
),

t3 AS (
    SELECT
        t2.*,

        CASE
            WHEN q21 IS NOT NULL AND q21 != '*'
                THEN SAFE_CAST(q21_marks AS FLOAT64)
            ELSE CAST(3 AS FLOAT64)
        END AS q21_null,

        CASE
            WHEN SAFE_CAST(q21_marks AS FLOAT64) = 1
                THEN '1. Yes'
            WHEN SAFE_CAST(q21_marks AS FLOAT64) = 0.5
                THEN '3. Other'
            WHEN SAFE_CAST(q21_marks AS FLOAT64) = 0
                THEN '2. No'
            WHEN q21 IS NULL OR q21 = '*'
                THEN '4. DNA'
            ELSE NULL
        END AS q21_bucket

    FROM t2
)

SELECT *
FROM t3
