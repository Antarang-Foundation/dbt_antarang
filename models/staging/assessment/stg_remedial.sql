
WITH cdm1 AS (
    SELECT
        Name as el_no,
        Barcode__c AS assessment_barcode,
        CreatedDate AS el_CreatedDate,
        RecordTypeId AS record_type_id,

        Interest_1__c AS q1_1,
        Interest_2__c AS q1_2,
        Interest_Marks__c AS q1_marks,

        Aptitude_1__c AS q2_1,
        Aptitude_2__c AS q2_2,
        Aptitude_Marks__c AS q2_marks,

        Career_Choice_1__c AS q3_1,
        Career_Choice_1_Marks__c AS q3_1_marks,
        Career_Choice_2__c AS q3_2,
        Career_Choice_2_Marks__c AS q3_2_marks,
        Career_Choice_Total_Marks__c AS q3_marks,
                    CAST(Academic_Year__c as STRING) as assessment_academic_year

    FROM {{ source('salesforce', 'OMR_Assessment__c') }}
    WHERE IsDeleted = FALSE   AND SAFE_CAST(Academic_Year__c AS INT64) = 2026
),

cdm2 AS (
    SELECT
        Barcode__c AS assessment_barcode,

        Q5__c AS q4,
        X5_Confident_about_chosen_career__c AS q4_marks

    FROM {{ source('salesforce', 'CDM2__c') }}
    WHERE IsDeleted = FALSE   AND SAFE_CAST(Academic_Year__c AS INT64) = 2026
),

cp AS (
    SELECT
        Barcode__c AS assessment_barcode,

        Q_8__c AS q5,
        Q_8_Marks__c AS q5_marks,

        Q_9_1__c AS q6_1,
        Q_9_1_Marks__c AS q6_1_marks,
        Q_9_2__c AS q6_2,
        Q_9_2_Marks__c AS q6_2_marks,
        Q_9_3__c AS q6_3,
        Q_9_3_Marks__c AS q6_3_marks,
        Q_9_4__c AS q6_4,
        Q_9_4_Marks__c AS q6_4_marks,
        Q_9_5__c AS q6_5,
        Q_9_5_Marks__c AS q6_5_marks,
        Q_9_6__c AS q6_6,
        Q_9_6_Marks__c AS q6_6_marks,
        Q_9_7__c AS q6_7,
        Q_9_7_Marks__c AS q6_7_marks,
        Q_9_8__c AS q6_8,
        Q_9_8_Marks__c AS q6_8_marks,
        Q_9_Marks__c AS q6_marks,

        Q_10__c AS q7,
        Q_10_Marks__c AS q7_marks

    FROM {{ source('salesforce', 'Career_Planning__c') }}
    WHERE IsDeleted = FALSE   AND SAFE_CAST(Academic_Year__c AS INT64) = 2026
),

cs AS (
    SELECT
        Barcode__c AS assessment_barcode,

        Q_11_1__c AS q8_1,
        Q_11_2__c AS q8_2,
        Q_11_3__c AS q8_3,
        Q_11_4__c AS q8_4,
        Q_11_5__c AS q8_5,
        Q_11_6__c AS q8_6,
        Q_11_7__c AS q8_7,
        Q_11_8__c AS q8_8,
        Q_11_9__c AS q8_9,
        Q_11_10__c AS q8_10,
        Q_11_Ans__c AS q8_marks

    FROM {{ source('salesforce', 'Career_Skill__c') }}
    WHERE IsDeleted = FALSE   AND SAFE_CAST(Academic_Year__c AS INT64) = 2026
),

fpp AS (
    SELECT
        Barcode__c AS assessment_barcode,

        Q_18_1__c AS q9_1,
        Q_18_2__c AS q9_2,
        Q_18_3__c AS q9_3,
        Q_18_4__c AS q9_4,
        Q_18_5__c AS q9_5,
        Q_18_6__c AS q9_6,
        Q_18_7__c AS q9_7,
        Q_18_8__c AS q9_8,
        Q_18_9__c AS q9_9,
        Q_18_10__c AS q9_10,
        Q_18_11__c AS q9_11,
        Q_18_Ans__c AS q9,

        Q_21__c AS q10,
        Q_21_Ans__c AS q10_marks,

        Q_22__c AS q11,
        Q_22_Ans__c AS q11_marks

    FROM {{ source('salesforce', 'Future_Planning__c') }}
    WHERE IsDeleted = FALSE   AND SAFE_CAST(Academic_Year__c AS INT64) = 2026
),

t2 AS (
    SELECT
        record_type_id,
        record_type
    FROM {{ ref('seed_recordtype') }}
)

SELECT
    cdm1.*,

    cdm2.q4,
    cdm2.q4_marks,

    cp.q5,
    cp.q5_marks,
    cp.q6_1,
    cp.q6_1_marks,
    cp.q6_2,
    cp.q6_2_marks,
    cp.q6_3,
    cp.q6_3_marks,
    cp.q6_4,
    cp.q6_4_marks,
    cp.q6_5,
    cp.q6_5_marks,
    cp.q6_6,
    cp.q6_6_marks,
    cp.q6_7,
    cp.q6_7_marks,
    cp.q6_8,
    cp.q6_8_marks,
    cp.q6_marks,
    cp.q7,
    cp.q7_marks,

    cs.q8_1,
    cs.q8_2,
    cs.q8_3,
    cs.q8_4,
    cs.q8_5,
    cs.q8_6,
    cs.q8_7,
    cs.q8_8,
    cs.q8_9,
    cs.q8_10,
    cs.q8_marks,

    fpp.q9_1,
    fpp.q9_2,
    fpp.q9_3,
    fpp.q9_4,
    fpp.q9_5,
    fpp.q9_6,
    fpp.q9_7,
    fpp.q9_8,
    fpp.q9_9,
    fpp.q9_10,
    fpp.q9_11,
    fpp.q9,
    fpp.q10,
    fpp.q10_marks,
    fpp.q11,
    fpp.q11_marks,

    t2.record_type

FROM cdm1

LEFT JOIN cdm2
    USING (assessment_barcode)

LEFT JOIN cp
    USING (assessment_barcode)

LEFT JOIN cs
    USING (assessment_barcode)

LEFT JOIN fpp
    USING (assessment_barcode)

LEFT JOIN t2
    USING (record_type_id)

WHERE LOWER(t2.record_type) = 'remedial'
