WITH base AS (

    SELECT *
    FROM {{ ref('int_student_diagnostic') }}

),

student_diag AS (SELECT

    base.*,

    ---------------------------------------------------
    -- BASELINE AWARENESS LEVEL
    ---------------------------------------------------

    CASE

        WHEN batch_grade IN ('Grade 9','Grade 10') THEN
            CASE
                WHEN bl_mean_score > 2 AND bl_mean_score <= 3
                    THEN 'Emerging Awareness'
                WHEN bl_mean_score > 1 AND bl_mean_score <= 2
                    THEN 'Developing Awareness'
                WHEN bl_mean_score >= 0 AND bl_mean_score <= 1
                    THEN 'Low awareness'
                ELSE 'DNA'
            END

        WHEN batch_grade IN ('Grade 11','Grade 12') THEN
            CASE
                WHEN bl_mean_score > 4 AND bl_mean_score <= 5
                    THEN 'High Readiness'
                WHEN bl_mean_score > 3 AND bl_mean_score <= 4
                    THEN 'Emerging Readiness'
                WHEN bl_mean_score > 2 AND bl_mean_score <= 3
                    THEN 'Developing Readiness'
                WHEN bl_mean_score >= 1 AND bl_mean_score <= 2
                    THEN 'Low Readiness'
                ELSE 'DNA'
            END

    END AS bl_awareness_level,

    ---------------------------------------------------
    -- ENDLINE AWARENESS LEVEL
    ---------------------------------------------------

    CASE

        WHEN batch_grade IN ('Grade 9','Grade 10') THEN
            CASE
                WHEN el_mean_score > 2 AND el_mean_score <= 3
                    THEN 'Emerging Awareness'
                WHEN el_mean_score > 1 AND el_mean_score <= 2
                    THEN 'Developing Awareness'
                WHEN el_mean_score >= 0 AND el_mean_score <= 1
                    THEN 'Low awareness'
                ELSE 'DNA'
            END

        WHEN batch_grade IN ('Grade 11','Grade 12') THEN
            CASE
                WHEN el_mean_score > 4 AND el_mean_score <= 5
                    THEN 'High Readiness'
                WHEN el_mean_score > 3 AND el_mean_score <= 4
                    THEN 'Emerging Readiness'
                WHEN el_mean_score > 2 AND el_mean_score <= 3
                    THEN 'Developing Readiness'
                WHEN el_mean_score >= 1 AND el_mean_score <= 2
                    THEN 'Low Readiness'
                ELSE 'DNA'
            END

    END AS el_awareness_level,

    ---------------------------------------------------
    -- TYPE OF ASSESSMENT FILLED
    ---------------------------------------------------

    CASE
        WHEN bl_sd_no IS NOT NULL
         AND el_sd_no IS NOT NULL
            THEN 'Both'

        WHEN bl_sd_no IS NOT NULL
         AND el_sd_no IS NULL
            THEN 'Baseline'

        WHEN bl_sd_no IS NULL
         AND el_sd_no IS NOT NULL
            THEN 'Endline'

    END AS type_of_assessment_filled,

    ---------------------------------------------------
    -- QUESTION LEVEL CHANGE
    ---------------------------------------------------

    CASE
        WHEN bl_q1 IS NULL OR el_q1 IS NULL THEN NULL
        WHEN el_q1 > bl_q1 THEN 'Improvement'
        WHEN el_q1 < bl_q1 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q1,

    CASE
        WHEN bl_q2 IS NULL OR el_q2 IS NULL THEN NULL
        WHEN el_q2 > bl_q2 THEN 'Improvement'
        WHEN el_q2 < bl_q2 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q2,

    CASE
        WHEN bl_q3 IS NULL OR el_q3 IS NULL THEN NULL
        WHEN el_q3 > bl_q3 THEN 'Improvement'
        WHEN el_q3 < bl_q3 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q3,

    CASE
        WHEN bl_q4 IS NULL OR el_q4 IS NULL THEN NULL
        WHEN el_q4 > bl_q4 THEN 'Improvement'
        WHEN el_q4 < bl_q4 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q4,

    CASE
        WHEN bl_q5 IS NULL OR el_q5 IS NULL THEN NULL
        WHEN el_q5 > bl_q5 THEN 'Improvement'
        WHEN el_q5 < bl_q5 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q5,

    CASE
        WHEN bl_q6 IS NULL OR el_q6 IS NULL THEN NULL
        WHEN el_q6 > bl_q6 THEN 'Improvement'
        WHEN el_q6 < bl_q6 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q6,

    CASE
        WHEN bl_q7 IS NULL OR el_q7 IS NULL THEN NULL
        WHEN el_q7 > bl_q7 THEN 'Improvement'
        WHEN el_q7 < bl_q7 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q7,

    CASE
        WHEN bl_q8 IS NULL OR el_q8 IS NULL THEN NULL
        WHEN el_q8 > bl_q8 THEN 'Improvement'
        WHEN el_q8 < bl_q8 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q8,

    CASE
        WHEN bl_q9 IS NULL OR el_q9 IS NULL THEN NULL
        WHEN el_q9 > bl_q9 THEN 'Improvement'
        WHEN el_q9 < bl_q9 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q9,

    CASE
        WHEN bl_q10 IS NULL OR el_q10 IS NULL THEN NULL
        WHEN el_q10 > bl_q10 THEN 'Improvement'
        WHEN el_q10 < bl_q10 THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_q10,

    ---------------------------------------------------
    -- MEAN SCORE CHANGE
    ---------------------------------------------------

    CASE
        WHEN bl_mean_score IS NULL OR el_mean_score IS NULL THEN NULL
        WHEN el_mean_score > bl_mean_score THEN 'Improvement'
        WHEN el_mean_score < bl_mean_score THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_mean_score,

    ---------------------------------------------------
    -- SD SCORE CHANGE
    ---------------------------------------------------

    CASE
        WHEN bl_sd_score IS NULL OR el_sd_score IS NULL THEN NULL
        WHEN el_sd_score > bl_sd_score THEN 'Improvement'
        WHEN el_sd_score < bl_sd_score THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_sd_score

FROM base
),

final AS (SELECT student_id, student_barcode, gender, batch_no, batch_academic_year, batch_language, facilitator_id, 
facilitator_name, facilitator_email, school_id, school_name, school_taluka, school_ward, school_district, school_state, 
school_partner, school_area, donor_id, batch_donor, batch_grade, assessment_barcode, bl_created_by_date, 
bl_sd_no, bl_q1, bl_q1_bucket, bl_q2, bl_q2_bucket, bl_q3, bl_q3_bucket, bl_q4, bl_q4_bucket, bl_q5, bl_q5_bucket, bl_q6, 
bl_q6_bucket, bl_q7, bl_q7_bucket, bl_q8, bl_q8_bucket, bl_q9, bl_q9_bucket, bl_q10, bl_q10_bucket, bl_mean_score, 
bl_sd_score, bl_awareness_level, el_created_by_date, el_sd_no, el_q1, el_q1_bucket, el_q2, el_q2_bucket, el_q3, el_q3_bucket, 
el_q4, el_q4_bucket, el_q5, el_q5_bucket, el_q6, el_q6_bucket, el_q7, el_q7_bucket, el_q8, el_q8_bucket, el_q9, el_q9_bucket, 
el_q10, el_q10_bucket, el_mean_score, el_sd_score, el_awareness_level, type_of_assessment_filled, bl_el_q1, bl_el_q2, bl_el_q3, 
bl_el_q4, bl_el_q5, bl_el_q6, bl_el_q7, bl_el_q8, bl_el_q9, bl_el_q10, bl_el_mean_score, bl_el_sd_score FROM student_diag
)

SELECT * FROM final