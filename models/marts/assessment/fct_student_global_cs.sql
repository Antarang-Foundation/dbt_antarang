WITH base AS (
    SELECT *
    FROM {{ ref('int_score_cs') }}
),

/* =========================
   BL STEP 1 - CALCULATIONS
========================= */
bl_1 AS (

    SELECT
        base.*,
        CASE
    WHEN bl_q5_1_marks IS NULL
     AND bl_q5_2_marks IS NULL
     AND bl_q5_3_marks IS NULL
     AND bl_q5_4_marks IS NULL
     AND bl_q5_5_marks IS NULL
     AND bl_q5_6_marks IS NULL
     AND bl_q5_7_marks IS NULL
     AND bl_q5_8_marks IS NULL
     AND bl_q5_9_marks IS NULL
    THEN NULL
    ELSE ROUND(
        SAFE_DIVIDE(
            IFNULL(SAFE_CAST(bl_q5_1_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(bl_q5_2_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(bl_q5_3_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(bl_q5_4_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(bl_q5_5_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(bl_q5_6_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(bl_q5_7_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(bl_q5_8_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(bl_q5_9_marks AS FLOAT64),0),
            9
        ),
        2
    )
END AS bl_q5_avg_marks,

        CASE
    WHEN bl_q5_1_marks IS NULL
     AND bl_q5_2_marks IS NULL
     AND bl_q5_3_marks IS NULL
     AND bl_q5_4_marks IS NULL
     AND bl_q5_5_marks IS NULL
     AND bl_q5_6_marks IS NULL
     AND bl_q5_7_marks IS NULL
     AND bl_q5_8_marks IS NULL
     AND bl_q5_9_marks IS NULL
    THEN NULL
    WHEN (
        IFNULL(SAFE_CAST(bl_q5_1_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_2_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_3_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_4_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_5_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_6_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_7_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_8_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q5_9_marks AS INT64),0)
    ) >= 2 THEN 1
    ELSE 0
END AS bl_q5_total_marks,

        CASE
    WHEN bl_q5_1 IS NULL
     AND bl_q5_2 IS NULL
     AND bl_q5_3 IS NULL
     AND bl_q5_4 IS NULL
     AND bl_q5_5 IS NULL
     AND bl_q5_6 IS NULL
     AND bl_q5_7 IS NULL
     AND bl_q5_8 IS NULL
     AND bl_q5_9 IS NULL
    THEN NULL
    ELSE SAFE_DIVIDE(
        (
            IFNULL(SAFE_CAST(bl_q5_1 AS INT64),0) +
            IFNULL(SAFE_CAST(bl_q5_2 AS INT64),0) +
            IFNULL(SAFE_CAST(bl_q5_3 AS INT64),0) +
            IFNULL(SAFE_CAST(bl_q5_4 AS INT64),0) +
            IFNULL(SAFE_CAST(bl_q5_5 AS INT64),0) +
            IFNULL(SAFE_CAST(bl_q5_6 AS INT64),0) +
            IFNULL(SAFE_CAST(bl_q5_7 AS INT64),0) +
            IFNULL(SAFE_CAST(bl_q5_8 AS INT64),0) +
            IFNULL(SAFE_CAST(bl_q5_9 AS INT64),0)
        ),
        9
    )
END AS bl_q5_overall_score,

    CASE
    WHEN bl_q5_1_marks IS NULL
     AND bl_q5_2_marks IS NULL
     AND bl_q5_5_marks IS NULL
     AND bl_q5_6_marks IS NULL
    THEN NULL
    ELSE SAFE_DIVIDE(
        IFNULL(SAFE_CAST(bl_q5_1_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(bl_q5_2_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(bl_q5_5_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(bl_q5_6_marks AS FLOAT64),0),
        4
    )
END AS bl_job_search_score,

        CASE
    WHEN bl_q5_3_marks IS NULL
     AND bl_q5_4_marks IS NULL
     AND bl_q5_8_marks IS NULL
    THEN NULL
    ELSE SAFE_DIVIDE(
        IFNULL(SAFE_CAST(bl_q5_3_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(bl_q5_4_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(bl_q5_8_marks AS FLOAT64),0),
        3
    )
END AS bl_communication_score,

       CASE
    WHEN bl_q5_7_marks IS NULL
     AND bl_q5_9_marks IS NULL
    THEN NULL
    ELSE SAFE_DIVIDE(
        IFNULL(SAFE_CAST(bl_q5_7_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(bl_q5_9_marks AS FLOAT64),0),
        2
    )
END AS bl_cognitive_score

    FROM base
),

/* =========================
   BL STEP 2 - BUCKETS
========================= */
bl AS (

    SELECT
        bl_1.*,

        CASE
            WHEN bl_q5_total_marks = 0 THEN '0 CRS'
            WHEN bl_q5_total_marks BETWEEN 1 AND 4 THEN '1-4 CRS'
            WHEN bl_q5_total_marks BETWEEN 5 AND 7 THEN '5-7 CRS'
            WHEN bl_q5_total_marks BETWEEN 8 AND 9 THEN '8-9 CRS'
            ELSE NULL
        END AS bl_q5_marks_bucket,

        CASE
            WHEN bl_job_search_score BETWEEN 1 AND 1.75 THEN 'Novice'
            WHEN bl_job_search_score > 1.75 AND bl_job_search_score <= 2.50 THEN 'Advanced Beginner'
            WHEN bl_job_search_score > 2.50 AND bl_job_search_score <= 3.25 THEN 'Competent'
            WHEN bl_job_search_score > 3.25 THEN 'Proficient'
            ELSE NULL
        END AS bl_job_search_level,

        CASE
            WHEN bl_communication_score BETWEEN 1 AND 1.75 THEN 'Novice'
            WHEN bl_communication_score > 1.75 AND bl_communication_score <= 2.50 THEN 'Advanced Beginner'
            WHEN bl_communication_score > 2.50 AND bl_communication_score <= 3.25 THEN 'Competent'
            WHEN bl_communication_score > 3.25 THEN 'Proficient'
            ELSE NULL
        END AS bl_communication_level,

        CASE
            WHEN bl_cognitive_score BETWEEN 1 AND 1.75 THEN 'Novice'
            WHEN bl_cognitive_score > 1.75 AND bl_cognitive_score <= 2.50 THEN 'Advanced Beginner'
            WHEN bl_cognitive_score > 2.50 AND bl_cognitive_score <= 3.25 THEN 'Competent'
            WHEN bl_cognitive_score > 3.25 THEN 'Proficient'
            ELSE NULL
        END AS bl_cognitive_level,

        CASE
            WHEN bl_q5_overall_score BETWEEN 1 AND 1.75 THEN 'Novice'
            WHEN bl_q5_overall_score > 1.75 AND bl_q5_overall_score <= 2.50 THEN 'Advanced Beginner'
            WHEN bl_q5_overall_score > 2.50 AND bl_q5_overall_score <= 3.25 THEN 'Competent'
            WHEN bl_q5_overall_score > 3.25 THEN 'Proficient'
            ELSE NULL
        END AS bl_q5_bucket

    FROM bl_1
),

el_1 AS (

    SELECT
        bl.*,
        CASE
    WHEN el_q5_1_marks IS NULL
     AND el_q5_2_marks IS NULL
     AND el_q5_3_marks IS NULL
     AND el_q5_4_marks IS NULL
     AND el_q5_5_marks IS NULL
     AND el_q5_6_marks IS NULL
     AND el_q5_7_marks IS NULL
     AND el_q5_8_marks IS NULL
     AND el_q5_9_marks IS NULL
    THEN NULL
    ELSE ROUND(
        SAFE_DIVIDE(
            IFNULL(SAFE_CAST(el_q5_1_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(el_q5_2_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(el_q5_3_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(el_q5_4_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(el_q5_5_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(el_q5_6_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(el_q5_7_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(el_q5_8_marks AS FLOAT64),0) +
            IFNULL(SAFE_CAST(el_q5_9_marks AS FLOAT64),0),
            9
        ),
        2
    )
END AS el_q5_avg_marks,

        CASE
    WHEN el_q5_1_marks IS NULL
     AND el_q5_2_marks IS NULL
     AND el_q5_3_marks IS NULL
     AND el_q5_4_marks IS NULL
     AND el_q5_5_marks IS NULL
     AND el_q5_6_marks IS NULL
     AND el_q5_7_marks IS NULL
     AND el_q5_8_marks IS NULL
     AND el_q5_9_marks IS NULL
    THEN NULL

    WHEN
        IF(SAFE_CAST(el_q5_1_marks AS INT64) >= 2, 1, 0) +
        IF(SAFE_CAST(el_q5_2_marks AS INT64) >= 2, 1, 0) +
        IF(SAFE_CAST(el_q5_3_marks AS INT64) >= 2, 1, 0) +
        IF(SAFE_CAST(el_q5_4_marks AS INT64) >= 2, 1, 0) +
        IF(SAFE_CAST(el_q5_5_marks AS INT64) >= 2, 1, 0) +
        IF(SAFE_CAST(el_q5_6_marks AS INT64) >= 2, 1, 0) +
        IF(SAFE_CAST(el_q5_7_marks AS INT64) >= 2, 1, 0) +
        IF(SAFE_CAST(el_q5_8_marks AS INT64) >= 2, 1, 0) +
        IF(SAFE_CAST(el_q5_9_marks AS INT64) >= 2, 1, 0) >= 1
    THEN 1

    ELSE 0
END AS el_q5_total_marks,

        CASE
    WHEN el_q5_1 IS NULL
     AND el_q5_2 IS NULL
     AND el_q5_3 IS NULL
     AND el_q5_4 IS NULL
     AND el_q5_5 IS NULL
     AND el_q5_6 IS NULL
     AND el_q5_7 IS NULL
     AND el_q5_8 IS NULL
     AND el_q5_9 IS NULL
    THEN NULL
    ELSE SAFE_DIVIDE(
        (
            IFNULL(SAFE_CAST(el_q5_1 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q5_2 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q5_3 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q5_4 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q5_5 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q5_6 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q5_7 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q5_8 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q5_9 AS INT64),0)
        ),
        9
    )
END AS el_q5_overall_score,

        CASE
    WHEN el_q5_1_marks IS NULL
     AND el_q5_2_marks IS NULL
     AND el_q5_5_marks IS NULL
     AND el_q5_6_marks IS NULL
    THEN NULL
    ELSE SAFE_DIVIDE(
        IFNULL(SAFE_CAST(el_q5_1_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(el_q5_2_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(el_q5_5_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(el_q5_6_marks AS FLOAT64),0),
        4
    )
END AS el_job_search_score,

CASE
    WHEN el_q5_3_marks IS NULL
     AND el_q5_4_marks IS NULL
     AND el_q5_8_marks IS NULL
    THEN NULL
    ELSE SAFE_DIVIDE(
        IFNULL(SAFE_CAST(el_q5_3_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(el_q5_4_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(el_q5_8_marks AS FLOAT64),0),
        3
    )
END AS el_communication_score,

        CASE
    WHEN el_q5_7_marks IS NULL
     AND el_q5_9_marks IS NULL
    THEN NULL
    ELSE SAFE_DIVIDE(
        IFNULL(SAFE_CAST(el_q5_7_marks AS FLOAT64),0) +
        IFNULL(SAFE_CAST(el_q5_9_marks AS FLOAT64),0),
        2
    )
END AS el_cognitive_score

    FROM bl
),

/* =========================
   EL CALCULATIONS
========================= */
el AS (

    SELECT
        el_1.*,

        CASE
            WHEN el_q5_total_marks = 0 THEN '0 CRS'
            WHEN el_q5_total_marks BETWEEN 1 AND 4 THEN '1-4 CRS'
            WHEN el_q5_total_marks BETWEEN 5 AND 7 THEN '5-7 CRS'
            WHEN el_q5_total_marks BETWEEN 8 AND 9 THEN '8-9 CRS'
            ELSE NULL
        END AS el_q5_marks_bucket,

        CASE
            WHEN el_job_search_score BETWEEN 1 AND 1.75 THEN 'Novice'
            WHEN el_job_search_score > 1.75 AND el_job_search_score <= 2.50 THEN 'Advanced Beginner'
            WHEN el_job_search_score > 2.50 AND el_job_search_score <= 3.25 THEN 'Competent'
            WHEN el_job_search_score > 3.25 THEN 'Proficient'
            ELSE NULL
        END AS el_job_search_level,

        CASE
            WHEN el_communication_score BETWEEN 1 AND 1.75 THEN 'Novice'
            WHEN el_communication_score > 1.75 AND el_communication_score <= 2.50 THEN 'Advanced Beginner'
            WHEN el_communication_score > 2.50 AND el_communication_score <= 3.25 THEN 'Competent'
            WHEN el_communication_score > 3.25 THEN 'Proficient'
            ELSE NULL
        END AS el_communication_level,

        CASE
            WHEN el_cognitive_score BETWEEN 1 AND 1.75 THEN 'Novice'
            WHEN el_cognitive_score > 1.75 AND el_cognitive_score <= 2.50 THEN 'Advanced Beginner'
            WHEN el_cognitive_score > 2.50 AND el_cognitive_score <= 3.25 THEN 'Competent'
            WHEN el_cognitive_score > 3.25 THEN 'Proficient'
            ELSE NULL
        END AS el_cognitive_level,

        CASE
            WHEN el_q5_overall_score BETWEEN 1 AND 1.75 THEN 'Novice'
            WHEN el_q5_overall_score > 1.75 AND el_q5_overall_score <= 2.50 THEN 'Advanced Beginner'
            WHEN el_q5_overall_score > 2.50 AND el_q5_overall_score <= 3.25 THEN 'Competent'
            WHEN el_q5_overall_score > 3.25 THEN 'Proficient'
            ELSE NULL
        END AS el_q5_bucket

    FROM el_1
),

final_select as (SELECT
    el.*,

    /* =========================
       BL vs EL DELTA
    ========================= */

    (el_q4_total_marks - bl_q4_total_marks) AS bl_el_q4_score,

    (el_q5_total_marks - bl_q5_total_marks) AS bl_el_q5_score
  

FROM el
),

final as (SELECT student_id, student_barcode, gender, batch_no, batch_academic_year, batch_language, facilitator_id, 
facilitator_name, facilitator_email, school_id, school_name, school_taluka, school_ward, school_district, school_state, 
school_partner, school_area, donor_id, batch_donor, batch_grade, assessment_barcode, bl_CreatedDate, bl_cs_no, bl_q4_1, 
bl_q4_1_marks, bl_q4_2, bl_q4_2_marks, bl_q4_3, bl_q4_3_marks, bl_q4_4, bl_q4_4_marks, bl_q4_5, bl_q4_5_marks, bl_q4_6, 
bl_q4_6_marks, bl_q4_7, bl_q4_7_marks, bl_q4_8, bl_q4_8_marks, bl_q4_9, bl_q4_9_marks, bl_q4_10, bl_q4_10_marks, bl_q4_total_marks, 
bl_q4_marks_bucket, bl_direct_learning, bl_observational_learning, bl_self_assessment_based_learning, bl_q4_overall_score, 
bl_q4_bucket, bl_q5_1, bl_q5_1_marks, bl_q5_2, bl_q5_2_marks, bl_q5_3, bl_q5_3_marks, bl_q5_4, bl_q5_4_marks, bl_q5_5, 
bl_q5_5_marks, bl_q5_6, bl_q5_6_marks, bl_q5_7, bl_q5_7_marks, bl_q5_8, bl_q5_8_marks, bl_q5_9, bl_q5_9_marks, bl_q5_avg_marks, 
bl_q5_total_marks, bl_q5_marks_bucket, bl_job_search_score, bl_communication_score, bl_cognitive_score, bl_job_search_level, 
bl_communication_level, bl_cognitive_level, bl_q5_overall_score, bl_q5_bucket, el_CreatedDate, el_cs_no, el_q4_1, el_q4_1_marks, 
el_q4_2, el_q4_2_marks, el_q4_3, el_q4_3_marks, el_q4_4, el_q4_4_marks, el_q4_5, el_q4_5_marks, el_q4_6, el_q4_6_marks, el_q4_7, 
el_q4_7_marks, el_q4_8, el_q4_8_marks, el_q4_9, el_q4_9_marks, el_q4_10, el_q4_10_marks, el_q4_total_marks, el_q4_marks_bucket, 
el_direct_learning, el_observational_learning, el_self_assessment_based_learning, el_q4_overall_score, el_q4_bucket, el_q5_1, 
el_q5_1_marks, el_q5_2, el_q5_2_marks, el_q5_3, el_q5_3_marks, el_q5_4, el_q5_4_marks, el_q5_5, el_q5_5_marks, el_q5_6, 
el_q5_6_marks, el_q5_7, el_q5_7_marks, el_q5_8, el_q5_8_marks, el_q5_9, el_q5_9_marks, el_q5_avg_marks, el_q5_total_marks, 
el_q5_marks_bucket, el_job_search_score, el_communication_score, el_cognitive_score, el_job_search_level, el_communication_level, 
el_cognitive_level, el_q5_overall_score, el_q5_bucket, bl_el_q4_score, bl_el_q5_score
FROM final_select
)

select * from final
