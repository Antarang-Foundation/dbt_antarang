WITH base AS (
  SELECT student_id, student_barcode, gender, batch_no, batch_academic_year, batch_language, facilitator_id, facilitator_name, 
  facilitator_email, school_id, school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, 
  donor_id, batch_donor, batch_grade, assessment_barcode, bl_CreatedDate, bl_cs_no, bl_q4_1, bl_q4_1_marks, bl_q4_2, bl_q4_2_marks, 
  bl_q4_3, bl_q4_3_marks, bl_q4_4, bl_q4_4_marks, bl_q4_5, bl_q4_5_marks, bl_q4_6, bl_q4_6_marks, bl_q4_7, bl_q4_7_marks, bl_q4_8, 
  bl_q4_8_marks, bl_q4_9, bl_q4_9_marks, bl_q4_10, bl_q4_10_marks, bl_q4_marks, bl_q5_1, bl_q5_1_marks, bl_q5_2, bl_q5_2_marks, 
  bl_q5_3, bl_q5_3_marks, bl_q5_4, bl_q5_4_marks, bl_q5_5, bl_q5_5_marks, bl_q5_6, bl_q5_6_marks, bl_q5_7, bl_q5_7_marks, bl_q5_8, 
  bl_q5_8_marks, bl_q5_9, bl_q5_9_marks, bl_q5_marks, el_CreatedDate, el_cs_no, el_q4_1, el_q4_1_marks, el_q4_2, el_q4_2_marks, 
  el_q4_3, el_q4_3_marks, el_q4_4, el_q4_4_marks, el_q4_5, el_q4_5_marks, el_q4_6, el_q4_6_marks, el_q4_7, el_q4_7_marks, el_q4_8, 
  el_q4_8_marks, el_q4_9, el_q4_9_marks, el_q4_10, el_q4_10_marks, el_q4_marks, el_q5_1, el_q5_1_marks, el_q5_2, el_q5_2_marks, 
  el_q5_3, el_q5_3_marks, el_q5_4, el_q5_4_marks, el_q5_5, el_q5_5_marks, el_q5_6, el_q5_6_marks, el_q5_7, el_q5_7_marks, el_q5_8, 
  el_q5_8_marks, el_q5_9, el_q5_9_marks, el_q5_marks
  FROM {{ ref('int_student_global_cs') }}
),

bl_1 AS (SELECT base.*,
    -- BL Q4 total
    (
      IFNULL(SAFE_CAST(bl_q4_1 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_2 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_3 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_4 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_5 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_6 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_7 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_8 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_9 AS INT64),0) +
      IFNULL(SAFE_CAST(bl_q4_10 AS INT64),0)
    ) AS bl_q4_total_marks,

    SAFE_DIVIDE(
    IFNULL(SAFE_CAST(bl_q4_2_marks AS INT64),0) +
    IFNULL(SAFE_CAST(bl_q4_8_marks AS INT64),0) +
    IFNULL(SAFE_CAST(bl_q4_9_marks AS INT64),0),
    3
) AS bl_direct_learning,

    SAFE_DIVIDE(
    IFNULL(SAFE_CAST(bl_q4_1_marks AS INT64),0) +
    IFNULL(SAFE_CAST(bl_q4_7_marks AS INT64),0) +
    IFNULL(SAFE_CAST(bl_q4_10_marks AS INT64),0),
    3
) AS bl_observational_learning,

    IFNULL(SAFE_CAST(bl_q4_5_marks AS INT64),0) AS bl_self_assessment_based_learning,

    (
    SAFE_DIVIDE(
        IFNULL(SAFE_CAST(bl_q4_2_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_8_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_9_marks AS INT64),0),
        3
    ) * 0.5
)
+
(
    SAFE_DIVIDE(
        IFNULL(SAFE_CAST(bl_q4_1_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_7_marks AS INT64),0) +
        IFNULL(SAFE_CAST(bl_q4_10_marks AS INT64),0),
        3
    ) * 0.3
)
+
(
    IFNULL(SAFE_CAST(bl_q4_5_marks AS INT64),0) * 0.2
) AS bl_q4_overall_score
    FROM base
),
bl AS (
  SELECT
    bl_1.*,
    CASE
    WHEN bl_q4_total_marks = 0 THEN '0 Experiential Opportunities'
    WHEN bl_q4_total_marks BETWEEN 1 AND 2 THEN '1-2 Experiential Opportunities'
    WHEN bl_q4_total_marks BETWEEN 3 AND 4 THEN '3-4 Experiential Opportunities'
    WHEN bl_q4_total_marks BETWEEN 5 AND 7 THEN '5-7 Experiential Opportunities'
END AS bl_q4_marks_bucket,
    

    CASE
            WHEN bl_q4_overall_score BETWEEN 0 AND 0.20 THEN 'Pre-Exposure Stage'
            WHEN bl_q4_overall_score > 0.20 AND bl_q4_overall_score <= 0.40 THEN 'Observational Learning Stage'
            WHEN bl_q4_overall_score > 0.40 AND bl_q4_overall_score <= 0.60 THEN 'Self-Appraisal / Early Exploration'
            WHEN bl_q4_overall_score > 0.60 AND bl_q4_overall_score <= 0.80 THEN 'Developing Self-Efficacy'
            WHEN bl_q4_overall_score > 0.80 THEN 'Goal-Directed Action Stage'
        END AS bl_q4_bucket

  FROM bl_1
),

el_1 AS (
    SELECT
        bl.*,

        (
            IFNULL(SAFE_CAST(el_q4_1 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_2 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_3 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_4 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_5 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_6 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_7 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_8 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_9 AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_10 AS INT64),0)
        ) AS el_q4_total_marks,

        SAFE_DIVIDE(
            IFNULL(SAFE_CAST(el_q4_2_marks AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_8_marks AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_9_marks AS INT64),0),
            3
        ) AS el_direct_learning,

        SAFE_DIVIDE(
            IFNULL(SAFE_CAST(el_q4_1_marks AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_7_marks AS INT64),0) +
            IFNULL(SAFE_CAST(el_q4_10_marks AS INT64),0),
            3
        ) AS el_observational_learning,

        IFNULL(SAFE_CAST(el_q4_5_marks AS INT64),0) AS el_self_assessment_based_learning,

        (
            SAFE_DIVIDE(
                IFNULL(SAFE_CAST(el_q4_2_marks AS INT64),0) +
                IFNULL(SAFE_CAST(el_q4_8_marks AS INT64),0) +
                IFNULL(SAFE_CAST(el_q4_9_marks AS INT64),0),
                3
            ) * 0.5
        )
        +
        (
            SAFE_DIVIDE(
                IFNULL(SAFE_CAST(el_q4_1_marks AS INT64),0) +
                IFNULL(SAFE_CAST(el_q4_7_marks AS INT64),0) +
                IFNULL(SAFE_CAST(el_q4_10_marks AS INT64),0),
                3
            ) * 0.3
        )
        +
        (
            IFNULL(SAFE_CAST(el_q4_5_marks AS INT64),0) * 0.2
        ) AS el_q4_overall_score

    FROM bl
),

el AS (
    SELECT
        el_1.*,

        CASE
            WHEN el_q4_total_marks = 0 THEN '0 Experiential Opportunities'
            WHEN el_q4_total_marks BETWEEN 1 AND 2 THEN '1-2 Experiential Opportunities'
            WHEN el_q4_total_marks BETWEEN 3 AND 4 THEN '3-4 Experiential Opportunities'
            WHEN el_q4_total_marks BETWEEN 5 AND 7 THEN '5-7 Experiential Opportunities'
        END AS el_q4_marks_bucket,

        CASE
            WHEN el_q4_overall_score BETWEEN 0 AND 0.20 THEN 'Pre-Exposure Stage'
            WHEN el_q4_overall_score > 0.20 AND el_q4_overall_score <= 0.40 THEN 'Observational Learning Stage'
            WHEN el_q4_overall_score > 0.40 AND el_q4_overall_score <= 0.60 THEN 'Self-Appraisal / Early Exploration'
            WHEN el_q4_overall_score > 0.60 AND el_q4_overall_score <= 0.80 THEN 'Developing Self-Efficacy'
            WHEN el_q4_overall_score > 0.80 THEN 'Goal-Directed Action Stage'
        END AS el_q4_bucket

    FROM el_1
)

SELECT *
FROM el