WITH base AS (

    SELECT *
    FROM {{ ref('int_score_cs') }}

),

final AS (

    SELECT
        base.*,

        /* =========================
           BL Q4
        ========================= */

        CASE
            WHEN bl_q4_total_marks IS NULL THEN 'DNA'
            WHEN bl_q4_total_marks = 0 THEN '0 Experiential Opportunities'
            WHEN bl_q4_total_marks BETWEEN 1 AND 2 THEN '1-2 Experiential Opportunities'
            WHEN bl_q4_total_marks BETWEEN 3 AND 4 THEN '3-4 Experiential Opportunities'
            WHEN bl_q4_total_marks BETWEEN 5 AND 7 THEN '5-7 Experiential Opportunities'
        END AS bl_q4_marks_bucket,

        CASE
            WHEN bl_q4_overall_score BETWEEN 0.00 AND 0.20 THEN 'Pre-Exposure Stage'
            WHEN bl_q4_overall_score > 0.20 AND bl_q4_overall_score <= 0.40 THEN 'Observational Learning Stage'
            WHEN bl_q4_overall_score > 0.40 AND bl_q4_overall_score <= 0.60 THEN 'Self-Appraisal / Early Exploration'
            WHEN bl_q4_overall_score > 0.60 AND bl_q4_overall_score <= 0.80 THEN 'Developing Self-Efficacy'
            WHEN bl_q4_overall_score > 0.80 THEN 'Goal-Directed Action Stage'
        END AS bl_q4_bucket,

        /* =========================
           BL Q5
        ========================= */

        ROUND(
            (
                bl_job_search_score +
                bl_communication_score +
                bl_cognitive_score
            ) / 3
        , 9) AS bl_q5_overall_score,

        CASE
            WHEN bl_q5_total_marks IS NULL THEN 'DNA'
            WHEN bl_q5_total_marks = 0 THEN '0 CRS'
            WHEN bl_q5_total_marks BETWEEN 1 AND 4 THEN '1-4 CRS'
            WHEN bl_q5_total_marks BETWEEN 5 AND 7 THEN '5-7 CRS'
            WHEN bl_q5_total_marks BETWEEN 8 AND 9 THEN '8-9 CRS'
        END AS bl_q5_marks_bucket,

        CASE
            WHEN bl_job_search_score BETWEEN 1.00 AND 1.75 THEN 'Novice'
            WHEN bl_job_search_score > 1.75 AND bl_job_search_score <= 2.50 THEN 'Advanced Beginner'
            WHEN bl_job_search_score > 2.50 AND bl_job_search_score <= 3.25 THEN 'Competent'
            WHEN bl_job_search_score > 3.25 THEN 'Proficient'
        END AS bl_job_search_level,

        CASE
            WHEN bl_communication_score BETWEEN 1.00 AND 1.75 THEN 'Novice'
            WHEN bl_communication_score > 1.75 AND bl_communication_score <= 2.50 THEN 'Advanced Beginner'
            WHEN bl_communication_score > 2.50 AND bl_communication_score <= 3.25 THEN 'Competent'
            WHEN bl_communication_score > 3.25 THEN 'Proficient'
        END AS bl_communication_level,

        CASE
            WHEN bl_cognitive_score BETWEEN 1.00 AND 1.75 THEN 'Novice'
            WHEN bl_cognitive_score > 1.75 AND bl_cognitive_score <= 2.50 THEN 'Advanced Beginner'
            WHEN bl_cognitive_score > 2.50 AND bl_cognitive_score <= 3.25 THEN 'Competent'
            WHEN bl_cognitive_score > 3.25 THEN 'Proficient'
        END AS bl_cognitive_level,

        /* =========================
           EL Q4
        ========================= */

        (
            COALESCE(SAFE_CAST(el_q4_1_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_2_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_3_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_4_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_5_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_6_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_7_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_8_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_9_marks AS INT64),0) +
            COALESCE(SAFE_CAST(el_q4_10_marks AS INT64),0)
        ) AS el_q4_total_marks,

        ROUND(
            ((el_direct_learning / 3) * 0.5) +
            ((el_observational_learning / 3) * 0.3) +
            (el_self_assessment_based_learning * 0.2)
        , 9) AS el_q4_overall_score,

        /* =========================
           EL Q5
        ========================= */

        ROUND(
            (
                el_job_search_score +
                el_communication_score +
                el_cognitive_score
            ) / 3
        , 9) AS el_q5_overall_score

    FROM base

),

final_buckets AS (

    SELECT
        *,

        CASE
            WHEN bl_q5_overall_score BETWEEN 1.00 AND 1.75 THEN 'Novice'
            WHEN bl_q5_overall_score > 1.75 AND bl_q5_overall_score <= 2.50 THEN 'Advanced Beginner'
            WHEN bl_q5_overall_score > 2.50 AND bl_q5_overall_score <= 3.25 THEN 'Competent'
            WHEN bl_q5_overall_score > 3.25 THEN 'Proficient'
        END AS bl_q5_bucket,

        CASE
            WHEN el_q4_total_marks IS NULL THEN 'DNA'
            WHEN el_q4_total_marks = 0 THEN '0 Experiential Opportunities'
            WHEN el_q4_total_marks BETWEEN 1 AND 2 THEN '1-2 Experiential Opportunities'
            WHEN el_q4_total_marks BETWEEN 3 AND 4 THEN '3-4 Experiential Opportunities'
            WHEN el_q4_total_marks BETWEEN 5 AND 7 THEN '5-7 Experiential Opportunities'
        END AS el_q4_marks_bucket,

        CASE
            WHEN el_q4_overall_score BETWEEN 0.00 AND 0.20 THEN 'Pre-Exposure Stage'
            WHEN el_q4_overall_score > 0.20 AND el_q4_overall_score <= 0.40 THEN 'Observational Learning Stage'
            WHEN el_q4_overall_score > 0.40 AND el_q4_overall_score <= 0.60 THEN 'Self-Appraisal / Early Exploration'
            WHEN el_q4_overall_score > 0.60 AND el_q4_overall_score <= 0.80 THEN 'Developing Self-Efficacy'
            WHEN el_q4_overall_score > 0.80 THEN 'Goal-Directed Action Stage'
        END AS el_q4_bucket,

        CASE
            WHEN el_q5_total_marks IS NULL THEN 'DNA'
            WHEN el_q5_total_marks = 0 THEN '0 CRS'
            WHEN el_q5_total_marks BETWEEN 1 AND 4 THEN '1-4 CRS'
            WHEN el_q5_total_marks BETWEEN 5 AND 7 THEN '5-7 CRS'
            WHEN el_q5_total_marks BETWEEN 8 AND 9 THEN '8-9 CRS'
        END AS el_q5_marks_bucket,

        CASE
            WHEN el_job_search_score BETWEEN 1.00 AND 1.75 THEN 'Novice'
            WHEN el_job_search_score > 1.75 AND el_job_search_score <= 2.50 THEN 'Advanced Beginner'
            WHEN el_job_search_score > 2.50 AND el_job_search_score <= 3.25 THEN 'Competent'
            WHEN el_job_search_score > 3.25 THEN 'Proficient'
        END AS el_job_search_level,

        CASE
            WHEN el_communication_score BETWEEN 1.00 AND 1.75 THEN 'Novice'
            WHEN el_communication_score > 1.75 AND el_communication_score <= 2.50 THEN 'Advanced Beginner'
            WHEN el_communication_score > 2.50 AND el_communication_score <= 3.25 THEN 'Competent'
            WHEN el_communication_score > 3.25 THEN 'Proficient'
        END AS el_communication_level,

        CASE
            WHEN el_cognitive_score BETWEEN 1.00 AND 1.75 THEN 'Novice'
            WHEN el_cognitive_score > 1.75 AND el_cognitive_score <= 2.50 THEN 'Advanced Beginner'
            WHEN el_cognitive_score > 2.50 AND el_cognitive_score <= 3.25 THEN 'Competent'
            WHEN el_cognitive_score > 3.25 THEN 'Proficient'
        END AS el_cognitive_level,

        CASE
            WHEN el_q5_overall_score BETWEEN 1.00 AND 1.75 THEN 'Novice'
            WHEN el_q5_overall_score > 1.75 AND el_q5_overall_score <= 2.50 THEN 'Advanced Beginner'
            WHEN el_q5_overall_score > 2.50 AND el_q5_overall_score <= 3.25 THEN 'Competent'
            WHEN el_q5_overall_score > 3.25 THEN 'Proficient'
        END AS el_q5_bucket,

        CASE
            WHEN el_q4_overall_score > bl_q4_overall_score THEN 'Improvement'
            WHEN el_q4_overall_score < bl_q4_overall_score THEN 'Area for Growth'
            ELSE 'No Change'
        END AS bl_el_q4_score,

        CASE
            WHEN el_q5_overall_score > bl_q5_overall_score THEN 'Improvement'
            WHEN el_q5_overall_score < bl_q5_overall_score THEN 'Area for Growth'
            ELSE 'No Change'
        END AS bl_el_q5_score

    FROM final

),

t0 as (SELECT student_id, student_barcode, gender, batch_no, batch_academic_year, batch_language, facilitator_id, facilitator_name, 
facilitator_email, school_id, school_name, school_taluka, school_ward, school_district, school_state, school_partner, 
school_area, donor_id, batch_donor, batch_grade, assessment_barcode, bl_CreatedDate, bl_cs_no, bl_q4_1, bl_q4_1_marks, 
bl_q4_2, bl_q4_2_marks, bl_q4_3, bl_q4_3_marks, bl_q4_4, bl_q4_4_marks, bl_q4_5, bl_q4_5_marks, bl_q4_6, bl_q4_6_marks, 
bl_q4_7, bl_q4_7_marks, bl_q4_8, bl_q4_8_marks, bl_q4_9, bl_q4_9_marks, bl_q4_10, bl_q4_10_marks, bl_q4_total_marks, 
bl_q4_marks_bucket, bl_direct_learning, bl_observational_learning, bl_self_assessment_based_learning, bl_q4_overall_score, 
bl_q4_bucket, bl_q5_1, bl_q5_1_marks, bl_q5_2, bl_q5_2_marks, bl_q5_3, bl_q5_3_marks, bl_q5_4, bl_q5_4_marks, bl_q5_5, 
bl_q5_5_marks, bl_q5_6, bl_q5_6_marks, bl_q5_7, bl_q5_7_marks, bl_q5_8, bl_q5_8_marks, bl_q5_9, bl_q5_9_marks, bl_q5_marks, 
bl_q5_total_marks, bl_q5_marks_bucket, bl_job_search_score, bl_communication_score, bl_cognitive_score, bl_job_search_level, 
bl_communication_level, bl_cognitive_level, bl_q5_overall_score, bl_q5_bucket, el_CreatedDate, el_cs_no, el_q4_1, el_q4_1_marks, 
el_q4_2, el_q4_2_marks, el_q4_3, el_q4_3_marks, el_q4_4, el_q4_4_marks, el_q4_5, el_q4_5_marks, el_q4_6, el_q4_6_marks, el_q4_7, 
el_q4_7_marks, el_q4_8, el_q4_8_marks, el_q4_9, el_q4_9_marks, el_q4_10, el_q4_10_marks, el_q4_total_marks, el_q4_marks_bucket, 
el_direct_learning, el_observational_learning, el_self_assessment_based_learning, el_q4_overall_score, el_q4_bucket, el_q5_1, 
el_q5_1_marks, el_q5_2, el_q5_2_marks, el_q5_3, el_q5_3_marks, el_q5_4, el_q5_4_marks, el_q5_5, el_q5_5_marks, el_q5_6, 
el_q5_6_marks, el_q5_7, el_q5_7_marks, el_q5_8, el_q5_8_marks, el_q5_9, el_q5_9_marks, el_q5_marks, el_q5_total_marks, 
el_q5_marks_bucket, el_job_search_score, el_communication_score, el_cognitive_score, el_job_search_level, el_communication_level, 
el_cognitive_level, el_q5_overall_score, el_q5_bucket, bl_el_q4_score, bl_el_q5_score
FROM final_buckets
)

select * from t0