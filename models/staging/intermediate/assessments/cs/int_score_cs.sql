WITH base AS (

    SELECT *
    FROM {{ ref('int_student_global_cs') }}

),

cleaned AS (

    SELECT
        *,

        SAFE_CAST(bl_q4_1 AS INT64) AS bl_q4_1_num,
        SAFE_CAST(bl_q4_2 AS INT64) AS bl_q4_2_num,
        SAFE_CAST(bl_q4_3 AS INT64) AS bl_q4_3_num,
        SAFE_CAST(bl_q4_4 AS INT64) AS bl_q4_4_num,
        SAFE_CAST(bl_q4_5 AS INT64) AS bl_q4_5_num,
        SAFE_CAST(bl_q4_6 AS INT64) AS bl_q4_6_num,
        SAFE_CAST(bl_q4_7 AS INT64) AS bl_q4_7_num,
        SAFE_CAST(bl_q4_8 AS INT64) AS bl_q4_8_num,
        SAFE_CAST(bl_q4_9 AS INT64) AS bl_q4_9_num,
        SAFE_CAST(bl_q4_10 AS INT64) AS bl_q4_10_num,
        SAFE_CAST(bl_q5_1 AS INT64) AS bl_q5_1_num,
        SAFE_CAST(bl_q5_2 AS INT64) AS bl_q5_2_num,
        SAFE_CAST(bl_q5_3 AS INT64) AS bl_q5_3_num,
        SAFE_CAST(bl_q5_4 AS INT64) AS bl_q5_4_num,
        SAFE_CAST(bl_q5_5 AS INT64) AS bl_q5_5_num,
        SAFE_CAST(bl_q5_6 AS INT64) AS bl_q5_6_num,
        SAFE_CAST(bl_q5_7 AS INT64) AS bl_q5_7_num,
        SAFE_CAST(bl_q5_8 AS INT64) AS bl_q5_8_num,
        SAFE_CAST(bl_q5_9 AS INT64) AS bl_q5_9_num,

        SAFE_CAST(el_q4_1 AS INT64) AS el_q4_1_num,
        SAFE_CAST(el_q4_2 AS INT64) AS el_q4_2_num,
        SAFE_CAST(el_q4_3 AS INT64) AS el_q4_3_num,
        SAFE_CAST(el_q4_4 AS INT64) AS el_q4_4_num,
        SAFE_CAST(el_q4_5 AS INT64) AS el_q4_5_num,
        SAFE_CAST(el_q4_6 AS INT64) AS el_q4_6_num,
        SAFE_CAST(el_q4_7 AS INT64) AS el_q4_7_num,
        SAFE_CAST(el_q4_8 AS INT64) AS el_q4_8_num,
        SAFE_CAST(el_q4_9 AS INT64) AS el_q4_9_num,
        SAFE_CAST(el_q4_10 AS INT64) AS el_q4_10_num,
        SAFE_CAST(el_q5_1 AS INT64) AS el_q5_1_num,
        SAFE_CAST(el_q5_2 AS INT64) AS el_q5_2_num,
        SAFE_CAST(el_q5_3 AS INT64) AS el_q5_3_num,
        SAFE_CAST(el_q5_4 AS INT64) AS el_q5_4_num,
        SAFE_CAST(el_q5_5 AS INT64) AS el_q5_5_num,
        SAFE_CAST(el_q5_6 AS INT64) AS el_q5_6_num,
        SAFE_CAST(el_q5_7 AS INT64) AS el_q5_7_num,
        SAFE_CAST(el_q5_8 AS INT64) AS el_q5_8_num,
        SAFE_CAST(el_q5_9 AS INT64) AS el_q5_9_num

    FROM base

)

SELECT

    cleaned.*,

    ------------------------------------------------------------------
    -- BL Q4 MARKS
    ------------------------------------------------------------------

    bl_q4_1  AS bl_q4_1_marks,
    bl_q4_2  AS bl_q4_2_marks,
    bl_q4_3  AS bl_q4_3_marks,
    bl_q4_4  AS bl_q4_4_marks,
    bl_q4_5  AS bl_q4_5_marks,
    bl_q4_6  AS bl_q4_6_marks,
    bl_q4_7  AS bl_q4_7_marks,
    bl_q4_8  AS bl_q4_8_marks,
    bl_q4_9  AS bl_q4_9_marks,
    bl_q4_10 AS bl_q4_10_marks,

    (
        COALESCE(bl_q4_1_num,0) +
        COALESCE(bl_q4_2_num,0) +
        COALESCE(bl_q4_3_num,0) +
        COALESCE(bl_q4_4_num,0) +
        COALESCE(bl_q4_5_num,0) +
        COALESCE(bl_q4_6_num,0) +
        COALESCE(bl_q4_7_num,0) +
        COALESCE(bl_q4_8_num,0) +
        COALESCE(bl_q4_9_num,0) +
        COALESCE(bl_q4_10_num,0)
    ) AS bl_q4_total_marks,

    ------------------------------------------------------------------
    -- BL LEARNING SCORES
    ------------------------------------------------------------------

    (
        COALESCE(bl_q4_2_num,0) +
        COALESCE(bl_q4_8_num,0) +
        COALESCE(bl_q4_9_num,0)
    ) AS bl_direct_learning,

    (
        COALESCE(bl_q4_10_num,0) +
        COALESCE(bl_q4_1_num,0) +
        COALESCE(bl_q4_7_num,0)
    ) AS bl_observational_learning,

    COALESCE(bl_q4_5_num,0) AS bl_self_assessment_based_learning,

    (
        (
            (
                COALESCE(bl_q4_2_num,0)+
                COALESCE(bl_q4_8_num,0)+
                COALESCE(bl_q4_9_num,0)
            ) / 3.0
        ) * 0.5
        +
        (
            (
                COALESCE(bl_q4_10_num,0)+
                COALESCE(bl_q4_1_num,0)+
                COALESCE(bl_q4_7_num,0)
            ) / 3.0
        ) * 0.3
        +
        (
            COALESCE(bl_q4_5_num,0) * 0.2
        )
    ) AS bl_q4_overall_score,

    ------------------------------------------------------------------
    -- BL Q5
    ------------------------------------------------------------------

    bl_q5_1 AS bl_q5_1_marks,
    bl_q5_2 AS bl_q5_2_marks,
    bl_q5_3 AS bl_q5_3_marks,
    bl_q5_4 AS bl_q5_4_marks,
    bl_q5_5 AS bl_q5_5_marks,
    bl_q5_6 AS bl_q5_6_marks,
    bl_q5_7 AS bl_q5_7_marks,
    bl_q5_8 AS bl_q5_8_marks,
    bl_q5_9 AS bl_q5_9_marks,

    (
        COALESCE(bl_q5_1_num,0)+
        COALESCE(bl_q5_2_num,0)+
        COALESCE(bl_q5_3_num,0)+
        COALESCE(bl_q5_4_num,0)+
        COALESCE(bl_q5_5_num,0)+
        COALESCE(bl_q5_6_num,0)+
        COALESCE(bl_q5_7_num,0)+
        COALESCE(bl_q5_8_num,0)+
        COALESCE(bl_q5_9_num,0)
    ) AS bl_q5_total_marks,

    (
        CASE WHEN bl_q5_1_num >= 2 THEN 1 ELSE 0 END +
        CASE WHEN bl_q5_2_num >= 2 THEN 1 ELSE 0 END +
        CASE WHEN bl_q5_3_num >= 2 THEN 1 ELSE 0 END +
        CASE WHEN bl_q5_4_num >= 2 THEN 1 ELSE 0 END +
        CASE WHEN bl_q5_5_num >= 2 THEN 1 ELSE 0 END +
        CASE WHEN bl_q5_6_num >= 2 THEN 1 ELSE 0 END +
        CASE WHEN bl_q5_7_num >= 2 THEN 1 ELSE 0 END +
        CASE WHEN bl_q5_8_num >= 2 THEN 1 ELSE 0 END +
        CASE WHEN bl_q5_9_num >= 2 THEN 1 ELSE 0 END
    ) AS bl_q5_marks,

    ROUND(
        (
            COALESCE(bl_q5_1_num,0)+
            COALESCE(bl_q5_2_num,0)+
            COALESCE(bl_q5_5_num,0)+
            COALESCE(bl_q5_6_num,0)
        ) / 4.0
    ,3) AS bl_job_search_score,

    ROUND(
        (
            COALESCE(bl_q5_3_num,0)+
            COALESCE(bl_q5_4_num,0)+
            COALESCE(bl_q5_8_num,0)
        ) / 3.0
    ,3) AS bl_communication_score,

    ROUND(
        (
            COALESCE(bl_q5_7_num,0)+
            COALESCE(bl_q5_9_num,0)
        ) / 2.0
    ,3) AS bl_cognitive_score,

    ------------------------------------------------------------------
    -- EL Q4
    ------------------------------------------------------------------

    el_q4_1 AS el_q4_1_marks,
    el_q4_2 AS el_q4_2_marks,
    el_q4_3 AS el_q4_3_marks,
    el_q4_4 AS el_q4_4_marks,
    el_q4_5 AS el_q4_5_marks,
    el_q4_6 AS el_q4_6_marks,
    el_q4_7 AS el_q4_7_marks,
    el_q4_8 AS el_q4_8_marks,
    el_q4_9 AS el_q4_9_marks,
    el_q4_10 AS el_q4_10_marks,

    ------------------------------------------------------------------
    -- EL LEARNING SCORES
    ------------------------------------------------------------------

    (
        COALESCE(el_q4_2_num,0) +
        COALESCE(el_q4_8_num,0) +
        COALESCE(el_q4_9_num,0)
    ) AS el_direct_learning,

    (
        COALESCE(el_q4_10_num,0) +
        COALESCE(el_q4_1_num,0) +
        COALESCE(el_q4_7_num,0)
    ) AS el_observational_learning,

    COALESCE(el_q4_5_num,0) AS el_self_assessment_based_learning,

    ------------------------------------------------------------------
    -- EL Q5
    ------------------------------------------------------------------
el_q5_1 AS el_q5_1_marks,
el_q5_2 AS el_q5_2_marks,
el_q5_3 AS el_q5_3_marks,
el_q5_4 AS el_q5_4_marks,
el_q5_5 AS el_q5_5_marks,
el_q5_6 AS el_q5_6_marks,
el_q5_7 AS el_q5_7_marks,
el_q5_8 AS el_q5_8_marks,
el_q5_9 AS el_q5_9_marks,

(
    COALESCE(el_q5_1_num,0)+
    COALESCE(el_q5_2_num,0)+
    COALESCE(el_q5_3_num,0)+
    COALESCE(el_q5_4_num,0)+
    COALESCE(el_q5_5_num,0)+
    COALESCE(el_q5_6_num,0)+
    COALESCE(el_q5_7_num,0)+
    COALESCE(el_q5_8_num,0)+
    COALESCE(el_q5_9_num,0)
) AS el_q5_total_marks,

(
    CASE WHEN el_q5_1_num >= 2 THEN 1 ELSE 0 END +
    CASE WHEN el_q5_2_num >= 2 THEN 1 ELSE 0 END +
    CASE WHEN el_q5_3_num >= 2 THEN 1 ELSE 0 END +
    CASE WHEN el_q5_4_num >= 2 THEN 1 ELSE 0 END +
    CASE WHEN el_q5_5_num >= 2 THEN 1 ELSE 0 END +
    CASE WHEN el_q5_6_num >= 2 THEN 1 ELSE 0 END +
    CASE WHEN el_q5_7_num >= 2 THEN 1 ELSE 0 END +
    CASE WHEN el_q5_8_num >= 2 THEN 1 ELSE 0 END +
    CASE WHEN el_q5_9_num >= 2 THEN 1 ELSE 0 END
) AS el_q5_marks,

    ROUND(
        (
            COALESCE(el_q5_1_num,0)+
            COALESCE(el_q5_2_num,0)+
            COALESCE(el_q5_5_num,0)+
            COALESCE(el_q5_6_num,0)
        ) / 4.0
    ,3) AS el_job_search_score,

    ROUND(
        (
            COALESCE(el_q5_3_num,0)+
            COALESCE(el_q5_4_num,0)+
            COALESCE(el_q5_8_num,0)
        ) / 3.0
    ,3) AS el_communication_score,

    ROUND(
        (
            COALESCE(el_q5_7_num,0)+
            COALESCE(el_q5_9_num,0)
        ) / 2.0
    ,3) AS el_cognitive_score

FROM cleaned