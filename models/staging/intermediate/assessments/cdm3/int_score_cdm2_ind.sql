WITH base AS (

    SELECT *
    FROM {{ ref('int_student_global_cdm2_ind') }}

),

final AS (

    SELECT

        *,

        ------------------------------------------------------------------
        -- BASELINE TOTAL
        ------------------------------------------------------------------

        COALESCE(SAFE_CAST(bl_q3_1_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_2_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_3_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_4_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_5_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_6_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_7_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_8_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_9_marks AS INT64)) +
        COALESCE(SAFE_CAST(bl_q3_10_marks AS INT64))
        AS bl_q3_total_marks,

        ------------------------------------------------------------------
        -- ENDLINE TOTAL
        ------------------------------------------------------------------

        COALESCE(SAFE_CAST(el_q3_1_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_2_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_3_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_4_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_5_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_6_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_7_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_8_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_9_marks AS INT64)) +
        COALESCE(SAFE_CAST(el_q3_10_marks AS INT64))
        AS el_q3_total_marks

    FROM base

)

SELECT

    final.*,

    ------------------------------------------------------------------
    -- BASELINE BUCKET
    ------------------------------------------------------------------

    CASE
        WHEN bl_q3_total_marks >= 0
         AND bl_q3_total_marks < 10
            THEN '0-2 Career Tracks'

        WHEN bl_q3_total_marks >= 10
         AND bl_q3_total_marks < 20
            THEN '2-4 Career Tracks'

        WHEN bl_q3_total_marks >= 20
         AND bl_q3_total_marks < 30
            THEN '4-6 Career Tracks'

        WHEN bl_q3_total_marks >= 30
         AND bl_q3_total_marks < 40
            THEN '6-8 Career Tracks'

        WHEN bl_q3_total_marks >= 40
         AND bl_q3_total_marks <= 50
            THEN '8-10 Career Tracks'

        ELSE 'DNA'
    END AS bl_q3_bucket,

    ------------------------------------------------------------------
    -- ENDLINE BUCKET
    ------------------------------------------------------------------

    CASE
        WHEN el_q3_total_marks >= 0
         AND el_q3_total_marks < 10
            THEN '0-2 Career Tracks'

        WHEN el_q3_total_marks >= 10
         AND el_q3_total_marks < 20
            THEN '2-4 Career Tracks'

        WHEN el_q3_total_marks >= 20
         AND el_q3_total_marks < 30
            THEN '4-6 Career Tracks'

        WHEN el_q3_total_marks >= 30
         AND el_q3_total_marks < 40
            THEN '6-8 Career Tracks'

        WHEN el_q3_total_marks >= 40
         AND el_q3_total_marks <= 50
            THEN '8-10 Career Tracks'

        ELSE 'DNA'
    END AS el_q3_bucket,

    ------------------------------------------------------------------
    -- TOTAL IMPROVEMENT
    ------------------------------------------------------------------

    (el_q3_total_marks - bl_q3_total_marks)
        AS bl_el_q3_total_marks,

    CASE
        WHEN (el_q3_total_marks - bl_q3_total_marks) > 0
            THEN 'Improvement'

        WHEN (el_q3_total_marks - bl_q3_total_marks) < 0
            THEN 'Area for Growth'

        WHEN (el_q3_total_marks - bl_q3_total_marks) = 0
            THEN 'No Change'

        ELSE 'DNA'
    END AS bl_el_q3_status,

    ------------------------------------------------------------------
    -- TYPE OF ASSESSMENT FILLED
    ------------------------------------------------------------------

    CASE
        WHEN bl_ca2_no IS NOT NULL
         AND el_ca2_no IS NOT NULL
            THEN 'Both'

        WHEN bl_ca2_no IS NOT NULL
            THEN 'Baseline'

        WHEN el_ca2_no IS NOT NULL
            THEN 'Endline'

        ELSE 'DNA'
    END AS type_of_assessment_filled,

    ------------------------------------------------------------------
    -- QUESTION LEVEL IMPROVEMENT
    ------------------------------------------------------------------

    SAFE_CAST(el_q3_1_marks AS INT64) -
    SAFE_CAST(bl_q3_1_marks AS INT64) AS bl_el_q3_1,

    SAFE_CAST(el_q3_2_marks AS INT64) -
    SAFE_CAST(bl_q3_2_marks AS INT64) AS bl_el_q3_2,

    SAFE_CAST(el_q3_3_marks AS INT64) -
    SAFE_CAST(bl_q3_3_marks AS INT64) AS bl_el_q3_3,

    SAFE_CAST(el_q3_4_marks AS INT64) -
    SAFE_CAST(bl_q3_4_marks AS INT64) AS bl_el_q3_4,

    SAFE_CAST(el_q3_5_marks AS INT64) -
    SAFE_CAST(bl_q3_5_marks AS INT64) AS bl_el_q3_5,

    SAFE_CAST(el_q3_6_marks AS INT64) -
    SAFE_CAST(bl_q3_6_marks AS INT64) AS bl_el_q3_6,

    SAFE_CAST(el_q3_7_marks AS INT64) -
    SAFE_CAST(bl_q3_7_marks AS INT64) AS bl_el_q3_7,

    SAFE_CAST(el_q3_8_marks AS INT64) -
    SAFE_CAST(bl_q3_8_marks AS INT64) AS bl_el_q3_8,

    SAFE_CAST(el_q3_9_marks AS INT64) -
    SAFE_CAST(bl_q3_9_marks AS INT64) AS bl_el_q3_9,

    SAFE_CAST(el_q3_10_marks AS INT64) -
    SAFE_CAST(bl_q3_10_marks AS INT64) AS bl_el_q3_10

FROM final