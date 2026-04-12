WITH t1 AS (
    SELECT * 
    FROM {{ ref('int_student_diagnostic') }}
),

with_mean AS (
    SELECT 
        *,

        -- Mean (NULL → 0, N = 10)
        (
            IFNULL(SAFE_CAST(q1 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q2 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q3 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q4 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q5 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q6 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q7 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q8 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q9 AS FLOAT64), 0) +
            IFNULL(SAFE_CAST(q10 AS FLOAT64), 0)
        ) / 10 AS mean_score

    FROM t1
),

final as (SELECT 
    *,

    -- SD (ignore NULL in deviation, but N = 10)
    ROUND(
        SQRT(
            (
                IF(q1 IS NOT NULL, POW(SAFE_CAST(q1 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q2 IS NOT NULL, POW(SAFE_CAST(q2 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q3 IS NOT NULL, POW(SAFE_CAST(q3 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q4 IS NOT NULL, POW(SAFE_CAST(q4 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q5 IS NOT NULL, POW(SAFE_CAST(q5 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q6 IS NOT NULL, POW(SAFE_CAST(q6 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q7 IS NOT NULL, POW(SAFE_CAST(q7 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q8 IS NOT NULL, POW(SAFE_CAST(q8 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q9 IS NOT NULL, POW(SAFE_CAST(q9 AS FLOAT64) - mean_score, 2), 0.0) +
                IF(q10 IS NOT NULL, POW(SAFE_CAST(q10 AS FLOAT64) - mean_score, 2), 0.0)
            ) / 10
        ),
    2) AS sd_score

FROM with_mean
)

SELECT 
    *,

    CASE 

        -- Grade 9 & 10
        WHEN assessment_grade IN ('Grade 9','Grade 10') AND mean_score BETWEEN 2.01 AND 3.00 THEN 'Career Ready'
        WHEN assessment_grade IN ('Grade 9','Grade 10') AND mean_score BETWEEN 1.01 AND 2.00 THEN 'Developing'
        WHEN assessment_grade IN ('Grade 9','Grade 10') AND mean_score BETWEEN 0.00 AND 1.00 THEN 'Needs Support'

        -- Grade 11 & 12
        WHEN assessment_grade IN ('Grade 11','Grade 12') AND mean_score BETWEEN 2.67 AND 4.00 THEN 'Career Ready'
        WHEN assessment_grade IN ('Grade 11','Grade 12') AND mean_score BETWEEN 1.34 AND 2.66 THEN 'Developing'
        WHEN assessment_grade IN ('Grade 11','Grade 12') AND mean_score BETWEEN 0.00 AND 1.33 THEN 'Needs Support'
    END AS awareness_level

FROM final