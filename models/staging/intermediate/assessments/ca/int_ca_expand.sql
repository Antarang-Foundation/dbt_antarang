WITH base AS (
    SELECT * FROM {{ ref('stg_ca') }}
),

unpivoted AS (
    SELECT
        cdm3_id, cdm3_name, assessment_grade, assessment_batch_id, assessment_academic_year, assessment_student_id,
        assessment_barcode, q.industry, q.options_selected, q.total_marks
    FROM base,
    UNNEST([
        STRUCT('Q6.2' AS industry, Q6_2 AS options_selected, Q6_2_marks AS total_marks),
        STRUCT('Q6.3', Q6_3, Q6_3_marks),
        STRUCT('Q6.6', Q6_6, Q6_6_marks),
        STRUCT('Q6.7', Q6_7, Q6_7_marks),
        STRUCT('Q6.8', Q6_8, Q6_8_marks),
        STRUCT('Q6.9', Q6_9, Q6_9_marks),
        STRUCT('Q6.10', Q6_10, Q6_10_marks),
        STRUCT('Q6.11', Q6_11, Q6_11_marks),
        STRUCT('Q6.12', Q6_12, Q6_12_marks),
        STRUCT('Q6.13', Q6_13, Q6_13_marks)
    ]) q
)

SELECT *
FROM unpivoted