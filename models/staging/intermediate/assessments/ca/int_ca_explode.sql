WITH base AS (
    SELECT *
    FROM {{ ref('int_ca_expand') }}
),

exploded AS (
    SELECT
        b.cdm3_id,
        b.cdm3_name,
        b.assessment_grade,
        b.assessment_batch_id,
        b.assessment_academic_year,
        b.assessment_student_id,
        b.assessment_barcode,
        b.industry,

        TRIM(opt) AS correct_options

    FROM base b

    LEFT JOIN UNNEST(
        CASE 
            WHEN b.options_selected IS NULL OR b.options_selected = '' 
            THEN [CAST(NULL AS STRING)]   -- ✅ FIX
            ELSE SPLIT(b.options_selected, ',')
        END
    ) AS opt
),

seed AS (
    SELECT 
        industry,
        option,
        career_type
    FROM {{ ref('seed_industry') }}
)

SELECT
    e.cdm3_id,
    e.cdm3_name,
    e.assessment_grade,
    e.assessment_batch_id,
    e.assessment_academic_year,
    e.assessment_student_id,
    e.assessment_barcode,
    e.industry,
    e.correct_options,
    s.career_type

FROM exploded e

LEFT JOIN seed s
    ON s.industry = e.industry AND s.option = e.correct_options
WHERE s.career_type IS NOT NULL