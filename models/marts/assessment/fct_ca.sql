WITH base AS (
    SELECT *
    FROM {{ ref('int_student_global_ca') }}
),

-- 🔹 join with seed
joined AS (
    SELECT
        b.student_id,
        b.assessment_student_id,
        b.industry,
        b.option,
        s.career_type,
        s.weight
    FROM base b
    LEFT JOIN {{ ref('seed_industry') }} s
        ON b.industry = s.industry
        AND b.option = s.option
),

-- 🔹 aggregate (THIS is your final grain)
aggregated AS (
    SELECT
        student_id,
        assessment_student_id,
        industry,

        COUNT(option) AS correct_count,

        COUNTIF(career_type = 'Steady') AS steady_count,
        COUNTIF(career_type = 'Emerging') AS emerging_count,

        SUM(weight) AS industry_wise_weightage

    FROM joined
    GROUP BY 1,2,3
),

-- 🔹 get DISTINCT student-level columns (important)
student_dim AS (
    SELECT DISTINCT
        student_id,
        student_barcode,
        student_grade,
        gender,
        student_batch_id,
        batch_no,
        batch_grade,
        batch_academic_year,
        batch_language,
        batch_facilitator_id,
        batch_school_id,
        facilitator_id,
        facilitator_name,
        facilitator_email,
        school_id,
        school_name,
        school_academic_year,
        school_language,
        school_district,
        school_state,
        assessment_student_id,
        assessment_barcode
    FROM base
)

-- 🔹 final output
SELECT
    d.*,
    a.industry,
    a.correct_count,
    a.steady_count,
    a.emerging_count,
    a.industry_wise_weightage

FROM student_dim d
LEFT JOIN aggregated a
    ON d.student_id = a.student_id
    AND d.assessment_student_id = a.assessment_student_id