WITH base_explode AS (
    SELECT *
    FROM {{ ref('int_ca_explode') }}
),

base_student AS (
    SELECT *
    FROM {{ ref('int_student_global') }}
),

-- 🔹 explode correct_options (a,b,c → rows)
correct_options_split AS (
    SELECT
        e.cdm3_id,
        e.cdm3_name,
        e.assessment_grade,
        e.assessment_batch_id,
        e.assessment_academic_year,
        e.assessment_student_id,
        e.assessment_barcode,
        e.industry,
        TRIM(opt) AS option,
        e.career_type
    FROM base_explode e,
    UNNEST(SPLIT(e.correct_options, ',')) AS opt
),

final AS (
    SELECT
        -- 🔹 from explode (your base)
        c.cdm3_id,
        c.cdm3_name,
        c.assessment_grade,
        c.assessment_batch_id,
        c.assessment_academic_year,
        c.assessment_student_id,
        c.assessment_barcode,
        c.industry,
        c.option,
        c.career_type,
        --c.record_type_id,
        --c.created_by_date,

        -- 🔹 from student table
        s.student_id,
        s.student_barcode,
        s.student_grade,
        s.gender,
        s.student_batch_id,
        s.batch_no,
        s.batch_id,
        s.batch_grade,
        s.g9_barcode,
        s.g10_barcode,
        s.g11_barcode,
        s.g12_barcode,
        s.batch_academic_year,
        s.batch_language,
        s.batch_facilitator_id,
        s.batch_school_id,
        s.facilitator_id,
        s.facilitator_name,
        s.facilitator_email,
        s.school_id,
        s.school_name,
        s.school_academic_year,
        s.school_language,
        s.school_taluka,
        s.school_ward,
        s.school_district,
        s.school_state,
        s.school_partner,
        s.school_area,
        s.batch_donor_id,
        s.donor_id,
        s.batch_donor,
        --s.created_by_date

    FROM correct_options_split c

    LEFT JOIN base_student s
        ON s.student_id = c.assessment_student_id
        AND s.batch_id = c.assessment_batch_id
)

SELECT *
FROM final