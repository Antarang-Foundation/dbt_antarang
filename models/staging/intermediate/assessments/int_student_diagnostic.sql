WITH int_student_global AS (
    SELECT 
        student_id, student_barcode, student_grade, gender, student_batch_id, batch_no, batch_grade, g9_barcode, g10_barcode, 
        g11_barcode, g12_barcode, batch_academic_year, batch_language, batch_facilitator_id, batch_school_id, facilitator_id, 
        facilitator_name, facilitator_email, school_id, school_name, school_academic_year, school_language, 
        school_taluka, school_ward, school_district, school_state, school_partner, school_area, batch_donor_id, donor_id, batch_donor
    FROM {{ ref('dev_int_global_dcp') }}
),

student_diagnostics AS (
    SELECT 
        -- ✅ ALL student columns
        b.*,

        -- ✅ diagnostics columns
        a.created_by_date, a.created_from_form, a.diagnostics_id, a.diagnostics_name, a.record_type_id, a.assessment_barcode, a.assessment_grade,

        COALESCE(a.a_q1, a.b_q1) AS q1,
        COALESCE(a.a_q2, a.b_q2) AS q2,
        COALESCE(a.a_q3, a.b_q3) AS q3,
        COALESCE(a.a_q4, a.b_q4) AS q4,
        COALESCE(a.a_q5, a.b_q5) AS q5,
        COALESCE(a.a_q6, a.b_q6) AS q6,
        COALESCE(a.a_q7, a.b_q7) AS q7,
        COALESCE(a.a_q8, a.b_q8) AS q8,
        COALESCE(a.a_q9, a.b_q9) AS q9,
        COALESCE(a.a_q10, a.b_q10) AS q10

    FROM {{ ref('stg_student_diagnostic') }} a
    LEFT JOIN int_student_global b
        ON a.assessment_barcode = b.student_barcode
)

SELECT *
FROM student_diagnostics
