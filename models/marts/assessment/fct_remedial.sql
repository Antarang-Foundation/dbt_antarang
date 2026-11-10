WITH
dcp AS (
    SELECT
        student_id,
        student_barcode,
        gender,
        batch_no,
        batch_academic_year,
        batch_language,
        facilitator_id,
        facilitator_name,
        facilitator_email,
        school_id,
        school_name,
        school_taluka,
        school_ward,
        school_district,
        school_state,
        school_partner,
        school_area,
        donor_id,
        batch_donor,
        batch_grade
    FROM {{ ref("dev_int_global_dcp") }}
    WHERE SAFE_CAST(batch_academic_year AS INT64) >= 2026
),

remedial AS (
    SELECT *
    FROM {{ ref("stg_remedial") }}
    WHERE SAFE_CAST(assessment_academic_year AS INT64) = 2026
),

final AS (
    SELECT
        dcp.student_id,
        dcp.student_barcode,
        dcp.gender,
        dcp.batch_no,
        dcp.batch_academic_year,
        dcp.batch_language,
        dcp.facilitator_id,
        dcp.facilitator_name,
        dcp.facilitator_email,
        dcp.school_id,
        dcp.school_name,
        dcp.school_taluka,
        dcp.school_ward,
        dcp.school_district,
        dcp.school_state,
        dcp.school_partner,
        dcp.school_area,
        dcp.donor_id,
        dcp.batch_donor,
        dcp.batch_grade,

        remedial.el_no,
        remedial.assessment_barcode,
        remedial.el_CreatedDate,
        remedial.record_type_id,

        remedial.q1_1,
        remedial.q1_2,
        remedial.q1_marks,

        remedial.q2_1,
        remedial.q2_2,
        remedial.q2_marks,

        remedial.q3_1,
        remedial.q3_1_marks,
        remedial.q3_2,
        remedial.q3_2_marks,
        remedial.q3_marks,

        remedial.q4,
        remedial.q4_marks,

        remedial.q5,
        remedial.q5_marks,

        remedial.q6_1,
        remedial.q6_1_marks,
        remedial.q6_2,
        remedial.q6_2_marks,
        remedial.q6_3,
        remedial.q6_3_marks,
        remedial.q6_4,
        remedial.q6_4_marks,
        remedial.q6_5,
        remedial.q6_5_marks,
        remedial.q6_6,
        remedial.q6_6_marks,
        remedial.q6_7,
        remedial.q6_7_marks,
        remedial.q6_8,
        remedial.q6_8_marks,
        remedial.q6_marks,

        remedial.q7,
        remedial.q7_marks,

        remedial.q8_1,
        remedial.q8_2,
        remedial.q8_3,
        remedial.q8_4,
        remedial.q8_5,
        remedial.q8_6,
        remedial.q8_7,
        remedial.q8_8,
        remedial.q8_9,
        remedial.q8_10,
        remedial.q8_marks,

        remedial.q9_1,
        remedial.q9_2,
        remedial.q9_3,
        remedial.q9_4,
        remedial.q9_5,
        remedial.q9_6,
        remedial.q9_7,
        remedial.q9_8,
        remedial.q9_9,
        remedial.q9_10,
        remedial.q9_11,
        remedial.q9,

        remedial.q10,
        remedial.q10_marks,

        remedial.q11,
        remedial.q11_marks,

        remedial.record_type

    FROM dcp
    INNER JOIN remedial
        ON remedial.assessment_barcode = dcp.student_barcode
)

SELECT *
FROM final