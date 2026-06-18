WITH baseline AS (

    SELECT *
    FROM (

        SELECT
            assessment_barcode,
            created_on,
            cdm2_no,

            q6_1,
            q6_2,
            q6_3,
            q6_4,
            q6_5,
            q6_6,
            q6_7,
            q6_8,
            q6_9,
            q6_10, 
            q6_1_marks, q6_2_marks, q6_3_marks, q6_4_marks, q6_5_marks, q6_6_marks, q6_7_marks, q6_8_marks, q6_9_marks, q6_10_marks,

            ROW_NUMBER() OVER (
                PARTITION BY assessment_barcode
                ORDER BY created_on DESC, cdm2_id DESC
            ) rn

        FROM {{ ref('stg_cdm2') }}
        WHERE LOWER(record_type) = 'baseline' AND SAFE_CAST(assessment_academic_year AS INT64) >= 2026

    )
    WHERE rn = 1

),

endline AS (

    SELECT *
    FROM (

        SELECT
            assessment_barcode,
            created_on,
            cdm2_no,

            q6_1,
            q6_2,
            q6_3,
            q6_4,
            q6_5,
            q6_6,
            q6_7,
            q6_8,
            q6_9,
            q6_10,
            q6_1_marks, q6_2_marks, q6_3_marks, q6_4_marks, q6_5_marks, q6_6_marks, q6_7_marks, q6_8_marks, q6_9_marks, q6_10_marks,

            ROW_NUMBER() OVER (
                PARTITION BY assessment_barcode
                ORDER BY created_on DESC, cdm2_id DESC
            ) rn

        FROM {{ ref('stg_cdm2') }}
        WHERE LOWER(record_type) = 'endline' AND SAFE_CAST(assessment_academic_year AS INT64) >= 2026

    )
    WHERE rn = 1

),

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
    FROM {{ ref('dev_int_global_dcp') }}
    WHERE SAFE_CAST(batch_academic_year AS INT64) >= 2026

),

final AS (SELECT

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

    COALESCE(baseline.assessment_barcode, endline.assessment_barcode)
        AS assessment_barcode,

    baseline.created_on AS bl_createddate,
    baseline.cdm2_no AS bl_ca2_no,

    baseline.q6_1  AS bl_q3_1,
    baseline.q6_2  AS bl_q3_2,
    baseline.q6_3  AS bl_q3_3,
    baseline.q6_4  AS bl_q3_4,
    baseline.q6_5  AS bl_q3_5,
    baseline.q6_6  AS bl_q3_6,
    baseline.q6_7  AS bl_q3_7,
    baseline.q6_8  AS bl_q3_8,
    baseline.q6_9  AS bl_q3_9,
    baseline.q6_10 AS bl_q3_10,

    baseline.q6_1_marks  AS bl_q3_1_marks,
    baseline.q6_2_marks  AS bl_q3_2_marks,
    baseline.q6_3_marks  AS bl_q3_3_marks,
    baseline.q6_4_marks  AS bl_q3_4_marks,
    baseline.q6_5_marks  AS bl_q3_5_marks,
    baseline.q6_6_marks  AS bl_q3_6_marks,
    baseline.q6_7_marks  AS bl_q3_7_marks,
    baseline.q6_8_marks  AS bl_q3_8_marks,
    baseline.q6_9_marks  AS bl_q3_9_marks,
    baseline.q6_10_marks AS bl_q3_10_marks,

    endline.created_on AS el_createddate,
    endline.cdm2_no AS el_ca2_no,

    endline.q6_1  AS el_q3_1,
    endline.q6_2  AS el_q3_2,
    endline.q6_3  AS el_q3_3,
    endline.q6_4  AS el_q3_4,
    endline.q6_5  AS el_q3_5,
    endline.q6_6  AS el_q3_6,
    endline.q6_7  AS el_q3_7,
    endline.q6_8  AS el_q3_8,
    endline.q6_9  AS el_q3_9,
    endline.q6_10 AS el_q3_10,

    endline.q6_1_marks  AS el_q3_1_marks,
    endline.q6_2_marks  AS el_q3_2_marks,
    endline.q6_3_marks  AS el_q3_3_marks,
    endline.q6_4_marks  AS el_q3_4_marks,
    endline.q6_5_marks  AS el_q3_5_marks,
    endline.q6_6_marks  AS el_q3_6_marks,
    endline.q6_7_marks  AS el_q3_7_marks,
    endline.q6_8_marks  AS el_q3_8_marks,
    endline.q6_9_marks  AS el_q3_9_marks,
    endline.q6_10_marks AS el_q3_10_marks
FROM dcp

LEFT JOIN baseline
    ON dcp.student_barcode = baseline.assessment_barcode

LEFT JOIN endline
    ON dcp.student_barcode = endline.assessment_barcode
)

SELECT * FROM final
