WITH t0 AS (

    SELECT *
    FROM (

        SELECT
            cdm2_id,
            assessment_barcode,
            record_type,
            created_on,
            created_from_form,
            cdm2_no,
            is_non_null,
            assessment_grade,
            assessment_academic_year,
            assessment_batch_id,

            q5,
            SAFE_CAST(q5_marks AS NUMERIC) AS q5_marks,

            q6_1,
            SAFE_CAST(q6_1_marks AS NUMERIC) AS q6_1_marks,

            q6_2,
            SAFE_CAST(q6_2_marks AS NUMERIC) AS q6_2_marks,

            q6_3,
            SAFE_CAST(q6_3_marks AS NUMERIC) AS q6_3_marks,

            q6_4,
            SAFE_CAST(q6_4_marks AS NUMERIC) AS q6_4_marks,

            q6_5,
            SAFE_CAST(q6_5_marks AS NUMERIC) AS q6_5_marks,

            q6_6,
            SAFE_CAST(q6_6_marks AS NUMERIC) AS q6_6_marks,

            q6_7,
            SAFE_CAST(q6_7_marks AS NUMERIC) AS q6_7_marks,

            q6_8,
            SAFE_CAST(q6_8_marks AS NUMERIC) AS q6_8_marks,

            q6_9,
            SAFE_CAST(q6_9_marks AS NUMERIC) AS q6_9_marks,

            q6_10,
            SAFE_CAST(q6_10_marks AS NUMERIC) AS q6_10_marks,

            q6_11,
            SAFE_CAST(q6_11_marks AS NUMERIC) AS q6_11_marks,

            q6_12,
            SAFE_CAST(q6_12_marks AS NUMERIC) AS q6_12_marks,

            SAFE_CAST(q6_total_marks AS NUMERIC) AS q6_total_marks,
            SAFE_CAST(cdm2_total_marks AS NUMERIC) AS cdm2_total_marks,

            error_status,
            data_cleanup,
            marks_recalculated,
            student_linked,

            ROW_NUMBER() OVER (
                PARTITION BY assessment_barcode
                ORDER BY created_on DESC, cdm2_id DESC
            ) AS rn

        FROM {{ ref('stg_cdm2') }}
        WHERE SAFE_CAST(assessment_academic_year AS INT64) >= 2026

    )
    WHERE rn = 1

),

dcp AS (

    SELECT
        student_id,
        student_barcode,
        gender,
        birth_date,
        student_age,
        religion,
        caste,
        father_education,
        father_occupation,
        mother_education,
        mother_occupation,
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

)

SELECT

    dcp.*,

    t0.cdm2_id,
    t0.assessment_barcode,
    t0.record_type,
    t0.created_on,
    t0.created_from_form,
    t0.cdm2_no,
    t0.assessment_grade,
    t0.assessment_academic_year,

    CASE
        WHEN t0.q5_marks >= 80 THEN 'High'
        WHEN t0.q5_marks >= 50 THEN 'Medium'
        WHEN t0.q5_marks IS NOT NULL THEN 'Low'
    END AS q5_category,

    CASE WHEN t0.q6_1_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_1_category,
    CASE WHEN t0.q6_2_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_2_category,
    CASE WHEN t0.q6_3_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_3_category,
    CASE WHEN t0.q6_4_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_4_category,
    CASE WHEN t0.q6_5_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_5_category,
    CASE WHEN t0.q6_6_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_6_category,
    CASE WHEN t0.q6_7_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_7_category,
    CASE WHEN t0.q6_8_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_8_category,
    CASE WHEN t0.q6_9_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_9_category,
    CASE WHEN t0.q6_10_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_10_category,
    CASE WHEN t0.q6_11_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_11_category,
    CASE WHEN t0.q6_12_marks >= 1 THEN 'Selected' ELSE 'Not Selected' END AS q6_12_category,

    CASE
        WHEN t0.cdm2_total_marks >= 80 THEN 'Excellent'
        WHEN t0.cdm2_total_marks >= 60 THEN 'Good'
        WHEN t0.cdm2_total_marks >= 40 THEN 'Average'
        WHEN t0.cdm2_total_marks IS NOT NULL THEN 'Needs Support'
    END AS overall_bucket,

    t0.cdm2_total_marks

FROM dcp
LEFT JOIN t0
    ON dcp.student_barcode = t0.assessment_barcode