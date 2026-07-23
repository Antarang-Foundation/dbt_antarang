WITH t0 AS (

    SELECT *
    FROM {{ ref('int_student_global_assessment') }}

),

t1 as (SELECT

    student_id,
    student_barcode,
    sd2_barcode,
    first_barcode,
    student_grade,
    student_details_2_submitted,
    sd2_grade,
    gender,
    batch_id,
    batch_no,
    batch_academic_year,
    no_of_students_facilitated,
    batch_grade,
    batch_language,
    fac_start_date,
    facilitator_name,
    facilitator_email,
    school_name,
    school_academic_year,
    school_language,
    school_taluka,
    school_ward,
    school_district,
    school_state,
    school_partner,
    school_area,
    batch_donor,
    assessment_barcode,
    assessment_grade,
    assessment_academic_year,
    created_from_form,

    ARRAY_TO_STRING(
        ARRAY(
            SELECT REPLACE(status, '_no', '')
            FROM UNNEST([
                IF(bl_cdm1_no IS NOT NULL, 'bl_cdm1_no', NULL),
                IF(el_cdm1_no IS NOT NULL, 'el_cdm1_no', NULL),
                IF(bl_cdm2_no IS NOT NULL, 'bl_cdm2_no', NULL),
                IF(el_cdm2_no IS NOT NULL, 'el_cdm2_no', NULL),
                IF(bl_cp_no IS NOT NULL, 'bl_cp_no', NULL),
                IF(el_cp_no IS NOT NULL, 'el_cp_no', NULL),
                IF(bl_cs_no IS NOT NULL, 'bl_cs_no', NULL),
                IF(el_cs_no IS NOT NULL, 'el_cs_no', NULL),
                IF(bl_fp_no IS NOT NULL, 'bl_fp_no', NULL),
                IF(el_fp_no IS NOT NULL, 'el_fp_no', NULL),
                IF(saf_no IS NOT NULL, 'saf_no', NULL),
                IF(sar_no IS NOT NULL, 'sar_no', NULL)
            ]) AS status
            WHERE status IS NOT NULL
        ),
        ', '
    ) AS submissions,

    CASE
        WHEN bl_cdm1_no IS NOT NULL
          OR el_cdm1_no IS NOT NULL
          OR bl_cdm2_no IS NOT NULL
          OR el_cdm2_no IS NOT NULL
          OR bl_cp_no IS NOT NULL
          OR el_cp_no IS NOT NULL
          OR bl_cs_no IS NOT NULL
          OR el_cs_no IS NOT NULL
          OR bl_fp_no IS NOT NULL
          OR el_fp_no IS NOT NULL
          OR saf_no IS NOT NULL
          OR sar_no IS NOT NULL
        THEN 1
        ELSE 0
    END AS atleast_one_submission,

    CASE
        WHEN bl_cdm1_no IS NOT NULL
          OR el_cdm1_no IS NOT NULL
          OR bl_cdm2_no IS NOT NULL
          OR el_cdm2_no IS NOT NULL
          OR bl_cp_no IS NOT NULL
          OR el_cp_no IS NOT NULL
          OR bl_cs_no IS NOT NULL
          OR el_cs_no IS NOT NULL
          OR bl_fp_no IS NOT NULL
          OR el_fp_no IS NOT NULL
        THEN 1
        ELSE 0
    END AS atleast_one_assessment_submission,

    CASE
        WHEN saf_no IS NOT NULL
          OR sar_no IS NOT NULL
        THEN 1
        ELSE 0
    END AS atleast_one_quiz_submission,

    CASE
        WHEN bl_cdm1_no IS NULL AND el_cdm1_no IS NULL THEN '1. Neither'
        WHEN bl_cdm1_no IS NOT NULL AND el_cdm1_no IS NULL THEN '2. Only_BL'
        WHEN bl_cdm1_no IS NULL AND el_cdm1_no IS NOT NULL THEN '3. Only_EL'
        WHEN bl_cdm1_no IS NOT NULL AND el_cdm1_no IS NOT NULL THEN '4. Both'
    END AS cdm1_status,

    CASE
        WHEN bl_cdm2_no IS NULL AND el_cdm2_no IS NULL THEN '1. Neither'
        WHEN bl_cdm2_no IS NOT NULL AND el_cdm2_no IS NULL THEN '2. Only_BL'
        WHEN bl_cdm2_no IS NULL AND el_cdm2_no IS NOT NULL THEN '3. Only_EL'
        WHEN bl_cdm2_no IS NOT NULL AND el_cdm2_no IS NOT NULL THEN '4. Both'
    END AS cdm2_status,

    CASE
        WHEN bl_cp_no IS NULL AND el_cp_no IS NULL THEN '1. Neither'
        WHEN bl_cp_no IS NOT NULL AND el_cp_no IS NULL THEN '2. Only_BL'
        WHEN bl_cp_no IS NULL AND el_cp_no IS NOT NULL THEN '3. Only_EL'
        WHEN bl_cp_no IS NOT NULL AND el_cp_no IS NOT NULL THEN '4. Both'
    END AS cp_status,

    CASE
        WHEN bl_cs_no IS NULL AND el_cs_no IS NULL THEN '1. Neither'
        WHEN bl_cs_no IS NOT NULL AND el_cs_no IS NULL THEN '2. Only_BL'
        WHEN bl_cs_no IS NULL AND el_cs_no IS NOT NULL THEN '3. Only_EL'
        WHEN bl_cs_no IS NOT NULL AND el_cs_no IS NOT NULL THEN '4. Both'
    END AS cs_status,

    CASE
        WHEN bl_fp_no IS NULL AND el_fp_no IS NULL THEN '1. Neither'
        WHEN bl_fp_no IS NOT NULL AND el_fp_no IS NULL THEN '2. Only_BL'
        WHEN bl_fp_no IS NULL AND el_fp_no IS NOT NULL THEN '3. Only_EL'
        WHEN bl_fp_no IS NOT NULL AND el_fp_no IS NOT NULL THEN '4. Both'
    END AS fp_status,

    CASE
        WHEN saf_no IS NULL AND sar_no IS NULL THEN '1. Neither'
        WHEN saf_no IS NOT NULL AND sar_no IS NULL THEN '2. Only_SAF'
        WHEN saf_no IS NULL AND sar_no IS NOT NULL THEN '3. Only_SAR'
        WHEN saf_no IS NOT NULL AND sar_no IS NOT NULL THEN '4. Both'
    END AS quiz_status,

    bl_cdm1_no,
    el_cdm1_no,
    bl_cdm2_no,
    el_cdm2_no,
    bl_cp_no,
    el_cp_no,
    bl_cs_no,
    el_cs_no,
    bl_fp_no,
    el_fp_no,
    saf_no,
    sar_no,

    cdm1_bl_created_on,
    cdm1_el_created_on,
    cdm2_bl_created_on,
    cdm2_el_created_on,
    cp_bl_created_on,
    cp_el_created_on,
    cs_bl_created_on,
    cs_el_created_on,
    fp_bl_created_on,
    fp_el_created_on,
    saf_created_on,
    sar_created_on,

    cdm1_bl_is_non_null,
    cdm1_el_is_non_null,
    cdm2_bl_is_non_null,
    cdm2_el_is_non_null,
    cp_bl_is_non_null,
    cp_el_is_non_null,
    cs_bl_is_non_null,
    cs_el_is_non_null,
    fp_bl_is_non_null,
    fp_el_is_non_null,
    saf_is_non_null,
    sar_is_non_null,

    saf_atleast_one_interest,
    saf_atleast_one_aptitude,
    saf_atleast_one_quiz,
    saf_atleast_one_feedback,
    sar_atleast_one_quiz,
    sar_atleast_one_reality

FROM t0
)

SELECT * FROM t1