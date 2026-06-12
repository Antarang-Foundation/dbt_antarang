WITH latest_assessment AS (
    SELECT *
    FROM {{ ref('int_student_global_cdm2_ind') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY student_id,
                     assessment_academic_year,
                     record_type
        ORDER BY created_on DESC
    ) = 1
),

baseline AS (
    SELECT *
    FROM latest_assessment
    WHERE LOWER(record_type) = 'baseline'
),

endline AS (
    SELECT *
    FROM latest_assessment
    WHERE LOWER(record_type) = 'endline'
)

SELECT
    COALESCE(bl.student_id, el.student_id) AS student_id,
    COALESCE(bl.student_barcode, el.student_barcode) AS student_barcode,

    CASE
        WHEN LOWER(COALESCE(bl.gender, el.gender)) = 'female'
            THEN 'Female'
        WHEN LOWER(COALESCE(bl.gender, el.gender)) = 'male'
            THEN 'Male'
        WHEN LOWER(COALESCE(bl.gender, el.gender)) IN ('transgender','other')
            THEN 'Transgender/Other'
        ELSE 'DNA'
    END AS gender,

    SAFE_CAST(
        COALESCE(bl.birth_date, el.birth_date)
        AS DATE
    ) AS dob,

    DATE_DIFF(
        CURRENT_DATE(),
        SAFE_CAST(COALESCE(bl.birth_date, el.birth_date) AS DATE),
        YEAR
    ) AS age,

    COALESCE(bl.religion, el.religion) AS religion,
    COALESCE(bl.caste, el.caste) AS caste,

    COALESCE(bl.father_education, el.father_education) AS father_education,
    COALESCE(bl.father_occupation, el.father_occupation) AS father_occupation,
    COALESCE(bl.mother_education, el.mother_education) AS mother_education,
    COALESCE(bl.mother_occupation, el.mother_occupation) AS mother_occupation,

    COALESCE(bl.batch_no, el.batch_no) AS batch_no,
    COALESCE(bl.batch_academic_year, el.batch_academic_year) AS batch_academic_year,
    COALESCE(bl.batch_language, el.batch_language) AS batch_language,

    COALESCE(bl.facilitator_id, el.facilitator_id) AS facilitator_id,
    COALESCE(bl.facilitator_name, el.facilitator_name) AS facilitator_name,
    COALESCE(bl.facilitator_email, el.facilitator_email) AS facilitator_email,

    COALESCE(bl.school_id, el.school_id) AS school_id,
    COALESCE(bl.school_name, el.school_name) AS school_name,
    COALESCE(bl.school_taluka, el.school_taluka) AS school_taluka,
    COALESCE(bl.school_ward, el.school_ward) AS school_ward,
    COALESCE(bl.school_district, el.school_district) AS school_district,
    COALESCE(bl.school_state, el.school_state) AS school_state,
    COALESCE(bl.school_partner, el.school_partner) AS school_partner,

    COALESCE(bl.school_area, el.school_area) AS school_area,

    COALESCE(bl.donor_id, el.donor_id) AS donor_id,
    COALESCE(bl.batch_donor, el.batch_donor) AS batch_donor,
    COALESCE(bl.batch_grade, el.batch_grade) AS batch_grade,

    ------------------------------------------------------------------
    -- BASELINE
    ------------------------------------------------------------------

    bl.assessment_barcode AS assessment_barcode,

    SAFE_CAST(bl.created_on AS TIMESTAMP) AS bl_createddate,

    bl.cdm2_no AS bl_ca2_no,

    bl.q5_category AS bl_q3_1,
    NULL AS bl_q3_1_marks,

    bl.q6_1_category AS bl_q3_2,
    NULL AS bl_q3_2_marks,

    bl.q6_2_category AS bl_q3_3,
    NULL AS bl_q3_3_marks,

    bl.q6_3_category AS bl_q3_4,
    NULL AS bl_q3_4_marks,

    bl.q6_4_category AS bl_q3_5,
    NULL AS bl_q3_5_marks,

    bl.q6_5_category AS bl_q3_6,
    NULL AS bl_q3_6_marks,

    bl.q6_6_category AS bl_q3_7,
    NULL AS bl_q3_7_marks,

    bl.q6_7_category AS bl_q3_8,
    NULL AS bl_q3_8_marks,

    bl.q6_8_category AS bl_q3_9,
    NULL AS bl_q3_9_marks,

    bl.q6_9_category AS bl_q3_10,
    NULL AS bl_q3_10_marks,

    SAFE_CAST(bl.cdm2_total_marks AS INT64) AS bl_q3_total_marks,

    CASE
        WHEN SAFE_CAST(bl.cdm2_total_marks AS INT64) >= 0
         AND SAFE_CAST(bl.cdm2_total_marks AS INT64) < 10
            THEN '0-2 Career Tracks'

        WHEN SAFE_CAST(bl.cdm2_total_marks AS INT64) >= 10
         AND SAFE_CAST(bl.cdm2_total_marks AS INT64) < 20
            THEN '2-4 Career Tracks'

        WHEN SAFE_CAST(bl.cdm2_total_marks AS INT64) >= 20
         AND SAFE_CAST(bl.cdm2_total_marks AS INT64) < 30
            THEN '4-6 Career Tracks'

        WHEN SAFE_CAST(bl.cdm2_total_marks AS INT64) >= 30
         AND SAFE_CAST(bl.cdm2_total_marks AS INT64) < 40
            THEN '6-8 Career Tracks'

        WHEN SAFE_CAST(bl.cdm2_total_marks AS INT64) BETWEEN 40 AND 50
            THEN '8-10 Career Tracks'

        ELSE 'DNA'
    END AS bl_q3_bucket,

    ------------------------------------------------------------------
    -- ENDLINE
    ------------------------------------------------------------------

    SAFE_CAST(el.created_on AS TIMESTAMP) AS el_createddate,

    el.cdm2_no AS el_ca2_no,

    el.q5_category AS el_q3_1,
    NULL AS el_q3_1_marks,

    el.q6_1_category AS el_q3_2,
    NULL AS el_q3_2_marks,

    el.q6_2_category AS el_q3_3,
    NULL AS el_q3_3_marks,

    el.q6_3_category AS el_q3_4,
    NULL AS el_q3_4_marks,

    el.q6_4_category AS el_q3_5,
    NULL AS el_q3_5_marks,

    el.q6_5_category AS el_q3_6,
    NULL AS el_q3_6_marks,

    el.q6_6_category AS el_q3_7,
    NULL AS el_q3_7_marks,

    el.q6_7_category AS el_q3_8,
    NULL AS el_q3_8_marks,

    el.q6_8_category AS el_q3_9,
    NULL AS el_q3_9_marks,

    el.q6_9_category AS el_q3_10,
    NULL AS el_q3_10_marks,

    SAFE_CAST(el.cdm2_total_marks AS INT64) AS el_q3_total_marks,

    el.overall_bucket AS el_q3_bucket,

    ------------------------------------------------------------------
    -- IMPROVEMENT
    ------------------------------------------------------------------

    SAFE_CAST(el.cdm2_total_marks AS INT64)
      - SAFE_CAST(bl.cdm2_total_marks AS INT64)
        AS bl_el_q3_total_marks,

    CASE
        WHEN bl.student_id IS NOT NULL
         AND el.student_id IS NOT NULL
            THEN 'Both'

        WHEN bl.student_id IS NOT NULL
            THEN 'Baseline'

        WHEN el.student_id IS NOT NULL
            THEN 'Endline'

        ELSE 'DNA'
    END AS type_of_assessment_filled

FROM baseline bl
FULL OUTER JOIN endline el
    ON bl.student_id = el.student_id
   AND bl.assessment_academic_year = el.assessment_academic_year