WITH int_student_global AS (

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

),

stg_ca AS (

    SELECT *
    FROM {{ ref('stg_ca') }}

),

bl AS (

    SELECT *
    FROM (

        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY assessment_student_id
                ORDER BY created_on DESC, ca1_id DESC
            ) AS rn
        FROM stg_ca
        WHERE record_type = 'Baseline'

    )

    WHERE rn = 1

),

el AS (

    SELECT *
    FROM (

        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY assessment_student_id
                ORDER BY created_on DESC, ca1_id DESC
            ) AS rn
        FROM stg_ca
        WHERE record_type = 'Endline'

    )

    WHERE rn = 1

)
SELECT

    s.student_id,
    s.student_barcode,
    s.gender,
    s.batch_no,
    s.batch_academic_year,
    s.batch_language,
    s.facilitator_id,
    s.facilitator_name,
    s.facilitator_email,
    s.school_id,
    s.school_name,
    s.school_taluka,
    s.school_ward,
    s.school_district,
    s.school_state,
    s.school_partner,
    s.school_area,
    s.donor_id,
    s.batch_donor,
    s.batch_grade,

    COALESCE(bl.assessment_barcode, el.assessment_barcode) AS assessment_barcode,

    /* ================= BL ================= */

    bl.created_on AS bl_createddate,
    bl.ca1_no AS bl_ca1_no,

    CASE WHEN REGEXP_CONTAINS(bl.q2_a, r'(^|,)A(,|$)') THEN 1 END AS bl_2a_a,
    CASE WHEN REGEXP_CONTAINS(bl.q2_a, r'(^|,)B(,|$)') THEN 1 END AS bl_2a_b,
    CASE WHEN REGEXP_CONTAINS(bl.q2_a, r'(^|,)C(,|$)') THEN 1 END AS bl_2a_c,
    CASE WHEN REGEXP_CONTAINS(bl.q2_a, r'(^|,)D(,|$)') THEN 1 END AS bl_2a_d,
    CASE WHEN REGEXP_CONTAINS(bl.q2_a, r'(^|,)E(,|$)') THEN 1 END AS bl_2a_e,
    CASE WHEN REGEXP_CONTAINS(bl.q2_a, r'(^|,)F(,|$)') THEN 1 END AS bl_2a_f,
    CASE WHEN REGEXP_CONTAINS(bl.q2_a, r'(^|,)G(,|$)') THEN 1 END AS bl_2a_g,
    CASE WHEN REGEXP_CONTAINS(bl.q2_a, r'(^|,)H(,|$)') THEN 1 END AS bl_2a_h,

    CASE WHEN REGEXP_CONTAINS(bl.q2_b, r'(^|,)A(,|$)') THEN 1 END AS bl_2b_a,
    CASE WHEN REGEXP_CONTAINS(bl.q2_b, r'(^|,)B(,|$)') THEN 1 END AS bl_2b_b,
    CASE WHEN REGEXP_CONTAINS(bl.q2_b, r'(^|,)C(,|$)') THEN 1 END AS bl_2b_c,
    CASE WHEN REGEXP_CONTAINS(bl.q2_b, r'(^|,)D(,|$)') THEN 1 END AS bl_2b_d,
    CASE WHEN REGEXP_CONTAINS(bl.q2_b, r'(^|,)E(,|$)') THEN 1 END AS bl_2b_e,
    CASE WHEN REGEXP_CONTAINS(bl.q2_b, r'(^|,)F(,|$)') THEN 1 END AS bl_2b_f,
    CASE WHEN REGEXP_CONTAINS(bl.q2_b, r'(^|,)G(,|$)') THEN 1 END AS bl_2b_g,

    /* ================= EL ================= */

    el.created_on AS el_createddate,
    el.ca1_no AS el_ca1_no,

    CASE WHEN REGEXP_CONTAINS(el.q2_a, r'(^|,)A(,|$)') THEN 1 END AS el_q2_a_a,
    CASE WHEN REGEXP_CONTAINS(el.q2_a, r'(^|,)B(,|$)') THEN 1 END AS el_q2_a_b,
    CASE WHEN REGEXP_CONTAINS(el.q2_a, r'(^|,)C(,|$)') THEN 1 END AS el_q2_a_c,
    CASE WHEN REGEXP_CONTAINS(el.q2_a, r'(^|,)D(,|$)') THEN 1 END AS el_q2_a_d,
    CASE WHEN REGEXP_CONTAINS(el.q2_a, r'(^|,)E(,|$)') THEN 1 END AS el_q2_a_e,
    CASE WHEN REGEXP_CONTAINS(el.q2_a, r'(^|,)F(,|$)') THEN 1 END AS el_q2_a_f,
    CASE WHEN REGEXP_CONTAINS(el.q2_a, r'(^|,)G(,|$)') THEN 1 END AS el_q2_a_g,
    CASE WHEN REGEXP_CONTAINS(el.q2_a, r'(^|,)H(,|$)') THEN 1 END AS el_q2_a_h,

    CASE WHEN REGEXP_CONTAINS(el.q2_b, r'(^|,)A(,|$)') THEN 1 END AS el_q2_b_a,
    CASE WHEN REGEXP_CONTAINS(el.q2_b, r'(^|,)B(,|$)') THEN 1 END AS el_q2_b_b,
    CASE WHEN REGEXP_CONTAINS(el.q2_b, r'(^|,)C(,|$)') THEN 1 END AS el_q2_b_c,
    CASE WHEN REGEXP_CONTAINS(el.q2_b, r'(^|,)D(,|$)') THEN 1 END AS el_q2_b_d,
    CASE WHEN REGEXP_CONTAINS(el.q2_b, r'(^|,)E(,|$)') THEN 1 END AS el_q2_b_e,
    CASE WHEN REGEXP_CONTAINS(el.q2_b, r'(^|,)F(,|$)') THEN 1 END AS el_q2_b_f,
    CASE WHEN REGEXP_CONTAINS(el.q2_b, r'(^|,)G(,|$)') THEN 1 END AS el_q2_b_g

FROM int_student_global s

LEFT JOIN bl
    ON s.student_id = bl.assessment_student_id

LEFT JOIN el
    ON s.student_id = el.assessment_student_id

WHERE s.batch_academic_year >= 2026