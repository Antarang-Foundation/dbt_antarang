WITH assessments AS (

    SELECT
        cs_id,
        assessment_barcode,
        record_type,
        created_on,
        cs_no,
        assessment_grade,
        assessment_academic_year,
        assessment_batch_id,

        q11_1,
        q11_2,
        q11_3,
        q11_4,
        q11_5,
        q11_6,
        q11_7,
        q11_8,
        q11_9,
        q11_10,

        q15_1,
        q15_2,
        q15_3,
        q15_4,
        q15_5,
        q15_6,
        q15_7,
        q15_8,
        q15_9

    FROM {{ ref('stg_cs') }}

    WHERE data_cleanup = TRUE
      AND marks_recalculated = TRUE
      AND student_linked = TRUE
      AND SAFE_CAST(assessment_academic_year AS INT64) >= 2026

),

students AS (

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

bl AS (

    SELECT *
    FROM (

        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    assessment_barcode,
                    assessment_academic_year
                ORDER BY created_on DESC
            ) AS rn

        FROM assessments

        WHERE LOWER(record_type) = 'baseline'

    )

    WHERE rn = 1

),

el AS (

    SELECT *
    FROM (

        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    assessment_barcode,
                    assessment_academic_year
                ORDER BY created_on DESC
            ) AS rn

        FROM assessments

        WHERE LOWER(record_type) = 'endline'

    )

    WHERE rn = 1

),

final as (SELECT

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

    --------------------------------------------------
    -- BASELINE
    --------------------------------------------------

    bl.assessment_barcode,
    bl.created_on AS bl_CreatedDate,
    bl.cs_no AS bl_cs_no,

    bl.q11_1 AS bl_q4_1,
    bl.q11_2 AS bl_q4_2,
    bl.q11_3 AS bl_q4_3,
    bl.q11_4 AS bl_q4_4,
    bl.q11_5 AS bl_q4_5,
    bl.q11_6 AS bl_q4_6,
    bl.q11_7 AS bl_q4_7,
    bl.q11_8 AS bl_q4_8,
    bl.q11_9 AS bl_q4_9,
    bl.q11_10 AS bl_q4_10,

    bl.q15_1 AS bl_q5_1,
    bl.q15_2 AS bl_q5_2,
    bl.q15_3 AS bl_q5_3,
    bl.q15_4 AS bl_q5_4,
    bl.q15_5 AS bl_q5_5,
    bl.q15_6 AS bl_q5_6,
    bl.q15_7 AS bl_q5_7,
    bl.q15_8 AS bl_q5_8,
    bl.q15_9 AS bl_q5_9,

    --------------------------------------------------
    -- ENDLINE
    --------------------------------------------------

    el.created_on AS el_CreatedDate,
    el.cs_no AS el_cs_no,

    el.q11_1 AS el_q4_1,
    el.q11_2 AS el_q4_2,
    el.q11_3 AS el_q4_3,
    el.q11_4 AS el_q4_4,
    el.q11_5 AS el_q4_5,
    el.q11_6 AS el_q4_6,
    el.q11_7 AS el_q4_7,
    el.q11_8 AS el_q4_8,
    el.q11_9 AS el_q4_9,
    el.q11_10 AS el_q4_10,

    el.q15_1 AS el_q5_1,
    el.q15_2 AS el_q5_2,
    el.q15_3 AS el_q5_3,
    el.q15_4 AS el_q5_4,
    el.q15_5 AS el_q5_5,
    el.q15_6 AS el_q5_6,
    el.q15_7 AS el_q5_7,
    el.q15_8 AS el_q5_8,
    el.q15_9 AS el_q5_9

FROM students s

LEFT JOIN bl
    ON s.student_barcode = bl.assessment_barcode
   AND SAFE_CAST(s.batch_academic_year AS INT64)
       = SAFE_CAST(bl.assessment_academic_year AS INT64)

LEFT JOIN el
    ON s.student_barcode = el.assessment_barcode
   AND SAFE_CAST(s.batch_academic_year AS INT64)
       = SAFE_CAST(el.assessment_academic_year AS INT64)

)

select * from final


