WITH int_student_global AS (

    SELECT
        student_id, student_barcode, gender, birth_date, student_age, religion, caste, father_education, father_occupation,
        mother_education, mother_occupation, batch_no, batch_academic_year, batch_language, facilitator_id, facilitator_name,
        facilitator_email, school_id, school_name, school_taluka, school_ward, school_district, school_state, school_partner,
        school_area, donor_id, batch_donor, batch_grade
    FROM {{ ref('dev_int_global_dcp') }}

),

diagnostics_base AS (

    SELECT

        a.created_by_date, a.created_from_form, a.diagnostics_id, a.sd_no, a.record_type, a.record_type_id, a.assessment_barcode,
        a.assessment_grade,

        CASE
            WHEN LOWER(a.record_type) = 'baseline' THEN 'Baseline'
            WHEN LOWER(a.record_type) = 'endline' THEN 'Endline'
        END AS assessment_type,

        ROW_NUMBER() OVER (
            PARTITION BY
                a.assessment_barcode,
                a.assessment_grade,
                CASE
                    WHEN LOWER(a.record_type) = 'baseline' THEN 'Baseline'
                    WHEN LOWER(a.record_type) = 'endline' THEN 'Endline'
                END
            ORDER BY a.created_by_date DESC
        ) AS rn,

        COALESCE(a.a_q1_marks, a.b_q1_marks) AS q1,
        COALESCE(a.a_q2_marks, a.b_q2_marks) AS q2,
        COALESCE(a.a_q3_marks, a.b_q3_marks) AS q3,
        COALESCE(a.a_q4_marks, a.b_q4_marks) AS q4,
        COALESCE(a.a_q5_marks, a.b_q5_marks) AS q5,
        COALESCE(a.a_q6_marks, a.b_q6_marks) AS q6,
        COALESCE(a.a_q7_marks, a.b_q7_marks) AS q7,
        COALESCE(a.a_q8_marks, a.b_q8_marks) AS q8,
        COALESCE(a.a_q9_marks, a.b_q9_marks) AS q9,
        COALESCE(a.a_q10_marks, a.b_q10_marks) AS q10

    FROM {{ ref('stg_student_diagnostic') }} a

),

latest_diagnostics AS (

    SELECT *
    FROM diagnostics_base
    WHERE rn = 1

),

diagnostics_scored AS (

    SELECT

        g.*,

        d.created_by_date, d.created_from_form, d.diagnostics_id, d.sd_no, d.record_type, d.record_type_id, d.assessment_barcode,
        d.assessment_grade, d.assessment_type, d.q1, d.q2, d.q3, d.q4, d.q5, d.q6, d.q7, d.q8, d.q9, d.q10,


        CASE
            WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
                CASE
                    WHEN d.q1 = 3 THEN 'Emerging Awareness'
                    WHEN d.q1 = 2 THEN 'Developing Awareness'
                    WHEN d.q1 = 1 THEN 'Low awareness'
                    ELSE 'DNA'
                END
            ELSE
                CASE
                    WHEN d.q1 = 4 THEN 'High Readiness'
                    WHEN d.q1 = 3 THEN 'Emerging Readiness'
                    WHEN d.q1 = 2 THEN 'Developing Readiness'
                    WHEN d.q1 = 1 THEN 'Low Readiness'
                    ELSE 'DNA'
                END
        END AS q1_bucket,

        CASE
            WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
                CASE
                    WHEN d.q2 = 3 THEN 'Emerging Awareness'
                    WHEN d.q2 = 2 THEN 'Developing Awareness'
                    WHEN d.q2 = 1 THEN 'Low awareness'
                    ELSE 'DNA'
                END
            ELSE
                CASE
                    WHEN d.q2 = 4 THEN 'High Readiness'
                    WHEN d.q2 = 3 THEN 'Emerging Readiness'
                    WHEN d.q2 = 2 THEN 'Developing Readiness'
                    WHEN d.q2 = 1 THEN 'Low Readiness'
                    ELSE 'DNA'
                END
        END AS q2_bucket,

        CASE
            WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
                CASE
                    WHEN d.q3 = 3 THEN 'Emerging Awareness'
                    WHEN d.q3 = 2 THEN 'Developing Awareness'
                    WHEN d.q3 = 1 THEN 'Low awareness'
                    ELSE 'DNA'
                END
            ELSE
                CASE
                    WHEN d.q3 = 4 THEN 'High Readiness'
                    WHEN d.q3 = 3 THEN 'Emerging Readiness'
                    WHEN d.q3 = 2 THEN 'Developing Readiness'
                    WHEN d.q3 = 1 THEN 'Low Readiness'
                    ELSE 'DNA'
                END
        END AS q3_bucket,

        CASE
            WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
                CASE
                    WHEN d.q4 = 3 THEN 'Emerging Awareness'
                    WHEN d.q4 = 2 THEN 'Developing Awareness'
                    WHEN d.q4 = 1 THEN 'Low awareness'
                    ELSE 'DNA'
                END
            ELSE
                CASE
                    WHEN d.q4 = 4 THEN 'High Readiness'
                    WHEN d.q4 = 3 THEN 'Emerging Readiness'
                    WHEN d.q4 = 2 THEN 'Developing Readiness'
                    WHEN d.q4 = 1 THEN 'Low Readiness'
                    ELSE 'DNA'
                END
        END AS q4_bucket,
        CASE
    WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
        CASE
            WHEN d.q5 = 3 THEN 'Emerging Awareness'
            WHEN d.q5 = 2 THEN 'Developing Awareness'
            WHEN d.q5 = 1 THEN 'Low awareness'
            ELSE 'DNA'
        END
    ELSE
        CASE
            WHEN d.q5 = 4 THEN 'High Readiness'
            WHEN d.q5 = 3 THEN 'Emerging Readiness'
            WHEN d.q5 = 2 THEN 'Developing Readiness'
            WHEN d.q5 = 1 THEN 'Low Readiness'
            ELSE 'DNA'
        END
END AS q5_bucket,

CASE
    WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
        CASE
            WHEN d.q6 = 3 THEN 'Emerging Awareness'
            WHEN d.q6 = 2 THEN 'Developing Awareness'
            WHEN d.q6 = 1 THEN 'Low awareness'
            ELSE 'DNA'
        END
    ELSE
        CASE
            WHEN d.q6 = 4 THEN 'High Readiness'
            WHEN d.q6 = 3 THEN 'Emerging Readiness'
            WHEN d.q6 = 2 THEN 'Developing Readiness'
            WHEN d.q6 = 1 THEN 'Low Readiness'
            ELSE 'DNA'
        END
END AS q6_bucket,

CASE
    WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
        CASE
            WHEN d.q7 = 3 THEN 'Emerging Awareness'
            WHEN d.q7 = 2 THEN 'Developing Awareness'
            WHEN d.q7 = 1 THEN 'Low awareness'
            ELSE 'DNA'
        END
    ELSE
        CASE
            WHEN d.q7 = 4 THEN 'High Readiness'
            WHEN d.q7 = 3 THEN 'Emerging Readiness'
            WHEN d.q7 = 2 THEN 'Developing Readiness'
            WHEN d.q7 = 1 THEN 'Low Readiness'
            ELSE 'DNA'
        END
END AS q7_bucket,

CASE
    WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
        CASE
            WHEN d.q8 = 3 THEN 'Emerging Awareness'
            WHEN d.q8 = 2 THEN 'Developing Awareness'
            WHEN d.q8 = 1 THEN 'Low awareness'
            ELSE 'DNA'
        END
    ELSE
        CASE
            WHEN d.q8 = 4 THEN 'High Readiness'
            WHEN d.q8 = 3 THEN 'Emerging Readiness'
            WHEN d.q8 = 2 THEN 'Developing Readiness'
            WHEN d.q8 = 1 THEN 'Low Readiness'
            ELSE 'DNA'
        END
END AS q8_bucket,

CASE
    WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
        CASE
            WHEN d.q9 = 3 THEN 'Emerging Awareness'
            WHEN d.q9 = 2 THEN 'Developing Awareness'
            WHEN d.q9 = 1 THEN 'Low awareness'
            ELSE 'DNA'
        END
    ELSE
        CASE
            WHEN d.q9 = 4 THEN 'High Readiness'
            WHEN d.q9 = 3 THEN 'Emerging Readiness'
            WHEN d.q9 = 2 THEN 'Developing Readiness'
            WHEN d.q9 = 1 THEN 'Low Readiness'
            ELSE 'DNA'
        END
END AS q9_bucket,

CASE
    WHEN d.assessment_grade IN ('Grade 9', 'Grade 10') THEN
        CASE
            WHEN d.q10 = 3 THEN 'Emerging Awareness'
            WHEN d.q10 = 2 THEN 'Developing Awareness'
            WHEN d.q10 = 1 THEN 'Low awareness'
            ELSE 'DNA'
        END
    ELSE
        CASE
            WHEN d.q10 = 4 THEN 'High Readiness'
            WHEN d.q10 = 3 THEN 'Emerging Readiness'
            WHEN d.q10 = 2 THEN 'Developing Readiness'
            WHEN d.q10 = 1 THEN 'Low Readiness'
            ELSE 'DNA'
        END
END AS q10_bucket,

        (
            IFNULL(d.q1,0) +
            IFNULL(d.q2,0) +
            IFNULL(d.q3,0) +
            IFNULL(d.q4,0) +
            IFNULL(d.q5,0) +
            IFNULL(d.q6,0) +
            IFNULL(d.q7,0) +
            IFNULL(d.q8,0) +
            IFNULL(d.q9,0) +
            IFNULL(d.q10,0)
        )

        /

        NULLIF(

            (CASE WHEN d.q1 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q2 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q3 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q4 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q5 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q6 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q7 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q8 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q9 IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN d.q10 IS NOT NULL THEN 1 ELSE 0 END),

        0)

        AS mean_score,

        (
            SELECT STDDEV_POP(val)
            FROM UNNEST([
                d.q1,d.q2,d.q3,d.q4,d.q5,
                d.q6,d.q7,d.q8,d.q9,d.q10
            ]) val
            WHERE val IS NOT NULL
        ) AS sd_score

    FROM latest_diagnostics d

    LEFT JOIN int_student_global g
        ON d.assessment_barcode = g.student_barcode

),

baseline AS (

    SELECT *
    FROM diagnostics_scored
    WHERE assessment_type = 'Baseline'

),

endline AS (

    SELECT *
    FROM diagnostics_scored
    WHERE assessment_type = 'Endline'

)

SELECT

    COALESCE(bl.student_id, el.student_id) AS student_id,
    COALESCE(bl.student_barcode, el.student_barcode) AS student_barcode,

    COALESCE(bl.gender, el.gender) AS gender,
    COALESCE(bl.birth_date, el.birth_date) AS birth_date,
    COALESCE(bl.student_age, el.student_age) AS student_age,

    COALESCE(bl.religion, el.religion) AS religion,
    COALESCE(bl.caste, el.caste) AS caste,

    COALESCE(bl.father_education, el.father_education) AS father_education,
    COALESCE(bl.father_occupation, el.father_occupation) AS father_occupation,

    COALESCE(bl.mother_education, el.mother_education) AS mother_education,
    COALESCE(bl.mother_occupation, el.mother_occupation) AS mother_occupation,

    COALESCE(bl.batch_no, el.batch_no) AS batch_no,
    COALESCE(bl.batch_academic_year, el.batch_academic_year) AS batch_academic_year,
    COALESCE(bl.batch_language, el.batch_language) AS batch_language,

    ------------------------------------------------
    -- BASELINE
    ------------------------------------------------

    bl.created_by_date AS bl_created_by_date,
    bl.sd_no AS bl_sd_no,

    bl.q1 AS bl_q1,
    bl.q1_bucket AS bl_q1_bucket,

    bl.q2 AS bl_q2,
    bl.q2_bucket AS bl_q2_bucket,

    bl.q3 AS bl_q3,
    bl.q3_bucket AS bl_q3_bucket,

    bl.q4 AS bl_q4,
    bl.q4_bucket AS bl_q4_bucket,

    bl.q5 AS bl_q5,
    bl.q5_bucket AS bl_q5_bucket,
    bl.q6 AS bl_q6,
    bl.q6_bucket AS bl_q6_bucket,
    bl.q7 AS bl_q7,
    bl.q7_bucket AS bl_q7_bucket,
    bl.q8 AS bl_q8,
    bl.q8_bucket AS bl_q8_bucket,
    bl.q9 AS bl_q9,
    bl.q9_bucket AS bl_q9_bucket,
    bl.q10 AS bl_q10,
    bl.q10_bucket AS bl_q10_bucket,

    bl.mean_score AS bl_mean_score,
    bl.sd_score AS bl_sd_score,

    ------------------------------------------------
    -- ENDLINE
    ------------------------------------------------

    el.created_by_date AS el_created_by_date,
    el.sd_no AS el_sd_no,

    el.q1 AS el_q1,
    el.q1_bucket AS el_q1_bucket,

    el.q2 AS el_q2,
    el.q2_bucket AS el_q2_bucket,

    el.q3 AS el_q3,
    el.q3_bucket AS el_q3_bucket,

    el.q4 AS el_q4,
    el.q4_bucket AS el_q4_bucket,

    el.q5 AS el_q5,
    el.q5_bucket AS el_q5_bucket,
    el.q6 AS el_q6,
    el.q6_bucket AS el_q6_bucket,
    el.q7 AS el_q7,
    el.q7_bucket AS el_q7_bucket,
    el.q8 AS el_q8,
    el.q8_bucket AS el_q8_bucket,
    el.q9 AS el_q9,
    el.q9_bucket AS el_q9_bucket,
    el.q10 AS el_q10,
    el.q10_bucket AS el_q10_bucket,

    el.mean_score AS el_mean_score,
    el.sd_score AS el_sd_score

FROM baseline bl

FULL OUTER JOIN endline el
    ON bl.student_barcode = el.student_barcode