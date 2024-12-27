WITH t1 AS (
    SELECT 
        student_id, 
        assessment_barcode, 
        bl_assessment_academic_year, 
        el_assessment_academic_year, 
        bl_q4_1, 
        bl_q4_2, 
        el_q4_1, 
        el_q4_2, 
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        batch_language, 
        school_state, 
        school_district, 
        school_taluka, 
        school_partner, 
        batch_donor, 
        gender
    FROM {{ ref('int_student_global_cdm1_pivot') }}
),

b AS (
    SELECT 
        student_id, 
        assessment_barcode AS bl_assessment_barcode, 
        bl_assessment_academic_year, 
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        batch_language, 
        school_state, 
        school_district, 
        school_taluka, 
        school_partner, 
        batch_donor, 
        gender, 
        baseline_stud_aspiration,
        bl_aspiration
    FROM t1
    UNPIVOT (
        baseline_stud_aspiration FOR bl_aspiration IN (bl_q4_1, bl_q4_2)
    ) AS unpvt1
),

t3 AS (
    SELECT 
        b.*,
        CASE 
            WHEN bl_aspiration = 'bl_q4_1' THEN 'q4_1'
            WHEN bl_aspiration = 'bl_q4_2' THEN 'q4_2'
            ELSE NULL
        END AS aspiration_mapping
    FROM b
)

select 
student_id, 
        bl_assessment_barcode, 
        bl_assessment_academic_year, 
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        batch_language, 
        school_state, 
        school_district, 
        school_taluka, 
        school_partner, 
        batch_donor, 
        gender, 
        baseline_stud_aspiration,
        aspiration_mapping
 from t3

