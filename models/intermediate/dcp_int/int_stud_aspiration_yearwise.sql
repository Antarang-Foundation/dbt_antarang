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

baseline_unpivot AS (
    SELECT 
        student_id, 
        assessment_barcode, 
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

endline_unpivot AS (
    SELECT 
        student_id, 
        assessment_barcode, 
        el_assessment_academic_year,
        endline_stud_aspiration, 
        el_aspiration
    FROM t1
    UNPIVOT (
        endline_stud_aspiration FOR el_aspiration IN (el_q4_1, el_q4_2)
    ) AS unpvt2
),

t2 AS (
    SELECT 
        b.student_id, 
        b.assessment_barcode, 
        b.bl_assessment_academic_year, 
        b.batch_no, 
        b.batch_academic_year, 
        b.batch_grade, 
        b.batch_language, 
        b.school_state, 
        b.school_district, 
        b.school_taluka, 
        b.school_partner, 
        b.batch_donor, 
        b.gender,
        b.baseline_stud_aspiration, 
        b.bl_aspiration,
        e.endline_stud_aspiration, 
        e.el_aspiration, 
        e.el_assessment_academic_year
    FROM baseline_unpivot AS b
    JOIN endline_unpivot AS e 
    ON b.student_id = e.student_id
)

SELECT * FROM t2
