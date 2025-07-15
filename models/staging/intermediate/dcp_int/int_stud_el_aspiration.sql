WITH t1 AS (
    SELECT 
        student_id, 
        student_barcode,
        assessment_barcode,  
        el_assessment_academic_year,  
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
        gender,
        el_cdm1_no
    FROM {{ ref('int_student_global_cdm1_pivot') }}
),

t2 AS (
    SELECT 
        student_id, 
        student_barcode,
        el_cdm1_no,
        assessment_barcode AS el_assessment_barcode, 
        el_assessment_academic_year,
        endline_stud_aspiration, 
        el_aspiration
    FROM t1
    UNPIVOT (
        endline_stud_aspiration FOR el_aspiration IN (el_q4_1, el_q4_2)
    ) AS unpvt2
),

t3 AS (
    SELECT 
        t2.*,
        CASE 
            WHEN el_aspiration = 'el_q4_1' THEN 'q4_1'
            WHEN el_aspiration = 'el_q4_2' THEN 'q4_2'
            ELSE 'DNA'
        END AS aspiration_mapping
    FROM t2
)

select 
        student_id as el_student_id, 
        el_cdm1_no,
        student_barcode,
        el_assessment_barcode, 
        el_assessment_academic_year,
        endline_stud_aspiration, 
        aspiration_mapping as el_aspiration_mapping
 from t3
