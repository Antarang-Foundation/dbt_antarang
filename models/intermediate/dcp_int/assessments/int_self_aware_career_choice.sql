WITH t1 AS (
    SELECT * FROM {{ref('stg_assessment_marks')}}
),

t2 AS (
    SELECT 
        student_id, student_barcode, student_grade, possible_career_report,
        batch_no, batch_academic_year, batch_grade, 
        school_state, school_district, school_taluka, school_partner, batch_donor
    FROM {{ ref('int_student_global') }}
),

t3 AS (
    SELECT * FROM t1 INNER JOIN t2 ON t1.assessment_student_id = t2.student_id
), 

t4 AS (
    SELECT 
        student_barcode, 
        possible_career_report, 
        percentage_student_ca_track, 
        percentage_student_cp_track, 
        percentage_student_sa_track,
        assessment_marks_number, 
        assessment_record_type,  
        total_marks,
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        school_state, 
        school_district, 
        school_taluka, 
        school_partner, 
        batch_donor,
        CASE     --this case will not work because possible_career_report & percentage_student_sa_track are both not string or not integer. both fields data type is different.
            WHEN batch_academic_year <= 2021 THEN possible_career_report
            WHEN batch_academic_year = 2021 THEN CAST(percentage_student_sa_track as STRING)
            ELSE 0
        END AS self_aware_career_choice
    FROM t3
)

SELECT * FROM t4
