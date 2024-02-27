with
    t0 as (
        SELECT 
                barcode, cdm1_no, q1, q1_marks, q2_1, q2_2, q2_marks, q3_1, q3_2, q3_marks, q4_1, q4_1_marks, q4_2, q4_2_marks, q4_marks, cdm1_total_marks, 
                assessment_batch_id, record_type, created_on, assessment_grade, assessment_academic_year, created_from_form
        FROM {{ ref('int_cdm1_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(cdm1_no) as cdm1_no, max(assessment_batch_id) as assessment_batch_id, max(created_on) as created_on, max(created_from_form) as created_from_form, 
            max(assessment_grade) as assessment_grade, max(assessment_academic_year) as assessment_academic_year,
            max(q1) as q1, max(q1_marks) as q1_marks, max(q2_1) as q2_1, max(q2_2) as q2_2, 
            max(q2_marks) as q2_marks, max(q3_1) as q3_1, max(q3_2) as q3_2, max(q3_marks) as q3_marks, max(q4_1) as q4_1, max(q4_1_marks) as q4_1_marks, 
            max(q4_2) as q4_2, max(q4_2_marks) as q4_2_marks, max(q4_marks) as q4_marks, max(cdm1_total_marks) as cdm1_total_marks

            FOR record_type IN ('Baseline', 'Endline')
            )
    )
    
SELECT *
From t1

--where student_barcode = '220018166'