with
    t0 as (
        SELECT 
                barcode, cp_no, q7, q7_marks, q8, q8_marks, q9_1, q9_1_marks, q9_2, q9_2_marks, q9_3, q9_3_marks, q9_4, q9_4_marks, q9_5, q9_5_marks, 
                q9_6, q9_6_marks, q9_7, q9_7_marks, q10, q10_marks, cp_total_marks, assessment_batch_id, record_type, created_on, assessment_grade, assessment_academic_year, created_from_form

        FROM {{ ref('int_cp_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(cp_no) as cp_no, max(assessment_batch_id) as assessment_batch_id, max(created_on) as created_on, max(created_from_form) as created_from_form,
            max(assessment_grade) as assessment_grade, max(assessment_academic_year) as assessment_academic_year,
            max(q7) as q7, max(q7_marks) as q7_marks, max(q8) as q8, max(q8_marks) as q8_marks,
            max(q9_1) as q9_1, max(q9_1_marks) as q9_1_marks, max(q9_2) as q9_2, max(q9_2_marks) as q9_2_marks, max(q9_3) as q9_3, 
            max(q9_3_marks) as q9_3_marks, max(q9_4) as q9_4, max(q9_4_marks) as q9_4_marks, max(q9_5) as q9_5, max(q9_5_marks) as q9_5_marks, 
            max(q9_6) as q9_6, max(q9_6_marks) as q9_6_marks, max(q9_7) as q9_7, max(q9_7_marks) as q9_7_marks, max(q10) as q10, 
            max(q10_marks) as q10_marks, max(cp_total_marks) as cp_total_marks

            FOR record_type IN ('Baseline', 'Endline')
            )
    )
    
     
SELECT *
From t1






