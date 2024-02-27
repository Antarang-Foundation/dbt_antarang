with
    t0 as (
        SELECT 
                barcode, fp_no, q17, q17_marks, q18_1, q18_2, q18_3, q18_4, q18_5, q18_6, q18_7, q18_8, q18_9, q18_10, q18_11, q18_marks, 
                q19, q19_marks, q20, q20_marks, q21, q21_marks, q22, q22_marks, fp_total_marks, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, 
                assessment_batch_id, record_type, created_on, assessment_grade, assessment_academic_year, created_from_form

        FROM {{ ref('int_fp_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(fp_no) as fp_no, max(assessment_batch_id) as assessment_batch_id, max(created_on) as created_on, max(created_from_form) as created_from_form, 
            max(assessment_grade) as assessment_grade, max(assessment_academic_year) as assessment_academic_year,
            
            max(q17) as q17, max(q17_marks) as q17_marks,           
            
            max(q18_1) as q18_1, max(q18_2) as q18_2, max(q18_3) as q18_3, max(q18_4) as q18_4, max(q18_5) as q18_5, max(q18_6) as q18_6, 
            max(q18_7) as q18_7, max(q18_8) as q18_8, max(q18_9) as q18_9, max(q18_10) as q18_10, max(q18_11) as q18_11, max(q18_marks) as q18_marks, 

            max(q19) as q19, max(q19_marks) as q19_marks,  max(q20) as q20, max(q20_marks) as q20_marks,  max(q21) as q21, max(q21_marks) as q21_marks,  
            max(q22) as q22, max(q22_marks) as q22_marks,  max(fp_total_marks) as fp_total_marks,

            max(f1) as f1, max(f2) as f2, max(f3) as f3, max(f4) as f4, max(f5) as f5, max(f6) as f6, max(f7) as f7, max(f8) as f8, max(f9) as f9, 
            max(f10) as f10, max(f11) as f11, max(f12) as f12

            FOR record_type IN ('Baseline', 'Endline')
            )
    )
    
     
SELECT *
From t1