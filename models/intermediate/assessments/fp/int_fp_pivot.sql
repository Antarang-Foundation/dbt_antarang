with
    t0 as (
        SELECT 
                assessment_barcode, fp_no, q17, q17_marks, q18_1, q18_2, q18_3, q18_4, q18_5, q18_6, q18_7, q18_8, q18_9, q18_10, q18_11, q18_marks, 
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
    ),

    t2 as (select 

assessment_barcode, 

fp_no_Baseline as bl_fp_no, assessment_batch_id_Baseline as bl_assessment_batch_id, created_on_Baseline as bl_created_on, 
created_from_form_Baseline as bl_created_from_form, assessment_grade_Baseline as bl_assessment_grade, 
assessment_academic_year_Baseline as bl_assessment_academic_year, 

q17_Baseline as bl_q17, q17_marks_Baseline as bl_q17_marks, q18_1_Baseline as bl_q18_1, q18_2_Baseline as bl_q18_2, q18_3_Baseline as bl_q18_3, 
q18_4_Baseline as bl_q18_4, q18_5_Baseline as bl_q18_5, q18_6_Baseline as bl_q18_6, q18_7_Baseline as bl_q18_7, q18_8_Baseline as bl_q18_8, 
q18_9_Baseline as bl_q18_9, q18_10_Baseline as bl_q18_10, q18_11_Baseline as bl_q18_11, q18_marks_Baseline as bl_q18_marks, q19_Baseline as bl_q19, 
q19_marks_Baseline as bl_q19_marks, q20_Baseline as bl_q20, q20_marks_Baseline as bl_q20_marks, q21_Baseline as bl_q21, q21_marks_Baseline as bl_q21_marks,
q22_Baseline as bl_q22, q22_marks_Baseline as bl_q22_marks, fp_total_marks_Baseline as bl_fp_total_marks, 

f1_Baseline as bl_f1, f2_Baseline as bl_f2, f3_Baseline as bl_f3, f4_Baseline as bl_f4, f5_Baseline as bl_f5, f6_Baseline as bl_f6, f7_Baseline as bl_f7, 
f8_Baseline as bl_f8, f9_Baseline as bl_f9, f10_Baseline as bl_f10, f11_Baseline as bl_f11, f12_Baseline as bl_f12,

fp_no_Endline as el_fp_no, assessment_batch_id_Endline as el_assessment_batch_id, created_on_Endline as el_created_on, 
created_from_form_Endline as el_created_from_form, assessment_grade_Endline as el_assessment_grade, 
assessment_academic_year_Endline as el_assessment_academic_year, 

q17_Endline as el_q17, q17_marks_Endline as el_q17_marks, q18_1_Endline as el_q18_1, q18_2_Endline as el_q18_2, q18_3_Endline as el_q18_3, 
q18_4_Endline as el_q18_4, q18_5_Endline as el_q18_5, q18_6_Endline as el_q18_6, q18_7_Endline as el_q18_7, q18_8_Endline as el_q18_8, 
q18_9_Endline as el_q18_9, q18_10_Endline as el_q18_10, q18_11_Endline as el_q18_11, q18_marks_Endline as el_q18_marks, q19_Endline as el_q19, 
q19_marks_Endline as el_q19_marks, q20_Endline as el_q20, q20_marks_Endline as el_q20_marks, q21_Endline as el_q21, q21_marks_Endline as el_q21_marks,
q22_Endline as el_q22, q22_marks_Endline as el_q22_marks, fp_total_marks_Endline as el_fp_total_marks, 

f1_Endline as el_f1, f2_Endline as el_f2, f3_Endline as el_f3, f4_Endline as el_f4, f5_Endline as el_f5, f6_Endline as el_f6, f7_Endline as el_f7, 
f8_Endline as el_f8, f9_Endline as el_f9, f10_Endline as el_f10, f11_Endline as el_f11, f12_Endline as el_f12,

from t1
)
    
SELECT *
From t2 order by assessment_barcode