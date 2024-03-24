with
    t0 as (
        SELECT 
                assessment_barcode, record_type, created_on, created_from_form, assessment_grade,  assessment_academic_year, assessment_batch_id,
                cp_no, is_non_null, q7, q7_marks, q8, q8_marks, q9_1, q9_1_marks, q9_2, q9_2_marks, q9_3, q9_3_marks, q9_4, q9_4_marks, q9_5, q9_5_marks, 
                q9_6, q9_6_marks, q9_7, q9_7_marks, q10, q10_marks, cp_total_marks

        FROM {{ ref('int_cp_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(created_on) as created_on, max(created_from_form) as created_from_form, max(assessment_academic_year) as assessment_academic_year,
            max(assessment_grade) as assessment_grade, max(assessment_batch_id) as assessment_batch_id, max(cp_no) as cp_no, 
            max(is_non_null) as is_non_null, 

            max(q7) as q7, max(q7_marks) as q7_marks, max(q8) as q8, max(q8_marks) as q8_marks,
            max(q9_1) as q9_1, max(q9_1_marks) as q9_1_marks, max(q9_2) as q9_2, max(q9_2_marks) as q9_2_marks, max(q9_3) as q9_3, 
            max(q9_3_marks) as q9_3_marks, max(q9_4) as q9_4, max(q9_4_marks) as q9_4_marks, max(q9_5) as q9_5, max(q9_5_marks) as q9_5_marks, 
            max(q9_6) as q9_6, max(q9_6_marks) as q9_6_marks, max(q9_7) as q9_7, max(q9_7_marks) as q9_7_marks, max(q10) as q10, 
            max(q10_marks) as q10_marks, max(cp_total_marks) as cp_total_marks

            FOR record_type IN ('Baseline', 'Endline')
            )
    ),

    t2 as (select 

assessment_barcode, 

created_on_Baseline as bl_created_on, created_from_form_Baseline as bl_created_from_form, assessment_grade_Baseline as bl_assessment_grade, 
assessment_academic_year_Baseline as bl_assessment_academic_year, assessment_batch_id_Baseline as bl_assessment_batch_id, 
cp_no_Baseline as bl_cp_no, is_non_null_Baseline as bl_is_non_null,

q7_Baseline as bl_q7, q7_marks_Baseline as bl_q7_marks, q8_Baseline as bl_q8, 
q8_marks_Baseline as bl_q8_marks, q9_1_Baseline as bl_q9_1, q9_1_marks_Baseline as bl_q9_1_marks, q9_2_Baseline as bl_q9_2, 
q9_2_marks_Baseline as bl_q9_2_marks, q9_3_Baseline as bl_q9_3, q9_3_marks_Baseline as bl_q9_3_marks, q9_4_Baseline as bl_q9_4, 
q9_4_marks_Baseline as bl_q9_4_marks, q9_5_Baseline as bl_q9_5, q9_5_marks_Baseline as bl_q9_5_marks, q9_6_Baseline as bl_q9_6, 
q9_6_marks_Baseline as bl_q9_6_marks, q9_7_Baseline as bl_q9_7, q9_7_marks_Baseline as bl_q9_7_marks,  q10_Baseline as bl_q10, 
q10_marks_Baseline as bl_q10_marks, cp_total_marks_Baseline as bl_cp_total_marks,

created_on_Endline as el_created_on, created_from_form_Endline as el_created_from_form, assessment_grade_Endline as el_assessment_grade, 
assessment_academic_year_Endline as el_assessment_academic_year, assessment_batch_id_Endline as el_assessment_batch_id, 
cp_no_Endline as el_cp_no, is_non_null_Endline as el_is_non_null,

q7_Endline as el_q7, q7_marks_Endline as el_q7_marks, q8_Endline as el_q8, 
q8_marks_Endline as el_q8_marks, q9_1_Endline as el_q9_1, q9_1_marks_Endline as el_q9_1_marks, q9_2_Endline as el_q9_2, 
q9_2_marks_Endline as el_q9_2_marks, q9_3_Endline as el_q9_3, q9_3_marks_Endline as el_q9_3_marks, q9_4_Endline as el_q9_4, 
q9_4_marks_Endline as el_q9_4_marks, q9_5_Endline as el_q9_5, q9_5_marks_Endline as el_q9_5_marks, q9_6_Endline as el_q9_6, 
q9_6_marks_Endline as el_q9_6_marks, q9_7_Endline as el_q9_7, q9_7_marks_Endline as el_q9_7_marks,  q10_Endline as el_q10, 
q10_marks_Endline as el_q10_marks, cp_total_marks_Endline as el_cp_total_marks

from t1
)
    
SELECT *
From t2 order by assessment_barcode






