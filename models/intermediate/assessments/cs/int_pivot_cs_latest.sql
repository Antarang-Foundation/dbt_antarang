with
    t0 as (
        SELECT 
                barcode, cs_no, q11_1, q11_2, q11_3, q11_4, q11_5, q11_6, q11_7, q11_8, q11_9, q11_marks, 
                q12_1, q12_2, q12_3, q12_4, q12_marks, q13, q13_marks, q14, q14_marks, 
                q15_1, q15_2, q15_3, q15_4, q15_5, q15_6, q15_7, q15_8, q15_9, q15_marks, q16, q16_marks, cs_total_marks, assessment_batch_id, record_type, 
                created_on, assessment_grade, assessment_academic_year, created_from_form

        FROM {{ ref('int_cs_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(cs_no) as cs_no, max(assessment_batch_id) as assessment_batch_id, max(created_on) as created_on, max(created_from_form) as created_from_form,
            max(assessment_grade) as assessment_grade, max(assessment_academic_year) as assessment_academic_year,
            max(q11_1) as q11_1, max(q11_2) as q11_2, max(q11_3) as q11_3, max(q11_4) as q11_4, 
            max(q11_5) as q11_5, max(q11_6) as q11_6, max(q11_7) as q11_7, max(q11_8) as q11_8, max(q11_9) as q11_9, max(q11_marks) as q11_marks, 

            max(q12_1) as q12_1, max(q12_2) as q12_2, max(q12_3) as q12_3, max(q12_4) as q12_4, max(q12_marks) as q12_marks,

            max(q13) as q13, max(q13_marks) as q13_marks, max(q14) as q14, max(q14_marks) as q14_marks, 

            max(q15_1) as q15_1, max(q15_2) as q15_2, max(q15_3) as q15_3, max(q15_4) as q15_4, max(q15_5) as q15_5, max(q15_6) as q15_6, 
            max(q15_7) as q15_7, max(q15_8) as q15_8, max(q15_9) as q15_9, max(q15_marks) as q15_marks,
            
            max(q16) as q16, max(q16_marks) as q16_marks, max(cs_total_marks) as cs_total_marks

            FOR record_type IN ('Baseline', 'Endline')
            )
    ),

    t2 as (select 

barcode, 

cs_no_Baseline as bl_cs_no, assessment_batch_id_Baseline as bl_assessment_batch_id, created_on_Baseline as bl_created_on, 
created_from_form_Baseline as bl_created_from_form, assessment_grade_Baseline as bl_assessment_grade, 
assessment_academic_year_Baseline as bl_assessment_academic_year, q11_1_Baseline as bl_q11_1, q11_2_Baseline as bl_q11_2, q11_3_Baseline as bl_q11_3, 
q11_4_Baseline as bl_q11_4, q11_5_Baseline as bl_q11_5, q11_6_Baseline as bl_q11_6, q11_7_Baseline as bl_q11_7, q11_8_Baseline as bl_q11_8, 
q11_9_Baseline as bl_q11_9, q11_marks_Baseline as bl_q11_marks, q12_1_Baseline as bl_q12_1, q12_2_Baseline as bl_q12_2, q12_3_Baseline as bl_q12_3, 
q11_4_Baseline as bl_q12_4, q12_marks_Baseline as bl_q12_marks, q13_Baseline as bl_q13, q13_marks_Baseline as bl_q13_marks, q14_Baseline as bl_q14,
q14_marks_Baseline as bl_q14_marks, q15_1_Baseline as bl_q15_1, q15_2_Baseline as bl_q15_2, q15_3_Baseline as bl_q15_3, q15_4_Baseline as bl_q15_4, 
q15_5_Baseline as bl_q15_5, q15_6_Baseline as bl_q15_6, q15_7_Baseline as bl_q15_7, q15_8_Baseline as bl_q15_8, q15_9_Baseline as bl_q15_9, 
q15_marks_Baseline as bl_q15_marks, q16_Baseline as bl_q16, q16_marks_Baseline as bl_q16_marks, cs_total_marks_Baseline as bl_cs_total_marks,

cs_no_Endline as el_cs_no, assessment_batch_id_Endline as el_assessment_batch_id, created_on_Endline as el_created_on, 
created_from_form_Endline as el_created_from_form, assessment_grade_Endline as el_assessment_grade, 
assessment_academic_year_Endline as el_assessment_academic_year, q11_1_Endline as el_q11_1, q11_2_Endline as el_q11_2, q11_3_Endline as el_q11_3, 
q11_4_Endline as el_q11_4, q11_5_Endline as el_q11_5, q11_6_Endline as el_q11_6, q11_7_Endline as el_q11_7, q11_8_Endline as el_q11_8, 
q11_9_Endline as el_q11_9, q11_marks_Endline as el_q11_marks, q12_1_Endline as el_q12_1, q12_2_Endline as el_q12_2, q12_3_Endline as el_q12_3, 
q11_4_Endline as el_q12_4, q12_marks_Endline as el_q12_marks, q13_Endline as el_q13, q13_marks_Endline as el_q13_marks, q14_Endline as el_q14,
q14_marks_Endline as el_q14_marks, q15_1_Endline as el_q15_1, q15_2_Endline as el_q15_2, q15_3_Endline as el_q15_3, q15_4_Endline as el_q15_4, 
q15_5_Endline as el_q15_5, q15_6_Endline as el_q15_6, q15_7_Endline as el_q15_7, q15_8_Endline as el_q15_8, q15_9_Endline as el_q15_9, 
q15_marks_Endline as el_q15_marks, q16_Endline as el_q16, q16_marks_Endline as el_q16_marks, cs_total_marks_Endline as el_cs_total_marks

from t1
)
    
SELECT *
From t2 order by barcode
