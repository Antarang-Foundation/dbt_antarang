with
    t0 as (
        SELECT 
                assessment_barcode, record_type, created_on, created_from_form, assessment_grade,  assessment_academic_year, assessment_batch_id, 
                cdm2_no, is_non_null, q5, q5_marks, q6_1, q6_1_marks, q6_2, q6_2_marks, q6_3, q6_3_marks, q6_4, q6_4_marks, q6_5, q6_5_marks, q6_6, q6_6_marks, 
                q6_7, q6_7_marks, q6_8, q6_8_marks, q6_9, q6_9_marks, q6_10, q6_10_marks, q6_11, q6_11_marks, q6_12, q6_12_marks, q6_total_marks, 
                cdm2_total_marks

        FROM {{ ref('int_cdm2_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(created_on) as created_on, max(created_from_form) as created_from_form, max(assessment_academic_year) as assessment_academic_year,
            max(assessment_grade) as assessment_grade, max(assessment_batch_id) as assessment_batch_id, max(cdm2_no) as cdm2_no, 
            max(is_non_null) as is_non_null, 

            max(q5) as q5, max(q5_marks) as q5_marks, max(q6_1) as q6_1, max(q6_1_marks) as q6_1_marks,
            max(q6_2) as q6_2, max(q6_2_marks) as q6_2_marks, max(q6_3) as q6_3, max(q6_3_marks) as q6_3_marks, max(q6_4) as q6_4, 
            max(q6_4_marks) as q6_4_marks, max(q6_5) as q6_5, max(q6_5_marks) as q6_5_marks, max(q6_6) as q6_6, max(q6_6_marks) as q6_6_marks, 
            max(q6_7) as q6_7, max(q6_7_marks) as q6_7_marks, max(q6_8) as q6_8, max(q6_8_marks) as q6_8_marks, max(q6_9) as q6_9, 
            max(q6_9_marks) as q6_9_marks, max(q6_10) as q6_10, max(q6_10_marks) as q6_10_marks, max(q6_11) as q6_11, max(q6_11_marks) as q6_11_marks, 
            max(q6_12) as q6_12, max(q6_12_marks) as q6_12_marks, max(q6_total_marks) as q6_total_marks, max(cdm2_total_marks) as cdm2_total_marks

            FOR record_type IN ('Baseline', 'Endline')
            )
    ),

    t2 as (select 

assessment_barcode, 

created_on_Baseline as bl_created_on, created_from_form_Baseline as bl_created_from_form, assessment_grade_Baseline as bl_assessment_grade, 
assessment_academic_year_Baseline as bl_assessment_academic_year, assessment_batch_id_Baseline as bl_assessment_batch_id, 
cdm2_no_Baseline as bl_cdm2_no, is_non_null_Baseline as bl_is_non_null,

q5_Baseline as bl_q5, q5_marks_Baseline as bl_q5_marks, 
q6_1_Baseline as bl_q6_1, q6_1_marks_Baseline as bl_q6_1_marks, q6_2_Baseline as bl_q6_2, q6_2_marks_Baseline as bl_q6_2_marks, 
q6_3_Baseline as bl_q6_3, q6_3_marks_Baseline as bl_q6_3_marks, q6_4_Baseline as bl_q6_4, q6_4_marks_Baseline as bl_q6_4_marks, 
q6_5_Baseline as bl_q6_5, q6_5_marks_Baseline as bl_q6_5_marks, q6_6_Baseline as bl_q6_6, q6_6_marks_Baseline as bl_q6_6_marks, 
q6_7_Baseline as bl_q6_7, q6_7_marks_Baseline as bl_q6_7_marks, q6_8_Baseline as bl_q6_8, q6_8_marks_Baseline as bl_q6_8_marks, 
q6_9_Baseline as bl_q6_9, q6_9_marks_Baseline as bl_q6_9_marks, q6_10_Baseline as bl_q6_10, q6_10_marks_Baseline as bl_q6_10_marks, 
q6_11_Baseline as bl_q6_11, q6_11_marks_Baseline as bl_q6_11_marks, q6_12_Baseline as bl_q6_12, q6_12_marks_Baseline as bl_q6_12_marks, 
q6_total_marks_Baseline as bl_q6_total_marks, cdm2_total_marks_Baseline as bl_cdm2_total_marks,

created_on_Endline as el_created_on, created_from_form_Endline as el_created_from_form, assessment_grade_Endline as el_assessment_grade, 
assessment_academic_year_Endline as el_assessment_academic_year, assessment_batch_id_Endline as el_assessment_batch_id, 
cdm2_no_Endline as el_cdm2_no, is_non_null_Endline as el_is_non_null,

q5_Endline as el_q5, q5_marks_Endline as el_q5_marks, q6_1_Endline as el_q6_1, 
q6_1_marks_Endline as el_q6_1_marks, q6_2_Endline as el_q6_2, q6_2_marks_Endline as el_q6_2_marks, q6_3_Endline as el_q6_3, 
q6_3_marks_Endline as el_q6_3_marks, q6_4_Endline as el_q6_4, q6_4_marks_Endline as el_q6_4_marks, q6_5_Endline as el_q6_5, 
q6_5_marks_Endline as el_q6_5_marks, q6_6_Endline as el_q6_6, q6_6_marks_Endline as el_q6_6_marks, q6_7_Endline as el_q6_7,
q6_7_marks_Endline as el_q6_7_marks, q6_8_Endline as el_q6_8, q6_8_marks_Endline as el_q6_8_marks, q6_9_Endline as el_q6_9, 
q6_9_marks_Endline as el_q6_9_marks, q6_10_Endline as el_q6_10, q6_10_marks_Endline as el_q6_10_marks, q6_11_Endline as el_q6_11, 
q6_11_marks_Endline as el_q6_11_marks, q6_12_Endline as el_q6_12, q6_12_marks_Endline as el_q6_12_marks, 
 q6_total_marks_Endline as el_q6_total_marks, cdm2_total_marks_Endline as el_cdm2_total_marks

from t1
)
    
SELECT *
From t2 order by assessment_barcode
