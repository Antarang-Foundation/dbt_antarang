with
    t0 as (
        SELECT 
                assessment_barcode, record_type, created_on, created_from_form, assessment_grade,  assessment_academic_year, assessment_batch_id, 
                cdm1_no, is_non_null, q1, q1_marks, q2_1, q2_2, q2_marks, q3_1, q3_2, q3_marks, q4_1, q4_1_marks, q4_2, q4_2_marks, q4_marks, 
                cdm1_total_marks, 
        FROM {{ ref('int_cdm1_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(created_on) as created_on, max(created_from_form) as created_from_form, max(assessment_academic_year) as assessment_academic_year,
            max(assessment_grade) as assessment_grade, max(assessment_batch_id) as assessment_batch_id, max(cdm1_no) as cdm1_no, 
            max(is_non_null) as is_non_null, 
            
            max(q1) as q1, max(q1_marks) as q1_marks, max(q2_1) as q2_1, max(q2_2) as q2_2, 
            max(q2_marks) as q2_marks, max(q3_1) as q3_1, max(q3_2) as q3_2, max(q3_marks) as q3_marks, max(q4_1) as q4_1, max(q4_1_marks) as q4_1_marks, 
            max(q4_2) as q4_2, max(q4_2_marks) as q4_2_marks, max(q4_marks) as q4_marks, max(cdm1_total_marks) as cdm1_total_marks

            FOR record_type IN ('Baseline', 'Endline')
            )
    ),

t2 as (select 

assessment_barcode, 

created_on_Baseline as bl_created_on, created_from_form_Baseline as bl_created_from_form, assessment_grade_Baseline as bl_assessment_grade, 
assessment_academic_year_Baseline as bl_assessment_academic_year, assessment_batch_id_Baseline as bl_assessment_batch_id, 
cdm1_no_Baseline as bl_cdm1_no, is_non_null_Baseline as bl_is_non_null,

q1_Baseline as bl_q1, q1_marks_Baseline as bl_q1_marks, 
q2_1_Baseline as bl_q2_1, q2_2_Baseline as bl_q2_2, q2_marks_Baseline as bl_q2_marks, 
q3_1_Baseline as bl_q3_1, q3_2_Baseline as bl_q3_2, q3_marks_Baseline as bl_q3_marks, 
q4_1_Baseline as bl_q4_1, q4_1_marks_Baseline as bl_q4_1_marks, q4_2_Baseline as bl_q4_2, q4_2_marks_Baseline as bl_q4_2_marks, 
q4_marks_Baseline as bl_q4_marks, cdm1_total_marks_Baseline as bl_cdm1_total_marks,

created_on_Endline as el_created_on, created_from_form_Endline as el_created_from_form, assessment_grade_Endline as el_assessment_grade, 
assessment_academic_year_Endline as el_assessment_academic_year, assessment_batch_id_Endline as el_assessment_batch_id, 
cdm1_no_Endline as el_cdm1_no, is_non_null_Endline as el_is_non_null,

q1_Endline as el_q1, q1_marks_Endline as el_q1_marks, 
q2_1_Endline as el_q2_1, q2_2_Endline as el_q2_2, q2_marks_Endline as el_q2_marks, 
q3_1_Endline as el_q3_1, q3_2_Endline as el_q3_2, q3_marks_Endline as el_q3_marks, 
q4_1_Endline as el_q4_1, q4_1_marks_Endline as el_q4_1_marks, q4_2_Endline as el_q4_2, q4_2_marks_Endline as el_q4_2_marks, 
q4_marks_Endline as el_q4_marks, cdm1_total_marks_Endline as el_cdm1_total_marks

from t1
)
    
SELECT *
From t2 order by assessment_barcode
