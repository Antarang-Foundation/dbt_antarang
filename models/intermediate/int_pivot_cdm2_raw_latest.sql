with
    t0 as (
        SELECT 
                barcode, cdm2_no, q5, q5_marks, q6_1, q6_1_marks, q6_2, q6_2_marks, q6_3, q6_3_marks, q6_4, q6_4_marks, q6_5, q6_5_marks, q6_6, q6_6_marks, 
                q6_7, q6_7_marks, q6_8, q6_8_marks, q6_9, q6_9_marks, q6_10, q6_10_marks, q6_11, q6_11_marks, q6_12, q6_12_marks, q6_total_marks, 
                cdm2_total_marks, assessment_batch_id, record_type, created_on, assessment_grade, assessment_academic_year, created_from_form

        FROM {{ ref('int_cdm2_raw_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(cdm2_no) as cdm2_no, max(assessment_batch_id) as assessment_batch_id, max(created_on) as created_on, max(created_from_form) as created_from_form,
            max(assessment_grade) as assessment_grade, max(assessment_academic_year) as assessment_academic_year,
            max(q5) as q5, max(q5_marks) as q5_marks, max(q6_1) as q6_1, max(q6_1_marks) as q6_1_marks,
            max(q6_2) as q6_2, max(q6_2_marks) as q6_2_marks, max(q6_3) as q6_3, max(q6_3_marks) as q6_3_marks, max(q6_4) as q6_4, 
            max(q6_4_marks) as q6_4_marks, max(q6_5) as q6_5, max(q6_5_marks) as q6_5_marks, max(q6_6) as q6_6, max(q6_6_marks) as q6_6_marks, 
            max(q6_7) as q6_7, max(q6_7_marks) as q6_7_marks, max(q6_8) as q6_8, max(q6_8_marks) as q6_8_marks, max(q6_9) as q6_9, 
            max(q6_9_marks) as q6_9_marks, max(q6_10) as q6_10, max(q6_10_marks) as q6_10_marks, max(q6_11) as q6_11, max(q6_11_marks) as q6_11_marks, 
            max(q6_12) as q6_12, max(q6_12_marks) as q6_12_marks, max(q6_total_marks) as q6_total_marks, max(cdm2_total_marks) as cdm2_total_marks

            FOR record_type IN ('Baseline', 'Endline')
            )
    )
    
     
SELECT *
From t1

