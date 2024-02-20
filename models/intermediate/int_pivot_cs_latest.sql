with
    t0 as (
        SELECT 
                barcode, cs_no, q11_1, q11_2, q11_3, q11_4, q11_5, q11_6, q11_7, q11_8, q11_9, q11_marks, 
                q12_1, q12_2, q12_3, q12_4, q12_marks, q13, q13_marks, q14, q14_marks, 
                q15_1, q15_2, q15_3, q15_4, q15_5, q15_6, q15_7, q15_8, q15_9, q15_marks, q16, q16_marks, cs_total_marks, batch_id, record_type, 
                created_on, grade, academic_year, created_from_form

        FROM {{ ref('int_cs_latest') }}
    ),
    t1 as (
        select *
        from t0
            PIVOT (
            max(cs_no) as cs_no, max(batch_id) as batch_id, max(created_on) as created_on, max(created_from_form) as created_from_form,
            max(grade) as grade, max(academic_year) as academic_year,
            max(q11_1) as q11_1, max(q11_2) as q11_2, max(q11_3) as q11_3, max(q11_4) as q11_4, 
            max(q11_5) as q11_5, max(q11_6) as q11_6, max(q11_7) as q11_7, max(q11_8) as q11_8, max(q11_9) as q11_9, max(q11_marks) as q11_marks, 

            max(q12_1) as q12_1, max(q12_2) as q12_2, max(q12_3) as q12_3, max(q12_4) as q12_4, max(q12_marks) as q12_marks,

            max(q13) as q13, max(q13_marks) as q13_marks, max(q14) as q14, max(q14_marks) as q14_marks, 

            max(q15_1) as q15_1, max(q15_2) as q15_2, max(q15_3) as q15_3, max(q15_4) as q15_4, max(q15_5) as q15_5, max(q15_6) as q15_6, 
            max(q15_7) as q15_7, max(q15_8) as q15_8, max(q15_9) as q15_9, max(q15_marks) as q15_marks,
            
            max(q16) as q16, max(q16_marks) as q16_marks, max(cs_total_marks) as cs_total_marks

            FOR record_type IN ('Baseline', 'Endline')
            )
    )
    
     
SELECT *
From t1
