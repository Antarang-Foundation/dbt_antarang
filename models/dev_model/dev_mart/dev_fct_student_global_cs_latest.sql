with 
t0 as (select assessment_barcode, record_type, created_on, created_from_form, cs_no, is_non_null, q11_1, q11_2, q11_3, q11_4, q11_5, 
q11_6, q11_7, q11_8, q11_9, q11_marks, q12_1, q12_2, q12_3, q12_4, q12_marks, q13, q13_marks, q14, q14_marks, q15_1, q15_2, q15_3, 
q15_4, q15_5, q15_6, q15_7, q15_8, q15_9, q15_marks, q16,q16_marks, cs_total_marks, data_cleanup, marks_recalculated, student_linked
from {{ ref('dev_stg_cs') }} where data_cleanup = true and marks_recalculated = true and student_linked = true
),

t1 as 
(select t0.*,
    ROW_NUMBER() OVER (
      PARTITION BY t0.assessment_barcode, t0.record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
),

t2 as (select t1.assessment_barcode, t1.record_type, t1.created_on, t1.created_from_form, t1.cs_no, t1.is_non_null, t1.q11_1, 
t1.q11_2, t1.q11_3, t1.q11_4, t1.q11_5, t1.q11_6, t1.q11_7, t1.q11_8, t1.q11_9, t1.q11_marks, t1.q12_1, t1.q12_2, t1.q12_3, 
t1.q12_4, t1.q12_marks, t1.q13, t1.q13_marks, t1.q14, t1.q14_marks, t1.q15_1, t1.q15_2, t1.q15_3, t1.q15_4, t1.q15_5, 
t1.q15_6, t1.q15_7, t1.q15_8, t1.q15_9, t1.q15_marks, t1.q16,q16_marks, t1.cs_total_marks from t1 where is_latest = 1
),

t3 as (select student_barcode, gender, caste, batch_id, batch_no, batch_academic_year, batch_grade, batch_language, 
facilitator_name, facilitator_email, school_name, school_taluka, school_ward, school_district, school_state, school_partner, 
school_area, batch_donor from {{ ref("dev_int_global_dcp") }} where batch_academic_year >= 2023),

t4 as (select t3.*, t2.* from t2    
    INNER join t3 on t3.student_barcode = t2.assessment_barcode
)

select * from t4