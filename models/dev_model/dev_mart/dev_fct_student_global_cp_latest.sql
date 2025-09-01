with 
t0 as (select assessment_barcode, record_type, created_on, created_from_form, cp_no, is_non_null, q7, q7_marks, q8, q8_marks,
q9_1, q9_1_marks, q9_2, q9_2_marks, q9_3, q9_3_marks, q9_4, q9_4_marks, q9_5, q9_5_marks, q9_6, q9_6_marks, q9_7, q9_7_marks, q10, 
q10_marks, cp_total_marks, q8_null, q8_bucket, data_cleanup, marks_recalculated, student_linked from {{ ref('dev_stg_cp') }} 
where data_cleanup = true and marks_recalculated = true and student_linked = true
),

t1 as 
(select t0.*,
    ROW_NUMBER() OVER (
      PARTITION BY t0.assessment_barcode, t0.record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
),

t2 as (select t1.assessment_barcode, t1.record_type, t1.created_on, t1.created_from_form, t1.cp_no, t1.is_non_null, t1.q7, 
t1.q7_marks, t1.q8, t1.q8_marks, t1.q9_1, t1.q9_1_marks, t1.q9_2, t1.q9_2_marks, t1.q9_3, t1.q9_3_marks, t1.q9_4, t1.q9_4_marks, 
t1.q9_5, t1.q9_5_marks, t1.q9_6, t1.q9_6_marks, t1.q9_7, t1.q9_7_marks, t1.q10, t1.q10_marks, t1.cp_total_marks, t1.q8_null, 
t1.q8_bucket from t1 where is_latest = 1
),

t3 as (select student_barcode, gender, caste, batch_id, batch_no, batch_academic_year, batch_grade, batch_language,facilitator_name, 
facilitator_email, school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, batch_donor
from {{ ref("dev_int_global_dcp") }} where batch_academic_year >= 2023
),

t4 as (select t3.*, t2.* from t2    
    LEFT join t3 on t3.student_barcode = t2.assessment_barcode
)

select * from t4
