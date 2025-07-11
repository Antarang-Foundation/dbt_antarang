with
t0 as (select assessment_barcode, record_type, created_on, created_from_form, fp_no, is_non_null, q17, q17_marks, q18_1, q18_2,
q18_3, q18_4, q18_5, q18_6, q18_7, q18_8, q18_9, q18_10, q18_11, q18_marks, q19, q19_marks, q20, q20_marks, q21, q21_marks, q22,
q22_marks, fp_total_marks, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, q21_null, q21_bucket, data_cleanup, marks_recalculated,
student_linked from {{ ref('dev_stg_fp') }} where data_cleanup = true and marks_recalculated = true and student_linked = true
),

t1 as 
(select t0.*,
    ROW_NUMBER() OVER (
      PARTITION BY t0.assessment_barcode, t0.record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
),

t2 as (select t1.assessment_barcode, t1.record_type, t1.created_on, t1.created_from_form, t1.fp_no, t1.is_non_null, t1.q17, 
t1.q17_marks, t1.q18_1, t1.q18_2, t1.q18_3, t1.q18_4, t1.q18_5, t1.q18_6, t1.q18_7, t1.q18_8, t1.q18_9, t1.q18_10, t1.q18_11, 
t1.q18_marks, t1.q19, t1.q19_marks, t1.q20, t1.q20_marks, t1.q21, t1.q21_marks, t1.q22, t1.q22_marks, t1.fp_total_marks, t1.f1, 
t1.f2, t1.f3, t1.f4, t1.f5, t1.f6, t1.f7, t1.f8, t1.f9, t1.f10, t1.f11, t1.f12, t1.q21_null, t1.q21_bucket from t1 where is_latest = 1
),

t3 as (select student_barcode, gender, caste, batch_id, batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name,
facilitator_email, school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, batch_donor
from {{ ref("dev_int_global_dcp") }} where batch_academic_year >= 2023),

t4 as (select t3.*, t2.* from t2    
    INNER join t3 on t3.student_barcode = t2.assessment_barcode
)

select * from t4
