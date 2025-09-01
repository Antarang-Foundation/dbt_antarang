with 
t0 as (select assessment_barcode, record_type, created_on, created_from_form, cdm1_no, is_non_null, 
q1, q1_marks, q2_1, q2_2, q2_marks, q3_1, q3_2, q3_marks, q4_1, q4_1_marks, q4_2,
q4_2_marks,	 q4_marks, cdm1_total_marks, error_status, data_cleanup, marks_recalculated, student_linked
from {{ ref('dev_stg_cdm1') }} where data_cleanup = true and marks_recalculated = true and student_linked = true),

t1 as (
  SELECT 
    t0.assessment_barcode, t0.record_type, t0.created_on, t0.created_from_form, t0.cdm1_no, t0.is_non_null, t0.q1, t0.q1_marks, 
    t0.q2_1, t0.q2_2, t0.q2_marks, t0.q3_1, t0.q3_2, t0.q3_marks, t0.q4_1, t0.q4_1_marks, t0.q4_2,q4_2_marks, t0.q4_marks, 
    t0.cdm1_total_marks, t0.error_status, t0.data_cleanup, t0.marks_recalculated, t0.student_linked,
    ROW_NUMBER() OVER (
      PARTITION BY t0.assessment_barcode, t0.record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
),

t2 as (
    select t1.assessment_barcode, t1.record_type, t1.created_on, t1.created_from_form, t1.cdm1_no, t1.is_non_null, t1.q1, 
    t1.q1_marks, t1.q2_1, t1.q2_2, t1.q2_marks, t1.q3_1, t1.q3_2, t1.q3_marks, t1.q4_1, t1.q4_1_marks, t1.q4_2, t1.q4_2_marks, 
    t1.q4_marks, t1.cdm1_total_marks from t1 
    where is_latest = 1
),

t3 as (select student_barcode, gender, caste, batch_id, batch_no, batch_academic_year, batch_grade, batch_language,
    facilitator_name, facilitator_email, school_name, school_taluka, school_ward, school_district, school_state, 
    school_partner, school_area, batch_donor  
    from {{ ref("dev_int_global_dcp") }} where batch_academic_year >= 2023
    ),

t4 as (select t3.*, t2.* from t2    
    LEFT join t3 on t3.student_barcode = t2.assessment_barcode
)

select * from t4
