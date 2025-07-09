with 
t0 as (select assessment_barcode, record_type, created_on, created_from_form, cdm2_no, is_non_null, q5, q5_marks, q6_1, 
q6_1_marks, q6_2, q6_2_marks, q6_3, q6_3_marks, q6_4, q6_4_marks, q6_5, q6_5_marks, q6_6, q6_6_marks, q6_7, q6_7_marks, q6_8, 
q6_8_marks, q6_9, q6_9_marks, q6_10, q6_10_marks, q6_11, q6_11_marks, q6_12, q6_12_marks, q6_total_marks, cdm2_total_marks,
data_cleanup, marks_recalculated, student_linked
from {{ ref('dev_stg_cdm2') }} where data_cleanup = true and marks_recalculated = true and student_linked = true),

t1 as 
(select t0.*,
    ROW_NUMBER() OVER (
      PARTITION BY t0.assessment_barcode, t0.record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
),

t2 as
(select t1.assessment_barcode, t1.record_type, t1.created_on, t1.created_from_form, t1.cdm2_no, t1.is_non_null, t1.q5, 
t1.q5_marks, t1.q6_1, t1.q6_1_marks, t1.q6_2, t1.q6_2_marks, t1.q6_3, t1.q6_3_marks, t1.q6_4, t1.q6_4_marks, t1.q6_5, 
t1.q6_5_marks, t1.q6_6, t1.q6_6_marks, t1.q6_7, t1.q6_7_marks, t1.q6_8, t1.q6_8_marks, t1.q6_9, t1.q6_9_marks, t1.q6_10, 
t1.q6_10_marks, t1.q6_11, t1.q6_11_marks, t1.q6_12, t1.q6_12_marks, t1.q6_total_marks, t1.cdm2_total_marks from t1 
where is_latest = 1
),

t3 as
(select student_barcode, gender, caste, batch_id, batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name,
facilitator_email, school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area,
batch_donor from {{ ref("dev_int_global_dcp") }} where batch_academic_year >= 2023
),

t4 as (select t3.*, t2.* from t2    
    INNER join t3 on t3.student_barcode = t2.assessment_barcode
)

select * from t4

