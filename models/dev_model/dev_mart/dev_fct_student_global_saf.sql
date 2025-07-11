with t0 as (select saf_id, assessment_barcode, record_type, created_on, created_from_form, saf_no, is_non_null, saf_atleast_one_interest,
saf_atleast_one_aptitude, saf_atleast_one_quiz, saf_atleast_one_feedback, interest_submitted, aptitude_submitted, feedback_submitted,
saf_i1, saf_i2, saf_i3, saf_a1, saf_a2, saf_a3, saf_q1, saf_q1_marks, saf_q2, saf_q2_marks, saf_q3, saf_q3_marks, saf_q4, saf_q4_marks,
saf_q5, saf_q5_marks, saf_f1, saf_f2, saf_f3, saf_f4, saf_f5, saf_f6, saf_f7, saf_f8, saf_f9, saf_f10, saf_f11, saf_f12, data_cleanup, 
marks_recalculated, student_linked from {{ ref('dev_stg_saf') }} where data_cleanup = true and marks_recalculated = true and student_linked = true
),

t1 as 
(select t0.*,
    ROW_NUMBER() OVER (
      PARTITION BY t0.assessment_barcode, t0.record_type 
      ORDER BY created_on DESC
    ) AS is_latest
  FROM t0
),

t2 as (select t1.saf_id, t1.assessment_barcode, t1.record_type, t1.created_on, t1.created_from_form, t1.saf_no, t1.is_non_null, 
t1.saf_atleast_one_interest, t1.saf_atleast_one_aptitude, t1.saf_atleast_one_quiz, t1.saf_atleast_one_feedback, t1.interest_submitted, 
t1.aptitude_submitted, t1.feedback_submitted, t1.saf_i1, t1.saf_i2, t1.saf_i3, t1.saf_a1, t1.saf_a2, t1.saf_a3, t1.saf_q1, 
t1.saf_q1_marks, t1.saf_q2, t1.saf_q2_marks, t1.saf_q3, t1.saf_q3_marks, t1.saf_q4, t1.saf_q4_marks, t1.saf_q5, t1.saf_q5_marks, 
t1.saf_f1, t1.saf_f2, t1.saf_f3, t1.saf_f4, t1.saf_f5, t1.saf_f6, t1.saf_f7, t1.saf_f8, t1.saf_f9, t1.saf_f10, t1.saf_f11, 
t1.saf_f12 from t1 where is_latest = 1
),

t3 as (select student_barcode, gender, caste, batch_id, batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name,
facilitator_email, school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, batch_donor
from {{ ref("dev_int_global_dcp") }} where batch_academic_year >= 2023
),

t4 as (select t3.*, t2.* from t2    
    INNER join t3 on t3.student_barcode = t2.assessment_barcode
)

select * from t4