with 
sar_latest as (
  select 
    sar_id, assessment_barcode, record_type, created_on, created_from_form, sar_no, is_non_null, sar_atleast_one_quiz,
    sar_atleast_one_reality, q2_submitted, reality_submitted, sar_q1, sar_q1_marks, sar_q2, sar_q2_marks, sar_q3, sar_q3_marks, 
    sar_q4, sar_q4_marks, sar_q5, sar_q5_marks, r1s1, r1s1_marks, r2s2, r2s2_marks, r3s3, r3s3_marks, r4s4, r4s4_marks, r5f1, 
    r5f1_marks, r6f2, r6f2_marks, r7f3, r7f3_marks, r8f4, r8f4_marks 
  from {{ ref('dev_int_sar_latest') }}
),

global_dcp as (
  select 
    student_barcode, gender, caste, batch_id, batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, 
    facilitator_email, school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, 
    batch_donor 
  from {{ ref('dev_int_global_dcp') }} where batch_academic_year >= 2023
),

global_sar as (
  select 
    s.*, 
    d.* 
  from sar_latest s 
  left join global_dcp d 
    on s.assessment_barcode = d.student_barcode
)

select * from global_sar

