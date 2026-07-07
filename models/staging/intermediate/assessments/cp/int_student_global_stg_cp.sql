with

    t1 as (select student_id, student_barcode, student_grade, student_batch_id, gender, batch_no, batch_academic_year, school_academic_year, batch_grade, 
    batch_language, fac_start_date, school_language, facilitator_name, facilitator_email,school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, batch_donor 
    from {{ ref("int_student_global") }}),
    t2 as (select cp_id, assessment_barcode, record_type, created_on, created_from_form, cp_no, is_non_null, assessment_grade, 
assessment_academic_year, assessment_batch_id, q7, q7_marks, q8, q8_marks, q9_1, q9_1_marks, q9_2, q9_2_marks, q9_3, q9_3_marks, 
q9_4, q9_4_marks, q9_5, q9_5_marks, q9_6, q9_6_marks, q9_7, q9_7_marks, q10, q10_marks, cp_total_marks, error_status, data_cleanup, 
marks_recalculated, student_linked, q8_null, q8_bucket from {{ ref("stg_cp") }}),
    t3 as (select * from t1 full outer join t2 on t1.student_barcode = t2.assessment_barcode 
    order by t1.student_barcode)

select * from t3