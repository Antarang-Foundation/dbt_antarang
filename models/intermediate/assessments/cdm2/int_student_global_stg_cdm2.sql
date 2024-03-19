with

    t1 as (select student_id, student_barcode, student_grade, student_batch_id, gender, batch_no, batch_academic_year, school_academic_year, batch_grade, 
    batch_language, school_language, facilitator_name, school_name, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor 
    from {{ ref("int_student_global") }}),
    t2 as (select * from {{ ref("stg_cdm2") }}),
    t3 as (select * from t1 full outer join t2 on t1.student_barcode = t2.assessment_barcode 
    order by t1.student_barcode)

select * from t3