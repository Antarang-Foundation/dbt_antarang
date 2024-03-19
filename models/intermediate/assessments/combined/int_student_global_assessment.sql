with

    t1 as (select student_id, contact_barcode, contact_grade, contact_batch_id, gender, batch_no, batch_academic_year, school_academic_year, batch_grade, 
    batch_language, school_language, facilitator_name, school_name, school_taluka, school_ward, school_district, school_state, school_partner, batch_donor 
    from {{ ref("int_student_global") }}),
    t2 as (select * from {{ ref("int_assessments_combined_trimmed") }}),
    t3 as (select t1.*, t2.barcode as assessment_barcode, t2.* except(barcode) from t1 full outer join t2 on t1.contact_barcode = t2.barcode 
    order by t1.contact_barcode)

select * from t3


