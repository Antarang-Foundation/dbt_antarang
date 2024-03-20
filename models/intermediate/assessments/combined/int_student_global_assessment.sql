with

    t1 as (select student_id, student_barcode, student_grade, gender, student_batch_id, batch_id, batch_no, batch_academic_year, batch_grade, batch_language,
    batch_facilitator_id, batch_school_id, facilitator_id, facilitator_name,  school_id, school_name, school_academic_year, school_language, school_taluka, 
    school_ward, school_district, school_state, school_partner, batch_donor_id, donor_id, batch_donor  
    from {{ ref("int_student_global") }}),
    t2 as (select * from {{ ref("int_assessments_combined_trimmed") }}),
    t3 as (select * from t1 full outer join t2 on t1.student_barcode = t2.assessment_barcode 
    order by t1.student_barcode)

select * from t3


