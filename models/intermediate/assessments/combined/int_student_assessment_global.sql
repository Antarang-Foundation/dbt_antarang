with

    t0 as (select * from {{ ref("int_student") }}),

    t1 as (select * from {{ ref("int_assessments_combined_trimmed") }}),

    t2 as (select t0.*, t1.barcode as assessment_barcode, t1.* except(barcode) from t0 left join t1 on t0.contact_barcode = t1.barcode),

    t3 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_taluka, school_ward, 
    school_district, school_state, school_partner, batch_donor, no_of_students_facilitated from {{ ref("int_global") }}),
    
    t4 as (select t2.*, t3.batch_id as global_batch_id, t3.* except (batch_id) from t2 left join t3 on t2.contact_batch_id = t3.batch_id)
    select * from t4 order by student_id, contact_grade


