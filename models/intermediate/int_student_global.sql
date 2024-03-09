with

    t0 as (select * from {{ ref("int_student") }}),
    
    t1 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, batch_trainer_name, school, school_district, school_state, 
    school_partner, donor, no_of_students_facilitated from {{ ref("int_global") }}),

    t2 as (select t0.*, t1.batch_id as global_batch_id, t1.* except(batch_id) from t0 left join t1 on t0.contact_batch_id = t1.batch_id)

    select * from t2 order by contact_id, uid, contact_grade, contact_barcode, contact_batch_id

