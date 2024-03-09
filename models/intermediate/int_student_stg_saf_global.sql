with

    t1 as (select * from {{ ref("int_student") }}),
    t2 as (select * from {{ ref("stg_saf") }}),
    t3 as (select t1.*, t2.barcode as assessment_barcode, t2.* except(barcode) from t1 left join t2 on t1.contact_barcode = t2.barcode order by t1.contact_barcode),
    t4 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, batch_trainer_name, school, school_district, school_state, 
    school_partner, donor, no_of_students_facilitated from {{ ref("int_global") }}),
    t5 as (select t3.*, t4.batch_id as global_batch_id, t4.* except (batch_id) from t3 left join t4 on t3.contact_batch_id = t4.batch_id order by t3.contact_batch_id)
    select * from t5 order by contact_id, uid, contact_grade, contact_barcode, contact_batch_id