with

    t0 as (select uid, full_name, gender, current_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, current_batch_no, g9_contact_batch_id, 
    g10_contact_batch_id, g11_contact_batch_id, g12_contact_batch_id, current_grade from {{ ref("int_students") }}),
    t1 as ((select g9_barcode as contact_barcode, g9_contact_batch_id as contact_batch_id, * from t0 where g9_barcode is not null or g9_contact_batch_id is not null) 
    union all (select g10_barcode as contact_barcode, g10_contact_batch_id as contact_batch_id, * from t0 where g10_barcode is not null or g10_contact_batch_id is not null)
    union all (select g11_barcode as contact_barcode, g11_contact_batch_id as contact_batch_id,  *  from t0 where g11_barcode is not null or g11_contact_batch_id is not null) 
    union all (select g12_barcode as contact_barcode, g12_contact_batch_id as contact_batch_id, * from t0 where g12_barcode is not null or g12_contact_batch_id is not null)),
    t2 as (select * from {{ ref("int_saf_latest") }}),
    t3 as (select t1.*, t2.barcode as assessment_barcode, t2.* except(barcode) from t1 left join t2 on t1.contact_barcode = t2.barcode order by t1.contact_barcode),
    t4 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, batch_trainer_name, school, school_district, school_state, 
    school_partner, donor, no_of_students_facilitated from {{ ref("int_global") }}),
    t5 as (select t3.*, t4.batch_id as global_batch_id, t4.* except (batch_id) from t3 left join t4 on t3.contact_batch_id = t4.batch_id order by t3.contact_batch_id)
    select * from t5 order by full_name