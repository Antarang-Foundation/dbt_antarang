with

    t0 as (select uid, full_name, gender, current_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, current_batch_no, g9_contact_batch_id, 
    g10_contact_batch_id, g11_contact_batch_id, g12_contact_batch_id, current_grade from {{ ref("int_students") }}),
    t1 as 
    
    ((select g9_contact_batch_id as contact_batch_id, * from t0 where g9_contact_batch_id is not null) 
    union all 
    (select g10_contact_batch_id as contact_batch_id, * from t0 where g10_contact_batch_id is not null)
    union all 
    (select g11_contact_batch_id as contact_batch_id,  *  from t0 where g11_contact_batch_id is not null) 
    union all 
    (select g12_contact_batch_id as contact_batch_id, * from t0 where g12_contact_batch_id is not null)),
    
    t2 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, batch_trainer_name, school, school_district, school_state, 
    school_partner, donor, no_of_students_facilitated from {{ ref("int_global") }}),

    t3 as (select t1.*, t2.batch_id as global_batch_id, t2.* except(batch_id) from t1 left join t2 on t1.contact_batch_id = t2.batch_id)

    select * from t3 order by full_name

