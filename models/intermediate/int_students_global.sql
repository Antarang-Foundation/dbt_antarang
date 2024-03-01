with 

t0 as (select uid, current_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, gender, g9_contact_batch_id, g10_contact_batch_id, 
g11_contact_batch_id, g12_contact_batch_id, current_grade from {{ ref('int_students') }}),

t1 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated, school, school_district,
 school_state, school_partner, batch_trainer_name, donor from {{ ref('int_global') }}),
 
t2 as (select * from t0 left join t1 on 

(case

when g9_contact_batch_id = batch_id then g9_contact_batch_id
when g10_contact_batch_id = batch_id then g10_contact_batch_id
when g11_contact_batch_id = batch_id then g11_contact_batch_id
when g12_contact_batch_id = batch_id then g12_contact_batch_id end) = batch_id)

select * from t2 order by uid

