with

    t0 as (select contact_id, uid, full_name, gender, current_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, current_batch_no, 
    g9_contact_batch_id, g10_contact_batch_id, g11_contact_batch_id, g12_contact_batch_id, current_grade1, current_grade2 
    from {{ ref("stg_student") }}),

    t1 as ((select g9_barcode as contact_barcode, g9_contact_batch_id as contact_batch_id, 'Grade 9' as `contact_grade`, * from t0 where g9_barcode is not null or g9_contact_batch_id is not null) 
    union all (select g10_barcode as contact_barcode, g10_contact_batch_id as contact_batch_id, 'Grade 10' as `contact_grade`, * from t0 where g10_barcode is not null or g10_contact_batch_id is not null)
    union all (select g11_barcode as contact_barcode, g11_contact_batch_id as contact_batch_id, 'Grade 11' as `contact_grade`,  *  from t0 where g11_barcode is not null or g11_contact_batch_id is not null) 
    union all (select g12_barcode as contact_barcode, g12_contact_batch_id as contact_batch_id, 'Grade 12' as `contact_grade`, * from t0 where g12_barcode is not null or g12_contact_batch_id is not null))

    select contact_id, uid, contact_grade, contact_barcode, contact_batch_id,  * except (contact_id, uid, contact_grade, contact_barcode, contact_batch_id)  
    from t1 order by contact_id, uid, contact_grade, contact_barcode, contact_batch_id