with

    t0 as (select student_id, first_barcode, student_name, gender, current_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, current_batch_no, 
    g9_batch_id, g10_batch_id, g11_batch_id, g12_batch_id, current_grade1, current_grade2 
    from {{ ref("stg_student") }}),

    t1 as ((select g9_barcode as contact_barcode, g9_batch_id as contact_batch_id, 'Grade 9' as `contact_grade`, * from t0 where g9_barcode is not null or g9_batch_id is not null) 
    union all (select g10_barcode as contact_barcode, g10_batch_id as contact_batch_id, 'Grade 10' as `contact_grade`, * from t0 where g10_barcode is not null or g10_batch_id is not null)
    union all (select g11_barcode as contact_barcode, g11_batch_id as contact_batch_id, 'Grade 11' as `contact_grade`,  *  from t0 where g11_barcode is not null or g11_batch_id is not null) 
    union all (select g12_barcode as contact_barcode, g12_batch_id as contact_batch_id, 'Grade 12' as `contact_grade`, * from t0 where g12_barcode is not null or g12_batch_id is not null))

    select student_id, student_name, first_barcode, contact_grade, contact_barcode, contact_batch_id,  
    * except (student_id, first_barcode, student_name, contact_grade, contact_barcode, contact_batch_id, g9_barcode, g10_barcode, g11_barcode, g12_barcode, 
    g9_batch_id, g10_batch_id, g11_batch_id, g12_batch_id)  

    from t1 order by student_id, contact_grade 