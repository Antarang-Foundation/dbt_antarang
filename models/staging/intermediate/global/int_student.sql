with

    t0 as (select student_id, first_barcode, student_name, gender, current_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, current_batch_no, birth_year, birth_date, 
    g9_whatsapp_no, g10_whatsapp_no, g11_whatsapp_no, g12_whatsapp_no, g9_alternate_no, g10_alternate_no, g11_alternate_no, g12_alternate_no, religion, caste, father_education, father_occupation, mother_education, mother_occupation,
    g9_batch_id, g10_batch_id, g11_batch_id, g12_batch_id, current_grade1, current_grade2, possible_career_report, career_tracks, clarity_report, current_aspiration, possible_careers_1, possible_careers_2, possible_careers_3, followup_1_aspiration, followup_2_aspiration, student_details_2_submitted
    from {{ ref("stg_student") }}),

    t1 as ((select g9_barcode as student_barcode, g9_batch_id as student_batch_id, 'Grade 9' as `student_grade`, * from t0 where g9_barcode is not null or g9_batch_id is not null) 
    union all (select g10_barcode as student_barcode, g10_batch_id as student_batch_id, 'Grade 10' as `student_grade`, * from t0 where g10_barcode is not null or g10_batch_id is not null)
    union all (select g11_barcode as student_barcode, g11_batch_id as student_batch_id, 'Grade 11' as `student_grade`,  *  from t0 where g11_barcode is not null or g11_batch_id is not null) 
    union all (select g12_barcode as student_barcode, g12_batch_id as student_batch_id, 'Grade 12' as `student_grade`, * from t0 where g12_barcode is not null or g12_batch_id is not null))

    select student_id, student_name, first_barcode, student_grade, student_barcode, student_batch_id,  
    * except (student_id, first_barcode, student_name, student_grade, student_barcode, student_batch_id, g9_barcode, g10_barcode, g11_barcode, g12_barcode, 
    g9_batch_id, g10_batch_id, g11_batch_id, g12_batch_id)  

    from t1 order by student_id, student_grade 