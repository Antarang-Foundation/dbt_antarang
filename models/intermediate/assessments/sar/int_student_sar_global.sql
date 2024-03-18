with

    t1 as (select * from {{ ref("int_student") }}),
    t2 as (select * from {{ ref("int_sar_latest") }}),
    t3 as (select t1.* except (first_barcode, current_barcode, current_batch_no, current_grade1, current_grade2), t2.barcode as assessment_barcode, 
    t2.* except(barcode) 
    from t1 left join t2 on t1.contact_barcode = t2.barcode 
    order by t1.contact_barcode),
    t4 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school_name, school_taluka, school_ward, school_district, 
    school_state, school_partner, batch_donor from {{ ref("int_global") }}),
    t5 as (select t3.* , t4.batch_id as global_batch_id, t4.* except (batch_id) from t3 left join t4 on t3.contact_batch_id = t4.batch_id 
    order by t3.contact_batch_id)
    select * from t5 order by student_id, batch_academic_year