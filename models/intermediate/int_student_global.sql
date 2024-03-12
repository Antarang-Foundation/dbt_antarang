with

    t0 as (select * from {{ ref("int_student") }}),
    
    t1 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, facilitator_name, school, school_taluka_id, school_ward_id, 
    school_district_id, school_state_id, school_partner, batch_donor, no_of_students_facilitated from {{ ref("int_global") }}),

    t2 as (select t0.*, t1.batch_id as global_batch_id, t1.* except(batch_id) from t0 full outer join t1 on t0.contact_batch_id = t1.batch_id)

    select * from t2 order by student_id, contact_grade

