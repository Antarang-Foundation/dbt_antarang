with

    t0 as (select * from {{ ref("int_student") }}),
    
    t1 as (select batch_id, batch_no, batch_academic_year, batch_grade, batch_language, 
        no_of_students_facilitated, fac_start_date,
        allocation_email_sent, batch_facilitator_id, facilitator_id, facilitator_name, facilitator_email, 
        batch_school_id, school_id, school_name, school_taluka, school_ward, school_district, school_state, 
        school_academic_year, school_language, enrolled_g9, enrolled_g10, enrolled_g11, enrolled_g12, 
        tagged_for_counselling, school_partner, school_area, batch_donor_id, donor_id, batch_donor  from {{ ref("int_global") }}),

    t2 as (select * from t0 full outer join t1 on t0.student_batch_id = t1.batch_id),

    t3 as (select *,
    CASE
    WHEN student_barcode = g9_barcode  AND student_details_2_grade = 'Grade 9'  THEN 'Grade 9'
    WHEN student_barcode = g10_barcode AND student_details_2_grade = 'Grade 10' THEN 'Grade 10'
    WHEN student_barcode = g11_barcode AND student_details_2_grade = 'Grade 11' THEN 'Grade 11'
    WHEN student_barcode = g12_barcode AND student_details_2_grade = 'Grade 12' THEN 'Grade 12'
END AS sd2_grade
from t2
),

t4 as (select *,
CASE 
    WHEN sd2_grade IS NOT NULL
    THEN student_barcode
    ELSE NULL
END AS sd2_barcode
from t3
    where batch_academic_year>=2023 
    order by student_id, student_grade
)

select * from t4


