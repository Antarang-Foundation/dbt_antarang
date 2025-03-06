with t1 as (
    select student_id as stud_id, 
        stud_barcode,
        bl_assessment_barcode,
        bl_cdm1_no, 
        baseline_stud_aspiration,
        aspiration_mapping from {{ref('int_stud_bl_aspiration')}}
),

t2 as (
    select 
        student_id,
        gender, 
        student_barcode, 
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        batch_language, 
        school_state, 
        school_district, 
        school_taluka, 
        school_partner, 
        batch_donor
        from {{ref('int_student_global')}}
        where batch_academic_year >= 2022 and batch_academic_year is not null and batch_grade is not null
),

t3 as (
    select * from t2 left join t1 on t2.student_barcode = t1.stud_barcode
),

t4 as (
    select 
        el_student_id, 
        el_cdm1_no,
        el_assessment_barcode, 
        el_assessment_academic_year,
        endline_stud_aspiration, 
        el_aspiration_mapping
        from {{ref('int_stud_el_aspiration')}}
),

t5 as (
    select * from t3 left join t4 on t3.student_barcode = t4.el_assessment_barcode
    and t3.aspiration_mapping = t4.el_aspiration_mapping   -- this means we can see only records whose both bl and el aspiration there
)

select 
student_id, student_barcode,gender, batch_no, batch_academic_year, batch_grade, batch_language, school_state, school_district, school_taluka, school_partner, 
batch_donor,bl_assessment_barcode, bl_cdm1_no, el_cdm1_no, el_assessment_barcode, 
aspiration_mapping, baseline_stud_aspiration, endline_stud_aspiration
from t5


