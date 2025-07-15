with 
t1 as (select 
student_id, 
first_barcode, --cast(first_barcode as INTEGER) as uid,
student_barcode, gender, batch_id, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated,
batch_facilitator_id, facilitator_name, batch_school_id, school_id, school_name,
school_taluka, school_district, school_state, school_academic_year, tagged_for_counselling, school_partner, school_area, batch_donor
from {{ref('fct_student_global')}}),

t2 as (
    select 
        batch_no, 
        batch_academic_year,
        count(distinct student_id) as total_student_beneficiaries
    from t1
    group by batch_no, batch_academic_year
)

select * from t2

