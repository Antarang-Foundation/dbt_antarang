with bot as (
    select *
    from {{ref('stg_bot_contacts')}}
),

t0 as (
select student_id, first_barcode, student_name, gender, current_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, current_batch_no, birth_year, birth_date, 
    g9_whatsapp_no, g10_whatsapp_no, g11_whatsapp_no, g12_whatsapp_no, g9_alternate_no, g10_alternate_no, g11_alternate_no, g12_alternate_no, religion, caste, father_education, father_occupation, mother_education, mother_occupation,
    g9_batch_id, g10_batch_id, g11_batch_id, g12_batch_id, current_grade1, current_grade2, possible_career_report, career_tracks, clarity_report
from {{ ref("stg_student") }}
),

t1 as (
(select g9_barcode as student_barcode, g9_batch_id as student_batch_id, 'Grade 9' as `student_grade`, g9_whatsapp_no as student_contact, * from t0 where g9_barcode is not null or g9_batch_id is not null or g9_whatsapp_no is not null) 
    union all (select g10_barcode as student_barcode, g10_batch_id as student_batch_id, 'Grade 10' as `student_grade`, g10_whatsapp_no as student_contact, * from t0 where g10_barcode is not null or g10_batch_id is not null or g10_whatsapp_no is not null)
    union all (select g11_barcode as student_barcode, g11_batch_id as student_batch_id, 'Grade 11' as `student_grade`, g11_whatsapp_no as student_contact, *  from t0 where g11_barcode is not null or g11_batch_id is not null or g11_whatsapp_no is not null) 
    union all (select g12_barcode as student_barcode, g12_batch_id as student_batch_id, 'Grade 12' as `student_grade`, g12_whatsapp_no as student_contact, * from t0 where g12_barcode is not null or g12_batch_id is not null or g12_whatsapp_no is not null)
),

t3 as (

select student_id, student_name, first_barcode, student_grade, student_barcode, student_batch_id, student_contact,  
* except 
(student_id, first_barcode, student_name, student_grade, student_barcode, student_batch_id, student_contact, g9_barcode, g10_barcode, g11_barcode, g12_barcode, 
g9_batch_id, g10_batch_id, g11_batch_id, g12_batch_id, g9_whatsapp_no, g10_whatsapp_no, g11_whatsapp_no, g12_whatsapp_no)  
from t1 order by student_id, student_grade 
),


t4 AS (
    SELECT * FROM t3 
    LEFT JOIN bot AS b
    ON t3.student_contact = b.phone
    OR RIGHT(t3.student_contact, 10) = RIGHT(b.phone, 10)
),

t5 as (
    select 
    student_id, student_name, first_barcode, student_grade, student_barcode, student_batch_id, student_contact, 
    gender, phone,
    case when RIGHT(student_contact, 10) = RIGHT(phone, 10) then 1   
         else 0
    end as student_on_chatbot
    from t4
),

t6 as (
    select 
    batch_id, batch_no, batch_academic_year, batch_grade, batch_language, no_of_students_facilitated,
    facilitator_name, school_name, school_state, school_district, school_taluka, batch_donor, school_area
    from {{ref('int_global')}}
),

t7 as (
    select * from t5 inner join t6 on t5.student_batch_id = t6.batch_id
)

SELECT * FROM t7


