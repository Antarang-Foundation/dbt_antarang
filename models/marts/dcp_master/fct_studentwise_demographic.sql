WITH t1 AS (
    SELECT 
        student_id, student_barcode, batch_no, batch_grade, batch_academic_year, 
        school_state, school_district, school_taluka, school_partner, school_area, 
        batch_donor, school_name, reality_1, reality_2, reality_3, reality_5, 
        reality_6, reality_4, reality_7, reality_8 
        --sar_atleast_one_reality, total_years_barcode_filled, gender, student_age, total_stud_have_report
    FROM {{ ref('int_ca_student_global') }} where batch_academic_year < 2022
),

t2 AS (
    SELECT 
        student_id, assessment_barcode as student_barcode, batch_no, batch_grade, batch_academic_year, 
        school_state, school_district, school_taluka, school_partner, school_area, 
        batch_donor, school_name, 
        r1s1 AS reality_1, 
        r2s2 AS reality_2, 
        r3s3 AS reality_3, 
        r5f1 AS reality_5, 
        r6f2 AS reality_6,  
        r4s4 AS reality_4, 
        r7f3 AS reality_7, 
        r8f4 AS reality_8
    FROM {{ ref('int_student_global_sar') }} where batch_academic_year >= 2022
),

t3 AS (
    SELECT * FROM t1 
    UNION ALL
    SELECT * FROM t2
), 

t4 as (
       SELECT * FROM t3 where student_barcode is not null      
),

t5 as (
    select stud_id, student_barcode as stud_barcode, total_years_barcode_filled, gender, student_age, total_stud_have_report
    FROM {{ ref('int_ca_student_global') }}
),

t6 as (
    select * from t4 left join t5 on t4.student_id = t5.stud_id
),

t7 as (
     SELECT 
        t6.*,
        CASE 
            WHEN COALESCE(reality_1, reality_2, reality_3, reality_4, reality_5, reality_6, reality_7, reality_8) IS NULL 
            THEN 0 
            ELSE 1 
        END AS sar_atleast_one_reality,
        CASE 
            WHEN COALESCE(reality_1, reality_2, reality_3, reality_4, reality_5, reality_6, reality_7, reality_8) IS NOT NULL 
            THEN student_barcode 
        END AS assessment_barcode
    FROM t6
)

select 
student_id,
stud_barcode as student_barcode,
--student_barcode as assessment_barcode,
assessment_barcode,
batch_no,
batch_grade,
batch_academic_year,
school_state,
school_district,
school_taluka,
school_partner,
school_area,
batch_donor,
school_name,
reality_1,
reality_2,
reality_3,
reality_5,
reality_6,
reality_4,
reality_7,
reality_8,
total_years_barcode_filled,
gender,
student_age,
total_stud_have_report,
sar_atleast_one_reality
from t7

