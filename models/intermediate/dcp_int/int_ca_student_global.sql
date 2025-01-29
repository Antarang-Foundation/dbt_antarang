WITH t1 AS (
    SELECT 
        student_id as stud_id, first_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, 
        reality_1, reality_2, reality_3, reality_5, reality_6, recommedation_status, recommendation_report_status, aspiration_1, aspiration_2, aspiration_3,
        -- Counting the number of non-null grade barcodes for each student
        (
            CASE WHEN g9_barcode IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN g10_barcode IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN g11_barcode IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN g12_barcode IS NOT NULL THEN 1 ELSE 0 END
        ) AS total_years_barcode_filled
    FROM {{ref('stg_student')}}
),


t2 as (
    select student_id, student_barcode, batch_no, batch_academic_year, batch_grade,
    school_state, school_district, school_taluka, 
    gender, birth_year, no_of_students_facilitated, 
    (batch_academic_year - birth_year) AS student_age
from {{ref('int_student_global')}}
),

t3 as (
    select * from t1 inner join t2 on t1.stud_id = t2.student_id
),

t4 as(
    select t3.*,
     CASE 
            WHEN recommedation_status = 'Processed' AND recommendation_report_status = 'Report processed' 
            THEN 1 
            ELSE 0 
        END AS total_stud_have_report
        from t3
)

select * from t4

