WITH t1 AS (
    SELECT 
        student_id as stud_id, first_barcode, g9_barcode, g10_barcode, g11_barcode, g12_barcode, 
        reality_1, reality_2, reality_3, reality_5, reality_6, 
        reality_4, reality_7, reality_8,
        recommedation_status, recommendation_report_status, aspiration_1, aspiration_2, aspiration_3,
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
    school_partner, school_area, batch_donor, school_name,
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
        END AS total_stud_have_report,

        case when reality_1 is null and reality_2 is null and reality_3 is null and reality_4 is null and reality_5 is null and reality_6 is null and reality_7 is null and 
reality_8 is null then 1 else 0 end as sar_with_no_reality_data
    from t3
)

select * from t4

