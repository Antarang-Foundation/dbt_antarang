WITH t1 AS (
    SELECT 
        student_id, student_barcode, batch_no, batch_grade, batch_academic_year, 
        school_state, school_district, school_taluka, school_partner, school_area, 
        batch_donor, school_name, total_years_barcode_filled, gender, student_age, 
        total_stud_have_report
          FROM {{ ref('int_ca_student_global') }} where student_barcode is not null )  

select * from t1