select 
student_id, student_barcode, batch_no, batch_academic_year, school_state, school_district, school_taluka, 
reality_1, reality_2, reality_3, reality_5, reality_6, total_years_barcode_filled, gender, student_age, total_stud_have_report

from {{ref('int_ca_student_global')}}