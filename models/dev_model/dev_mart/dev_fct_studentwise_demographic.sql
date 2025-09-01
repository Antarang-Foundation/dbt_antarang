Select student_id, student_barcode, assessment_barcode, batch_no, batch_grade, batch_academic_year, school_state, school_district,
school_taluka, school_partner, school_area, batch_donor, school_name, reality_1, reality_2, reality_3, reality_5, reality_6,
reality_4, reality_7, reality_8, total_years_barcode_filled, gender, student_age, total_stud_have_report, sar_atleast_one_reality
From {{ ref('dev_int_studentwise_demographic') }}