Select student_id, student_barcode, assessment_barcode, batch_no, batch_grade, batch_academic_year, school_state, school_district,
school_taluka, school_partner, school_area, batch_donor, school_name, reality_1, reality_2, reality_3, reality_5, reality_6,
reality_4, reality_7, reality_8, gender, student_age, sar_atleast_one_reality, total_stud_have_report, total_years_barcode_filled
From {{ ref('dev_int_studentwise_demographic') }}

--total_stud_have_report, total_years_barcode_filled
--count(student_id) = 460584
--count(distinct student_id) = 416243
--count(student_barcode) = 460584
--count(distinct student_barcode) = 460582