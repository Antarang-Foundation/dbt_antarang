select student_name, first_barcode, student_barcode, gender, current_barcode, current_batch_no, birth_year, birth_date, g9_whatsapp_no, 
g10_whatsapp_no, g11_whatsapp_no, g12_whatsapp_no, g9_alternate_no, g10_alternate_no, g11_alternate_no, g12_alternate_no, religion, 
caste, father_education, father_occupation, mother_education, mother_occupation, student_details_2_submitted, batch_no, batch_academic_year, 
batch_grade, batch_language, no_of_students_facilitated, fac_start_date, fac_end_date, facilitator_name, facilitator_email, 
school_name, school_taluka, school_ward, school_district, school_state, enrolled_g9, enrolled_g10, enrolled_g11,enrolled_g12, 
tagged_for_counselling, school_partner, school_area, batch_donor from {{ref('dev_int_global_dcp')}}
where batch_academic_year >= 2023
