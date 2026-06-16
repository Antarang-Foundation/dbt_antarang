SELECT student_id, student_barcode, gender, batch_no, batch_academic_year, batch_language, facilitator_id, facilitator_name, 
facilitator_email, school_id, school_name, school_taluka, school_ward, school_district, school_state, school_partner, school_area, 
donor_id, batch_donor, batch_grade, assessment_barcode, bl_createddate, bl_ca2_no, bl_q3_1, bl_q3_2, bl_q3_3, bl_q3_4, bl_q3_5, 
bl_q3_6, bl_q3_7, bl_q3_8, bl_q3_9, bl_q3_10, bl_q3_1_marks, bl_q3_2_marks, bl_q3_3_marks, bl_q3_4_marks, bl_q3_5_marks, 
bl_q3_6_marks, bl_q3_7_marks, bl_q3_8_marks, bl_q3_9_marks, bl_q3_10_marks, el_createddate, el_ca2_no, el_q3_1, el_q3_2, el_q3_3, 
el_q3_4, el_q3_5, el_q3_6, el_q3_7, el_q3_8, el_q3_9, el_q3_10, el_q3_1_marks, el_q3_2_marks, el_q3_3_marks, el_q3_4_marks, 
el_q3_5_marks, el_q3_6_marks, el_q3_7_marks, el_q3_8_marks, el_q3_9_marks, el_q3_10_marks, bl_q3_total_marks, el_q3_total_marks, 
bl_q3_bucket, el_q3_bucket, bl_el_q3_total_marks, bl_el_q3_status, type_of_assessment_filled, bl_el_q3_1, bl_el_q3_2, bl_el_q3_3, 
bl_el_q3_4, bl_el_q3_5, bl_el_q3_6, bl_el_q3_7, bl_el_q3_8, bl_el_q3_9, bl_el_q3_10
    FROM {{ ref('int_score_cdm2_ind') }}

