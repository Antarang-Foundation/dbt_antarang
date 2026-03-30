with source as (select id,
name as diagnostics_name,
grade as batch_grade, 
barcode as student_barcode,
a_q_1_1, a_q_1_2, a_q_1_3, a_q_1_4, a_q_1_5, a_q_1_6, a_q_1_7, a_q_1_8, a_q_1_9, a_q_1_10, b_q_1_1, b_q_1_2, b_q_1_3, 
b_q_1_4, b_q_1_5, b_q_1_6, b_q_1_7, b_q_1_8, b_q_1_9, b_q_1_10, 
batch_id,
form_name,	
record_type,
created_date,
error_status,
A_Q_1_1_Marks,
A_Q_1_2_Marks,
A_Q_1_3_Marks,
A_Q_1_4_Marks,  A_Q_1_5_Marks, A_Q_1_6_Marks, A_Q_1_7_Marks, A_Q_1_8_Marks, A_Q_1_9_Marks, B_Q_1_1_Marks, B_Q_1_2_Marks, 
B_Q_1_3_Marks, B_Q_1_4_Marks, B_Q_1_5_Marks, B_Q_1_6_Marks, B_Q_1_7_Marks, B_Q_1_8_Marks, B_Q_1_9_Marks, academic_year,	
data_clean_up, form_language, A_Q_1_10_Marks, B_Q_1_10_Marks, student_linked, created_from_form, marks_recalculated, 
form_submitted_set_a, form_submitted_set_b
 from {{ source('salesforce', 'Stage_') }}
)

select * from source