select * except (student_id, student_batch_id, batch_id,  batch_facilitator_id, batch_school_id, facilitator_id, school_id, batch_donor_id, 
donor_id, r1s1_marks, r2s2_marks, r3s3_marks, r4s4_marks, r5f1_marks, r6f2_marks, r7f3_marks, r8f4_marks) 

from {{ref('int_student_global_assessment')}}

