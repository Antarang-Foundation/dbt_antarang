WITH global_ca AS (
    SELECT
    student_id,
    student_barcode,
    gender,
    batch_no,
    batch_academic_year,
    batch_language,
    facilitator_id,
    facilitator_name,
    facilitator_email,
    school_id,
    school_name,
    school_taluka,
    school_ward,
    school_district,
    school_state,
    school_partner,
    school_area,
    donor_id,
    batch_donor,
    batch_grade,
    assessment_barcode,

    bl_createddate AS bl_ca_createddate,
    bl_ca1_no,

    bl_2a_a,
    bl_2a_b,
    bl_2a_c,
    bl_2a_d,
    bl_2a_e,
    bl_2a_f,
    bl_2a_g,
    bl_2a_h,

    bl_q2_a_marks,

    CASE
        WHEN bl_q2_a_marks IS NULL THEN 'Data Not Available'
        WHEN bl_q2_a_marks BETWEEN 4 AND 5 THEN 'High Awareness'
        WHEN bl_q2_a_marks = 3 THEN 'Moderate Awareness'
        WHEN bl_q2_a_marks BETWEEN 1 AND 2 THEN 'Low Awareness'
        WHEN bl_q2_a_marks = 0 THEN 'No Awareness'
    END AS bl_q2_a_bucket,

    bl_2b_a,
    bl_2b_b,
    bl_2b_c,
    bl_2b_d,
    bl_2b_e,
    bl_2b_f,
    bl_2b_g,

    bl_q2_b_marks,

    CASE
        WHEN bl_q2_b_marks IS NULL THEN 'Data Not Available'
        WHEN bl_q2_b_marks BETWEEN 4 AND 5 THEN 'High Awareness'
        WHEN bl_q2_b_marks = 3 THEN 'Moderate Awareness'
        WHEN bl_q2_b_marks BETWEEN 1 AND 2 THEN 'Low Awareness'
        WHEN bl_q2_b_marks = 0 THEN 'No Awareness'
    END AS bl_q2_b_bucket,

    el_createddate AS el_ca_createddate,
    el_ca1_no,

    el_q2_a_a,
    el_q2_a_b,
    el_q2_a_c,
    el_q2_a_d,
    el_q2_a_e,
    el_q2_a_f,
    el_q2_a_g,
    el_q2_a_h,

    el_q2_a_marks,

    CASE
        WHEN el_q2_a_marks IS NULL THEN 'Data Not Available'
        WHEN el_q2_a_marks BETWEEN 4 AND 5 THEN 'High Awareness'
        WHEN el_q2_a_marks = 3 THEN 'Moderate Awareness'
        WHEN el_q2_a_marks BETWEEN 1 AND 2 THEN 'Low Awareness'
        WHEN el_q2_a_marks = 0 THEN 'No Awareness'
    END AS el_2a_bucket,

    el_q2_b_a,
    el_q2_b_b,
    el_q2_b_c,
    el_q2_b_d,
    el_q2_b_e,
    el_q2_b_f,
    el_q2_b_g,

    el_q2_b_marks,

    CASE
        WHEN el_q2_b_marks IS NULL THEN 'Data Not Available'
        WHEN el_q2_b_marks BETWEEN 4 AND 5 THEN 'High Awareness'
        WHEN el_q2_b_marks = 3 THEN 'Moderate Awareness'
        WHEN el_q2_b_marks BETWEEN 1 AND 2 THEN 'Low Awareness'
        WHEN el_q2_b_marks = 0 THEN 'No Awareness'
    END AS el_2b_bucket,

    CASE
        WHEN el_q2_a_marks IS NULL
          OR bl_q2_a_marks IS NULL THEN NULL
        WHEN el_q2_a_marks > bl_q2_a_marks THEN 'Improvement'
        WHEN el_q2_a_marks < bl_q2_a_marks THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_2a,

    CASE
        WHEN el_q2_b_marks IS NULL
          OR bl_q2_b_marks IS NULL THEN NULL
        WHEN el_q2_b_marks > bl_q2_b_marks THEN 'Improvement'
        WHEN el_q2_b_marks < bl_q2_b_marks THEN 'Area for Growth'
        ELSE 'No Change'
    END AS bl_el_2b

FROM {{ ref('int_student_global_ca') }}
),

final AS (SELECT
student_id, student_barcode, gender, batch_no, batch_academic_year, batch_language, facilitator_id, facilitator_name, 
facilitator_email, school_id, school_name, school_taluka, school_ward, school_district, school_state, school_partner, 
school_area, donor_id, batch_donor, batch_grade, assessment_barcode, bl_ca_createddate, bl_ca1_no, bl_2a_a, bl_2a_b, bl_2a_c, 
bl_2a_d, bl_2a_e, bl_2a_f, bl_2a_g, bl_2a_h, bl_q2_a_marks, bl_q2_a_bucket, bl_2b_a, bl_2b_b, bl_2b_c, bl_2b_d, bl_2b_e, bl_2b_f, 
bl_2b_g, bl_q2_b_marks, bl_q2_b_bucket, el_ca_createddate, el_ca1_no, el_q2_a_a, el_q2_a_b, el_q2_a_c, el_q2_a_d, el_q2_a_e, 
el_q2_a_f, el_q2_a_g, el_q2_a_h, el_q2_a_marks, el_2a_bucket, el_q2_b_a, el_q2_b_b, el_q2_b_c, el_q2_b_d, el_q2_b_e, el_q2_b_f, 
el_q2_b_g, el_q2_b_marks, el_2b_bucket, bl_el_2a, bl_el_2b
FROM global_ca
)

select * from final