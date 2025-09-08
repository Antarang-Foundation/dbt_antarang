with 

-- Step 1: Clean assessment records
t0 as (
    select 
        assessment_barcode, record_type, created_on, created_from_form, cdm1_no, is_non_null, 
        q1, q1_marks, q2_1, q2_2, q2_marks, q3_1, q3_2, q3_marks, 
        q4_1, q4_1_marks, q4_2, q4_2_marks, q4_marks, 
        cdm1_total_marks, error_status, data_cleanup, marks_recalculated, student_linked
    from {{ ref('dev_stg_cdm1') }}
    where data_cleanup = true 
      and marks_recalculated = true 
      and student_linked = true
),

-- Step 2: Deduplicate assessments (latest per student+record_type)
t1 as (
    select 
        t0.*,
        row_number() over (
            partition by t0.assessment_barcode, t0.record_type 
            order by t0.created_on desc
        ) as rn
    from t0
),

t2 as (
    select 
        assessment_barcode, record_type, created_on, created_from_form, cdm1_no, is_non_null, 
        q1, q1_marks, q2_1, q2_2, q2_marks, q3_1, q3_2, q3_marks, 
        q4_1, q4_1_marks, q4_2, q4_2_marks, q4_marks, cdm1_total_marks
    from t1
    where rn = 1
),

-- Step 3: Deduplicate student-batch mapping (latest batch per student)
t3 as (
    select *
    from (
        select 
            student_barcode, gender, caste, batch_id, batch_no, batch_academic_year, batch_grade, batch_language,
            facilitator_name, facilitator_email, school_name, school_taluka, school_ward, school_district, 
            school_state, school_partner, school_area, batch_donor,
            row_number() over (
                partition by student_barcode 
                order by batch_academic_year desc, batch_no desc
            ) as rn
        from {{ ref("dev_int_global_dcp") }}
        where batch_academic_year >= 2023
    )
    where rn = 1
),

-- Step 4: Join assessments with students
t4 as (
    select 
        t3.*,
        t2.*
    from t2
    left join t3 
        on t3.student_barcode = t2.assessment_barcode
)

-- Final output
select * 
from t4