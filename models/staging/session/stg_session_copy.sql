WITH t0 AS (
    SELECT 
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        no_of_students_facilitated, 
        school_district, 
        school_state, 
        session_no,  
        total_student_present
    FROM {{ref('int_global_session')}}
    WHERE school_district IN ('Nagaland', 'Palghar', 'Yamunanagar', 'RJ Model B')
),

t1 AS (
    SELECT *
    FROM t0
    PIVOT (
        MAX(total_student_present) 
        FOR session_no IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
    ) AS p
),


renamed as (
    select 
        batch_no as batch_no,
        batch_academic_year as batch_academic_year,
        batch_grade as batch_grade,
        no_of_students_facilitated as no_of_students_facilitated,
        school_district as school_district,
        school_state as school_state,
        _1 as total_student_present_s1,
        _2 as total_student_present_s2,
        _3 as total_student_present_s3,
        _4 as total_student_present_s4,
        _5 as total_student_present_s5,
        _6 as total_student_present_s6,
        _7 as total_student_present_s7,
        _8 as total_student_present_s8,
        _9 as total_student_present_s9,
        _10 as total_student_present_s10,
        _11 as total_student_present_s11,
        _12 as total_student_present_s12,
        _13 as total_student_present_s13,
        _14 as total_student_present_s14,
  from t1  
)

SELECT * FROM renamed
