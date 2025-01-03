/* total accessed chatbot
chatbot students
AF students 
*/ 

WITH t1 AS (
    SELECT * FROM {{ ref('fct_bot_contact_global') }}
),

t2 AS (
    SELECT 
        batch_no, 
        batch_academic_year, 
        batch_grade, 
        batch_language, 
        no_of_students_facilitated, 
        school_name, 
        school_state, 
        school_district, 
        batch_donor,
        
        -- Count of unique students who accessed the chatbot
        COUNT(DISTINCT CASE WHEN student_on_chatbot = 1 THEN student_barcode END) 
        AS chatbot_reach,

        -- Adoption percentage calculation
        CASE 
            WHEN no_of_students_facilitated IS NOT NULL AND no_of_students_facilitated != 0 
            THEN COUNT(DISTINCT CASE WHEN student_on_chatbot = 1 THEN student_barcode END) 
                / no_of_students_facilitated * 100
            ELSE 0
        END AS adoption_percentage
    FROM t1
    GROUP BY 
        batch_no, batch_academic_year, batch_grade, batch_language, 
        no_of_students_facilitated, school_name, school_state, 
        school_district, batch_donor
)

SELECT * FROM t2

