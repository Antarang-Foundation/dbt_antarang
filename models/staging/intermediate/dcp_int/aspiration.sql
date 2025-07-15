WITH t1 AS (
    SELECT 
        student_id, student_name, student_barcode, batch_no, batch_academic_year, batch_grade,
        current_aspiration, possible_careers_1, possible_careers_2, possible_careers_3, 
        followup_1_aspiration, followup_2_aspiration
    FROM {{ ref('int_student_global') }}
),

t2 AS (
    SELECT id AS career_id, profession_name FROM {{ ref('stg_iarp_master') }}
),

t3 AS (
    SELECT 
        t1.*, 
        t2_1.profession_name AS profession_1
    FROM t1
    LEFT JOIN t2 AS t2_1 ON t1.possible_careers_1 = t2_1.career_id
),

t4 AS (
    SELECT 
        t3.*, 
        t2_2.profession_name AS profession_2
    FROM t3
    LEFT JOIN t2 AS t2_2 ON t3.possible_careers_2 = t2_2.career_id
),

t5 AS (
    SELECT 
        t4.*, 
        t2_3.profession_name AS profession_3
    FROM t4
    LEFT JOIN t2 AS t2_3 ON t4.possible_careers_3 = t2_3.career_id
)

SELECT 
    student_id, student_name, student_barcode, batch_no, batch_academic_year, batch_grade,
    current_aspiration, profession_1, profession_2, profession_3, 
    followup_1_aspiration, followup_2_aspiration
FROM t5
