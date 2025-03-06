WITH t1 AS (
    SELECT * FROM {{ ref('fct_stud_aspiration') }}
    where batch_grade = 'Grade 9' and batch_academic_year < 2022
),

t2 AS (
    SELECT 
        student_id AS stud_id, student_barcode AS stud_barcode, current_aspiration, 
        profession_1, profession_2, profession_3, 
        followup_1_aspiration, followup_2_aspiration
    FROM {{ ref('test_aspiration') }}
),

t3 AS (
    SELECT * 
    FROM t1 
    FULL OUTER JOIN t2 
    ON t1.student_barcode = t2.stud_barcode
),

t4 AS (
    -- Row for profession_1
    SELECT 
        t3.*, 
        profession_1 AS possible_careers
    FROM t3
    WHERE profession_1 IS NOT NULL

    UNION ALL

    -- Row for profession_2
    SELECT 
        t3.*, 
        profession_2 AS possible_careers
    FROM t3
    WHERE profession_2 IS NOT NULL

    UNION ALL

    -- Row for profession_3
    SELECT 
        t3.*, 
        profession_3 AS possible_careers
    FROM t3
    WHERE profession_3 IS NOT NULL
),

t5 AS (
    SELECT 
        t4.*, 
        ROW_NUMBER() OVER (PARTITION BY stud_barcode ORDER BY (SELECT NULL)) AS rn
    FROM t4
),

t6 AS (
    SELECT 
        t5.*, 
        CASE 
            WHEN rn = 1 THEN current_aspiration 
            ELSE 'NA' 
        END AS updated_current_aspiration 
    FROM t5
)

SELECT * 
except (stud_barcode, stud_id,
    current_aspiration, profession_1, profession_2, profession_3
)
FROM t6
