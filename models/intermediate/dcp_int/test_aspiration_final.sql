WITH t1 AS (
    SELECT * FROM {{ ref('fct_stud_aspiration') }}
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
        profession_1 AS endline_stud_aspiration2
    FROM t3
    WHERE profession_1 IS NOT NULL

    UNION ALL

    -- Row for profession_2
    SELECT 
        t3.*, 
        profession_2 AS endline_stud_aspiration2
    FROM t3
    WHERE profession_2 IS NOT NULL

    UNION ALL

    -- Row for profession_3
    SELECT 
        t3.*, 
        profession_3 AS endline_stud_aspiration2
    FROM t3
    WHERE profession_3 IS NOT NULL
)

SELECT *
except (
    profession_1, profession_2, profession_3, baseline_stud_aspiration2
)
FROM t4
