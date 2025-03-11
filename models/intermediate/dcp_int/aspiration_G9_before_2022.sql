WITH t1 AS (
    SELECT 
        student_id,
        student_barcode,
        gender,
        batch_no,
        batch_academic_year,
        batch_grade,
        batch_language,
        school_state,
        school_district,
        school_taluka,
        school_partner,
        batch_donor
        FROM {{ ref('int_student_global') }}
    where batch_grade = 'Grade 9' and batch_academic_year < 2022
),

t2 AS (
    SELECT 
        student_id AS stud_id, student_barcode AS stud_barcode, current_aspiration, 
        profession_1, profession_2, profession_3
    FROM {{ ref('aspiration') }}
),

t3 AS (
    SELECT * 
    FROM t1 
    LEFT OUTER JOIN t2 
    ON t1.student_barcode = t2.stud_barcode
),

t4 AS (
    -- Row for profession_1
    SELECT 
        t3.*, 
        profession_1 AS endline_stud_aspiration,
        'PC1 and CA' AS aspiration_mapping
    FROM t3
    WHERE profession_1 IS NOT NULL

    UNION ALL

    -- Row for profession_2
    SELECT 
        t3.*, 
        profession_2 AS endline_stud_aspiration,
        'PC2' AS aspiration_mapping
    FROM t3
    WHERE profession_2 IS NOT NULL

    UNION ALL

    -- Row for profession_3
    SELECT 
        t3.*, 
        profession_3 AS endline_stud_aspiration,
        'PC3' AS aspiration_mapping
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
            WHEN aspiration_mapping = 'PC1 and CA' THEN current_aspiration 
            ELSE 'NA' 
        END AS baseline_stud_aspiration 
    FROM t5
),

t7 AS (
    SELECT 
        t6.*,  
        CAST(NULL AS STRING) AS bl_assessment_barcode, 
        CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no,
        CAST(NULL AS STRING) AS el_assessment_barcode
    FROM t6
)

SELECT 
    student_id,
    student_barcode,
    gender,
    batch_no,
    batch_academic_year,
    batch_grade,
    batch_language,
    school_state,
    school_district,
    school_taluka,
    school_partner,
    batch_donor,
    bl_assessment_barcode,
    bl_cdm1_no,
    el_cdm1_no,
    el_assessment_barcode,
    aspiration_mapping,
    baseline_stud_aspiration, 
    endline_stud_aspiration
FROM t7
