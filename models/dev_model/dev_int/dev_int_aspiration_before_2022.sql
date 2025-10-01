WITH grade9_t1 AS (
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
    FROM {{ ref('dev_int_global_dcp') }}
),

grade9_t2 AS (
    SELECT 
        student_id AS stud_id, 
        student_barcode AS stud_barcode, 
        current_aspiration, 
        profession_1, 
        profession_2, 
        profession_3
    FROM {{ ref('dev_int_aspiration') }}
),

grade9_t3 AS (
    SELECT * 
    FROM grade9_t1 g9
    LEFT JOIN grade9_t2 g92
    ON g9.student_barcode = g92.stud_barcode
),

grade9_t4 AS (
    -- PC1
    SELECT g3.*, profession_1 AS endline_stud_aspiration, 'PC1 and CA' AS aspiration_mapping
    FROM grade9_t3 g3 WHERE profession_1 IS NOT NULL

    UNION ALL
    -- PC2
    SELECT g3.*, profession_2 AS endline_stud_aspiration, 'PC2' AS aspiration_mapping
    FROM grade9_t3 g3 WHERE profession_2 IS NOT NULL

    UNION ALL
    -- PC3
    SELECT g3.*, profession_3 AS endline_stud_aspiration, 'PC3' AS aspiration_mapping
    FROM grade9_t3 g3 WHERE profession_3 IS NOT NULL
),

grade9_t5 AS (
    SELECT 
        g4.*,
        ROW_NUMBER() OVER (PARTITION BY stud_barcode ORDER BY (SELECT NULL)) AS rn
    FROM grade9_t4 g4
),

grade9_final AS (
    SELECT 
        g5.*,
        CASE 
            WHEN aspiration_mapping = 'PC1 and CA' THEN current_aspiration 
            ELSE 'NA' 
        END AS baseline_stud_aspiration,
        CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no
    FROM grade9_t5 g5
),

-- ================================
-- Grade 10 Branch
-- ================================
grade10_t1 AS (
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
        current_aspiration, 
        followup_1_aspiration, 
        followup_2_aspiration
    FROM {{ ref('dev_int_global_dcp') }}
),

grade10_t2 AS (
    -- FU1
    SELECT g10.*, followup_1_aspiration AS endline_stud_aspiration, 'Follow_up1' AS aspiration_mapping
    FROM grade10_t1 g10 WHERE followup_1_aspiration IS NOT NULL

    UNION ALL
    -- FU2
    SELECT g10.*, followup_2_aspiration AS endline_stud_aspiration, 'Follow_up2' AS aspiration_mapping
    FROM grade10_t1 g10 WHERE followup_2_aspiration IS NOT NULL
),

grade10_final AS (
    SELECT 
        g10t2.*,
        CASE 
            WHEN current_aspiration IS NOT NULL THEN 'NA' 
            WHEN current_aspiration IS NULL THEN 'NA' 
            ELSE '0'  
        END AS baseline_stud_aspiration,
        CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no
    FROM grade10_t2 g10t2
),

-- ================================
-- Final Output (Grade 9 + Grade 10)
-- ================================
final as ( SELECT 
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
    bl_cdm1_no,
    el_cdm1_no,
    aspiration_mapping,
    baseline_stud_aspiration,
    endline_stud_aspiration
FROM grade9_final

UNION ALL

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
    bl_cdm1_no,
    el_cdm1_no,
    aspiration_mapping,
    baseline_stud_aspiration,
    endline_stud_aspiration
FROM grade10_final
)

select * from final
