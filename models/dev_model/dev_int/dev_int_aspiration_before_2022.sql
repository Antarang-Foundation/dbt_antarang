WITH 
-- ================================
-- Grade 9 Branch
-- ================================

-- Step 1: Base Grade 9 Data
grade9_t1 AS (
    SELECT 
        student_id,
        COALESCE(student_barcode, 'MISSING') AS student_barcode,
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
        possible_careers_1, 
        possible_careers_2, 
        possible_careers_3,
        followup_1_aspiration,
        followup_2_aspiration
    FROM {{ ref('dev_int_global_dcp') }}
    WHERE LOWER(batch_grade) = 'grade 9'
),

-- Step 2: Master Career Mapping
career_master AS (
    SELECT 
        id AS career_id, 
        name AS profession_name
    FROM {{ source('salesforce', 'IARP_Master__c') }}
    WHERE IsDeleted = false
),

-- Step 3: Map possible careers to profession names
grade9_t2 AS (
    SELECT 
        g9.student_id,
        g9.student_barcode,
        g9.gender,
        g9.batch_no,
        g9.batch_academic_year,
        g9.batch_grade,
        g9.batch_language,
        g9.school_state,
        g9.school_district,
        g9.school_taluka,
        g9.school_partner,
        g9.batch_donor,
        g9.current_aspiration,
        cm1.profession_name AS profession_1,
        cm2.profession_name AS profession_2,
        cm3.profession_name AS profession_3
    FROM grade9_t1 g9
    LEFT JOIN career_master cm1 ON g9.possible_careers_1 = cm1.career_id
    LEFT JOIN career_master cm2 ON g9.possible_careers_2 = cm2.career_id
    LEFT JOIN career_master cm3 ON g9.possible_careers_3 = cm3.career_id
),

-- Step 4: Reshape data to create one row per aspiration (PC1/PC2/PC3)
grade9_t3 AS (
    SELECT g92.*, profession_1 AS endline_stud_aspiration, 'PC1 and CA' AS aspiration_mapping
    FROM grade9_t2 g92
    UNION ALL
    SELECT g92.*, profession_2 AS endline_stud_aspiration, 'PC2' AS aspiration_mapping
    FROM grade9_t2 g92
    UNION ALL
    SELECT g92.*, profession_3 AS endline_stud_aspiration, 'PC3' AS aspiration_mapping
    FROM grade9_t2 g92
),

-- Step 5: Final Grade 9 table with baseline and endline aspirations
grade9_final AS (
    SELECT 
        g3.student_id,
        g3.student_barcode,
        g3.gender,
        g3.batch_no,
        g3.batch_academic_year,
        g3.batch_grade,
        g3.batch_language,
        g3.school_state,
        g3.school_district,
        g3.school_taluka,
        g3.school_partner,
        g3.batch_donor,
        g3.aspiration_mapping,
        g3.endline_stud_aspiration,
        CASE 
            WHEN g3.aspiration_mapping = 'PC1 and CA' THEN g3.current_aspiration
            ELSE 'NA' 
        END AS baseline_stud_aspiration,
        CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no
    FROM grade9_t3 g3
),

-- ================================
-- Grade 10 Branch
-- ================================

-- Step 1: Base Grade 10 Data
grade10_t1 AS (
    SELECT 
        student_id,
        COALESCE(student_barcode, 'MISSING') AS student_barcode,
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
    WHERE LOWER(batch_grade) = 'grade 10'
),

-- Step 2: Reshape followups into endline aspirations
grade10_t2 AS (
    SELECT g10.*, followup_1_aspiration AS endline_stud_aspiration, 'Follow_up1' AS aspiration_mapping
    FROM grade10_t1 g10
    UNION ALL
    SELECT g10.*, followup_2_aspiration AS endline_stud_aspiration, 'Follow_up2' AS aspiration_mapping
    FROM grade10_t1 g10
),

-- Step 3: Final Grade 10 table
grade10_final AS (
    SELECT 
        g10t2.student_id,
        g10t2.student_barcode,
        g10t2.gender,
        g10t2.batch_no,
        g10t2.batch_academic_year,
        g10t2.batch_grade,
        g10t2.batch_language,
        g10t2.school_state,
        g10t2.school_district,
        g10t2.school_taluka,
        g10t2.school_partner,
        g10t2.batch_donor,
        g10t2.aspiration_mapping,
        g10t2.endline_stud_aspiration,
        'NA' AS baseline_stud_aspiration,
        CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no
    FROM grade10_t2 g10t2
),

-- ================================
-- Final Output
-- ================================
final AS (
    SELECT * FROM grade9_final
    UNION ALL
    SELECT * FROM grade10_final
)

SELECT *
FROM final
WHERE batch_academic_year < 2022 