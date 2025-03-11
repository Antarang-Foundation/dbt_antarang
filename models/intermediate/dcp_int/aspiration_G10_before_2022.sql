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
        batch_donor,
        current_aspiration, 
        followup_1_aspiration, 
        followup_2_aspiration
    FROM {{ ref('int_student_global') }}
    WHERE batch_grade = 'Grade 10' AND batch_academic_year < 2022
),

t2 AS (
    -- Row for followup_1_aspiration
    SELECT 
        t1.*, 
        followup_1_aspiration AS endline_stud_aspiration,
        'Follow_up1' AS aspiration_mapping
    FROM t1
    WHERE followup_1_aspiration IS NOT NULL

    UNION ALL

    -- Row for followup_2_aspiration
    SELECT 
        t1.*, 
        followup_2_aspiration AS endline_stud_aspiration,
        'Follow_up2' AS aspiration_mapping
    FROM t1
    WHERE followup_2_aspiration IS NOT NULL
),

t3 AS (
    SELECT 
        t2.*,
        CASE 
            WHEN current_aspiration IS NOT NULL THEN 'NA' 
            When current_aspiration IS NULL THEN 'NA' 
            ELSE '0'  
        END AS baseline_stud_aspiration 
    FROM t2
),

t4 AS (
    SELECT 
        t3.*,  
        CAST(NULL AS STRING) AS bl_assessment_barcode, 
        CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no,
        CAST(NULL AS STRING) AS el_assessment_barcode
    FROM t3
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
FROM t4
