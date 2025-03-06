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
        followup_1_aspiration AS endline_stud_aspiration
    FROM t1
    --WHERE followup_1_aspiration IS NOT NULL

    UNION ALL

    -- Row for followup_2_aspiration
    SELECT 
        t1.*, 
        followup_2_aspiration AS endline_stud_aspiration
    FROM t1
    --WHERE followup_2_aspiration IS NOT NULL
),

t3 AS (
    SELECT 
        t2.*,
        CASE 
            WHEN current_aspiration IS NOT NULL THEN 'NA' 
            WHEN current_aspiration IS NULL THEN 'NA' 
            ELSE '0'  
        END AS baseline_stud_aspiration 
    FROM t2
),

t4 AS (
    SELECT 
        t3.*,
        CASE 
            WHEN followup_1_aspiration IS NOT NULL THEN 'Follow_up1'
            WHEN followup_1_aspiration is not null and followup_2_aspiration IS NOT NULL THEN 'Follow_up2'
            ELSE NULL
        END AS aspiration_mapping
    FROM t3
),

t5 AS (
    SELECT 
        t4.*,  
        CAST(NULL AS STRING) AS bl_assessment_barcode, 
        CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no,
        CAST(NULL AS STRING) AS el_assessment_barcode
    FROM t4
)

/*SELECT 
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
FROM t5
*/

select * from t5
