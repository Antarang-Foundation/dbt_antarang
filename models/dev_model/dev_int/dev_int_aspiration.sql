WITH 
-- Step 1: Base Student Data
t1 AS (
    SELECT 
        student_id,
        student_name,
        student_barcode,
        batch_no,
        batch_academic_year,
        batch_grade,
        current_aspiration,
        batch_language,
        school_state,
        school_district,
        school_taluka,
        school_partner,
        batch_donor
    FROM {{ ref('dev_int_global_dcp') }}
),

-- Step 2: Join CDM1 data with student data
t2 AS (
    SELECT 
        d.assessment_barcode,
        d.record_type,
        d.cdm1_no,
        d.q4_1,
        d.q4_2,
        t1.*
    FROM {{ ref('dev_stg_cdm1') }} d
    RIGHT JOIN t1 
        ON t1.student_barcode = d.assessment_barcode
),

-- Step 3: Aggregate baseline and endline values
t3 AS (
    SELECT 
        t2.student_id,
        t2.student_name,
        t2.student_barcode,
        t2.batch_no,
        t2.batch_academic_year,
        t2.batch_grade,
        t2.current_aspiration,
        t2.batch_language,
        t2.school_state,
        t2.school_district,
        t2.school_taluka,
        t2.school_partner,
        t2.batch_donor,
        t2.assessment_barcode,
        t2.record_type,
        t2.cdm1_no,
        t2.q4_1,
        t2.q4_2,

        MAX(CASE WHEN t2.record_type = 'Baseline' THEN t2.q4_1 END) AS bl_q4_1,
        MAX(CASE WHEN t2.record_type = 'Endline'  THEN t2.q4_1 END) AS el_q4_1,

        MAX(CASE WHEN t2.record_type = 'Baseline' THEN t2.q4_2 END) AS bl_q4_2,
        MAX(CASE WHEN t2.record_type = 'Endline'  THEN t2.q4_2 END) AS el_q4_2,

        MAX(CASE WHEN t2.record_type = 'Baseline' THEN t2.cdm1_no END) AS bl_cdm1_no,
        MAX(CASE WHEN t2.record_type = 'Endline'  THEN t2.cdm1_no END) AS el_cdm1_no
    FROM t2
    GROUP BY 
        t2.student_id,
        t2.student_name,
        t2.student_barcode,
        t2.batch_no,
        t2.batch_academic_year,
        t2.batch_grade,
        t2.current_aspiration,
        t2.batch_language,
        t2.school_state,
        t2.school_district,
        t2.school_taluka,
        t2.school_partner,
        t2.batch_donor,
        t2.assessment_barcode,
        t2.record_type,
        t2.cdm1_no,
        t2.q4_1,
        t2.q4_2
),

-- Step 4: Baseline unpivot
t4 AS (
    SELECT 
        student_id,
        student_name,
        student_barcode,
        batch_no,
        batch_academic_year,
        batch_grade,
        current_aspiration,
        batch_language,
        school_state,
        school_district,
        school_taluka,
        school_partner,
        batch_donor,
        CAST(bl_cdm1_no AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no,
        assessment_barcode,
        'Baseline' AS record_type,
        cdm1_no,
        q4_1,
        q4_2,
        bl_aspiration AS aspiration_col,
        baseline_stud_aspiration AS stud_aspiration
    FROM t3
    UNPIVOT (
        baseline_stud_aspiration FOR bl_aspiration IN (bl_q4_1, bl_q4_2)
    ) AS unpvt1
),

-- Step 5: Endline unpivot
t5 AS (
    SELECT 
        student_id,
        student_name,
        student_barcode,
        batch_no,
        batch_academic_year,
        batch_grade,
        current_aspiration,
        batch_language,
        school_state,
        school_district,
        school_taluka,
        school_partner,
        batch_donor,
        CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(el_cdm1_no AS STRING) AS el_cdm1_no,
        assessment_barcode,
        'Endline' AS record_type,
        cdm1_no,
        q4_1,
        q4_2,
        el_aspiration AS aspiration_col,
        endline_stud_aspiration AS stud_aspiration
    FROM t3
    UNPIVOT (
        endline_stud_aspiration FOR el_aspiration IN (el_q4_1, el_q4_2)
    ) AS unpvt2
),

-- Step 6: Combine baseline and endline
t6 AS (
    SELECT * FROM t4
    UNION ALL
    SELECT * FROM t5
),

-- Step 7: Map aspiration column to question type
t7 AS (
    SELECT 
        t6.*,
        CASE 
            WHEN aspiration_col IN ('bl_q4_1', 'el_q4_1') THEN 'q4_1'
            WHEN aspiration_col IN ('bl_q4_2', 'el_q4_2') THEN 'q4_2'
            ELSE 'DNA'
        END AS aspiration_mapping
    FROM t6
),

-- Step 8: Final aggregation
t8 AS (
    SELECT
        student_id,
        student_name,
        student_barcode,
        batch_no,
        batch_academic_year,
        batch_grade,
        current_aspiration,
        assessment_barcode,
        aspiration_mapping,
        batch_language,
        school_state,
        school_district,
        school_taluka,
        school_partner,
        batch_donor,
        MAX(bl_cdm1_no) AS bl_cdm1_no,
        MAX(el_cdm1_no) AS el_cdm1_no,
        MAX(CASE WHEN record_type = 'Baseline' THEN stud_aspiration END) AS baseline_stud_aspiration,
        MAX(CASE WHEN record_type = 'Endline'  THEN stud_aspiration END) AS endline_stud_aspiration
    FROM t7
    GROUP BY
        student_id,
        student_name,
        student_barcode,
        batch_no,
        batch_academic_year,
        batch_grade,
        current_aspiration,
        assessment_barcode,
        aspiration_mapping,
        batch_language,
        school_state,
        school_district,
        school_taluka,
        school_partner,
        batch_donor
)

-- Final Output
SELECT *
FROM t8