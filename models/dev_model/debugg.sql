WITH t1 AS (
    SELECT 
        id AS career_id, 
        name AS profession_name
    FROM {{ source('salesforce', 'IARP_Master__c') }}
    WHERE IsDeleted = false
),

t2 AS (
    SELECT 
        student_id, student_name, student_barcode, batch_no, batch_academic_year, batch_grade, current_aspiration, possible_careers_1, 
        possible_careers_2, possible_careers_3, followup_1_aspiration, followup_2_aspiration FROM {{ ref('dev_int_global_dcp') }}
),

t3 AS (
    SELECT 
        t2.student_id, t2.student_name, t2.student_barcode, t2.batch_no, t2.batch_academic_year, t2.batch_grade, t2.current_aspiration,
        t1a.profession_name AS profession_1,
        t1b.profession_name AS profession_2,
        t1c.profession_name AS profession_3,
        t2.followup_1_aspiration, t2.followup_2_aspiration
    FROM t2
    LEFT JOIN t1 AS t1a ON t2.possible_careers_1 = t1a.career_id
    LEFT JOIN t1 AS t1b ON t2.possible_careers_2 = t1b.career_id
    LEFT JOIN t1 AS t1c ON t2.possible_careers_3 = t1c.career_id
),

t4 AS (
    SELECT 
        d.assessment_barcode, d.record_type, d.cdm1_no, d.q4_1, d.q4_2, t3.*
    FROM {{ ref('dev_stg_cdm1') }} d
    LEFT JOIN t3 ON t3.student_barcode = d.assessment_barcode
),

t5 AS (
    SELECT 
        t4.student_id, t4.student_name, t4.student_barcode, t4.batch_no, t4.batch_academic_year, t4.batch_grade, t4.current_aspiration,
        t4.profession_1, t4.profession_2, t4.profession_3, t4.followup_1_aspiration, t4.followup_2_aspiration,
        t4.assessment_barcode, t4.record_type, t4.cdm1_no, t4.q4_1, t4.q4_2,

        MAX(CASE WHEN t4.record_type = 'Baseline' THEN t4.q4_1 END) AS bl_q4_1,
        MAX(CASE WHEN t4.record_type = 'Endline'  THEN t4.q4_1 END) AS el_q4_1,

        MAX(CASE WHEN t4.record_type = 'Baseline' THEN t4.q4_2 END) AS bl_q4_2,
        MAX(CASE WHEN t4.record_type = 'Endline'  THEN t4.q4_2 END) AS el_q4_2,

        MAX(CASE WHEN t4.record_type = 'Baseline' THEN t4.cdm1_no END) AS bl_cdm1_no,
        MAX(CASE WHEN t4.record_type = 'Endline'  THEN t4.cdm1_no END) AS el_cdm1_no
    FROM t4
    GROUP BY 
        t4.student_id, t4.student_name, t4.student_barcode, t4.batch_no, t4.batch_academic_year, t4.batch_grade, t4.current_aspiration,
        t4.profession_1, t4.profession_2, t4.profession_3, t4.followup_1_aspiration, t4.followup_2_aspiration, t4.assessment_barcode,
        t4.record_type, t4.cdm1_no, t4.q4_1, t4.q4_2
),

-- baseline unpivot fixed
t6 AS (
    SELECT 
        student_id, student_name, student_barcode, batch_no, batch_academic_year, batch_grade, current_aspiration, profession_1,
        profession_2, profession_3, CAST(bl_cdm1_no AS STRING) AS bl_cdm1_no,
        CAST(NULL AS STRING) AS el_cdm1_no, followup_1_aspiration, followup_2_aspiration, assessment_barcode, 'Baseline' AS record_type,
        cdm1_no, q4_1, q4_2, bl_aspiration AS aspiration_col, baseline_stud_aspiration AS stud_aspiration
    FROM t5
    UNPIVOT (
        baseline_stud_aspiration FOR bl_aspiration IN (bl_q4_1, bl_q4_2)
    ) AS unpvt1
),

-- endline unpivot
t7 AS (
    SELECT 
        student_id, student_name, student_barcode, batch_no, batch_academic_year, batch_grade, current_aspiration, 
        profession_1, profession_2, profession_3, CAST(NULL AS STRING) AS bl_cdm1_no,
        CAST(el_cdm1_no AS STRING) AS el_cdm1_no, followup_1_aspiration, followup_2_aspiration, assessment_barcode,
        'Endline' AS record_type, cdm1_no, q4_1, q4_2, el_aspiration AS aspiration_col, endline_stud_aspiration AS stud_aspiration
    FROM t5
    UNPIVOT (
        endline_stud_aspiration FOR el_aspiration IN (el_q4_1, el_q4_2)
    ) AS unpvt2
),

-- combine baseline + endline
t8 AS (
    SELECT * FROM t6
    UNION ALL
    SELECT * FROM t7
),

-- map aspiration column to q4_1 / q4_2
t9 AS (
    SELECT 
        t8.*,
        CASE 
            WHEN aspiration_col IN ('bl_q4_1','el_q4_1') THEN 'q4_1'
            WHEN aspiration_col IN ('bl_q4_2','el_q4_2') THEN 'q4_2'
            ELSE 'DNA'
        END AS aspiration_mapping
    FROM t8
),

t10 AS (
    SELECT
        student_id, student_name, student_barcode, batch_no, batch_academic_year, batch_grade, current_aspiration, profession_1,
        profession_2, profession_3, followup_1_aspiration, followup_2_aspiration, assessment_barcode, aspiration_mapping,
        MAX(bl_cdm1_no) AS bl_cdm1_no,
        MAX(el_cdm1_no) AS el_cdm1_no,
        MAX(CASE WHEN record_type = 'Baseline' THEN stud_aspiration END) AS baseline_stud_aspiration,
        MAX(CASE WHEN record_type = 'Endline'  THEN stud_aspiration END) AS endline_stud_aspiration
    FROM t9
    GROUP BY
        student_id, student_name, student_barcode, batch_no, batch_academic_year, batch_grade, current_aspiration, profession_1,
        profession_2, profession_3, followup_1_aspiration, followup_2_aspiration, assessment_barcode, aspiration_mapping
)

SELECT *
FROM t10
where student_barcode IN ('2401111310', '2401118642', '2401020073')
order by student_barcode
-- 2025 - 15481
-- 2024 - 103273
