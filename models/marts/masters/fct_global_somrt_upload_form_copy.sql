WITH t1 AS (
    SELECT 
        COUNT(DISTINCT student_barcode) AS combined_sd, 
        COUNT(DISTINCT assessment_barcode) AS combined_barcodes,
        batch_no as batch_no
    FROM {{ref('fct_student_global_assessment_status')}} 
     GROUP BY 
        batch_no
),

t2 AS (
    SELECT 
        batch_no AS session_batch_no, 
        session_name,
        session_type, 
        total_student_present, 
        total_parent_present, 
        present_count, 
        attendance_count
    FROM {{ref('fct_global_session')}}
),

t3 AS (
    SELECT * 
    FROM t1
    FULL OUTER JOIN t2 ON t1.batch_no = t2.session_batch_no
)

SELECT * 
FROM t3