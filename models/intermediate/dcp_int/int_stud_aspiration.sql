WITH t1 AS (
    SELECT * 
    FROM {{ ref('int_student_bl_aspiration') }}
),

t2 AS (
    SELECT * 
    FROM {{ ref('int_stud_el_aspiration_yearwise') }}
),

t3 AS (
   select * from t1 full outer join t2 
   on t1.bl_assessment_barcode = t2.el_assessment_barcode and t1.aspiration_mapping = t2.el_aspiration_mapping
)

SELECT * FROM t3
