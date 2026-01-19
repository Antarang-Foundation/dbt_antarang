with dev_int_aspiration as 
(Select student_id,	student_barcode, batch_no, gender, batch_academic_year,	batch_grade, batch_language, school_state,	
school_district, school_taluka,	school_partner,	batch_donor, current_aspiration, aspiration_mapping,	bl_cdm1_no,	el_cdm1_no,	
baseline_stud_aspiration, endline_stud_aspiration FROM {{ ref('dev_int_aspiration') }}
),

dev_int_asp_before_2022 as
(Select student_id,	student_barcode, batch_no, gender, batch_academic_year,	batch_grade, batch_language, school_state,	
school_district, school_taluka,	school_partner,	batch_donor, bl_cdm1_no, el_cdm1_no, aspiration_mapping, 
baseline_stud_aspiration, endline_stud_aspiration FROM {{ ref('dev_int_aspiration_before_2022') }}
),

aspiration as (SELECT 
   a.student_id, a.student_barcode, a.batch_no, a.gender, a.batch_academic_year, a.batch_grade, a.batch_language, a.school_state,
   a.school_district, a.school_taluka, a.school_partner, a.batch_donor, a.bl_cdm1_no, a.el_cdm1_no, a.aspiration_mapping, 
   a.baseline_stud_aspiration, a.endline_stud_aspiration FROM dev_int_aspiration a

   UNION ALL

   SELECT
   b.student_id, b.student_barcode, b.batch_no, b.gender, b.batch_academic_year, b.batch_grade, b.batch_language, b.school_state,
   b.school_district, b.school_taluka, b.school_partner, b.batch_donor, b.bl_cdm1_no, b.el_cdm1_no, b.aspiration_mapping, 
   b.baseline_stud_aspiration, b.endline_stud_aspiration FROM dev_int_asp_before_2022 b
),

final_aspiration AS (
    SELECT 
        aspiration.*,
        CASE 
            WHEN /*aspiration_mapping LIKE '%PC1 and CA%' 
                 OR aspiration_mapping LIKE '%q4_1%' 
                 OR aspiration_mapping LIKE '%q4_2%'*/
                 baseline_stud_aspiration IS NOT NULL 
            THEN student_barcode 
        END AS bl_assessment_barcode,
        CASE 
            WHEN endline_stud_aspiration IS NOT NULL 
            THEN student_barcode 
        END AS el_assessment_barcode
    FROM aspiration
)

select * from final_aspiration

--Order by student_id, aspiration_mapping

