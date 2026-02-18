WITH batches AS (
    SELECT * FROM {{ ref("dev_stg_batch") }}
),
schools AS (
    SELECT * FROM {{ ref("dev_stg_school") }}
),
facilitators AS (
    SELECT * FROM {{ ref("dev_stg_facilitator") }}
),
donors AS (
    SELECT * FROM {{ ref("dev_stg_donor") }}
),

-- base student expansion across grades
student_expanded AS (
    SELECT 
        s.student_id,
        s.student_name,
        s.gender,
        s.birth_year,
        s.birth_date,
        g.student_grade,
        g.student_barcode,
        g.student_batch_id,
        g.whatsapp_no,
        g.alternate_no,
        s.first_barcode,
        s.current_barcode,
        s.current_batch_no,
        s.current_grade1,
        s.current_grade2,
        s.religion,
        s.caste,
        s.father_education,
        s.father_occupation,
        s.mother_education,
        s.mother_occupation,
        s.possible_career_report,
        s.career_tracks,
        s.clarity_report,
        s.current_aspiration,
        s.possible_careers_1,
        s.possible_careers_2,
        s.possible_careers_3,
        s.followup_1_aspiration,
        s.followup_2_aspiration,
        s.reality_1,
        s.reality_2,
        s.reality_3,
        s.reality_4,
        s.reality_5,
        s.reality_6,
        s.reality_7,
        s.reality_8,
        s.g9_barcode,
        s.g10_barcode,
        s.g11_barcode,
        s.g12_barcode,
        s.g9_batch_id,
        s.g10_batch_id,
        s.g11_batch_id,
        s.g12_batch_id,
        s.recommedation_status,
        s.recommendation_report_status,
        s.student_details_2_submitted,
        s.g9_whatsapp_no, s.g10_whatsapp_no, s.g11_whatsapp_no, s.g12_whatsapp_no, s.g9_alternate_no, 
        s.g10_alternate_no, s.g11_alternate_no, s.g12_alternate_no,
        s.student_details_2_grade, 
s.Student_GRADE_COUNT, s.G9_Batch_Student_Flag, s.G10_Batch_Student_Flag, s.G11_Batch_Student_Flag, s.G12_Batch_Student_Flag,
        -- Counting number of non-null barcodes
        (
            CASE WHEN s.g9_barcode IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN s.g10_barcode IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN s.g11_barcode IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN s.g12_barcode IS NOT NULL THEN 1 ELSE 0 END
        ) AS total_years_barcode_filled
    FROM {{ ref("dev_stg_student") }} s,
    UNNEST([
        STRUCT('Grade 9' AS student_grade, s.g9_barcode AS student_barcode, s.g9_batch_id AS student_batch_id, s.g9_whatsapp_no AS whatsapp_no, s.g9_alternate_no AS alternate_no),
        STRUCT('Grade 10' AS student_grade, s.g10_barcode AS student_barcode, s.g10_batch_id AS student_batch_id, s.g10_whatsapp_no AS whatsapp_no, s.g10_alternate_no AS alternate_no),
        STRUCT('Grade 11' AS student_grade, s.g11_barcode AS student_barcode, s.g11_batch_id AS student_batch_id, s.g11_whatsapp_no AS whatsapp_no, s.g11_alternate_no AS alternate_no),
        STRUCT('Grade 12' AS student_grade, s.g12_barcode AS student_barcode, s.g12_batch_id AS student_batch_id, s.g12_whatsapp_no AS whatsapp_no, s.g12_alternate_no AS alternate_no)
    ]) AS g
),

-- batches + school + facilitators + donors
batch_context AS (
    SELECT 
        b.batch_id, b.batch_no, CAST(b.batch_academic_year AS INT) AS batch_academic_year, b.batch_grade, b.batch_language, 
        b.no_of_students_facilitated, b.fac_start_date, b.fac_end_date, b.allocation_email_sent, b.batch_facilitator_id, f.facilitator_academic_year,
        f.facilitator_id, f.facilitator_name, f.facilitator_email, b.batch_school_id, s.school_id, s.school_name, 
        s.school_taluka, s.school_ward, s.school_district, s.school_state, s.school_academic_year, s.school_language, 
        s.enrolled_g9, s.enrolled_g10, s.enrolled_g11, s.enrolled_g12, s.tagged_for_counselling, s.school_partner, 
        s.school_area, b.batch_donor_id, d.donor_id, d.donor_name AS batch_donor
    FROM batches b
    LEFT JOIN schools s ON b.batch_school_id = s.school_id
    LEFT JOIN facilitators f ON b.batch_facilitator_id = f.facilitator_id
    LEFT JOIN donors d ON b.batch_donor_id = d.donor_id
),

-- combine student with batch context
student_batch AS (
    SELECT
        se.*,
        bc.*,
        COALESCE(se.student_batch_id, 'Unmapped') AS resolved_batch_id,
        (CAST(bc.batch_academic_year AS INT) - CAST(se.birth_year AS INT)) AS student_age,
        CASE WHEN bc.batch_id IS NULL THEN 1 ELSE 0 END AS is_unmapped_student
    FROM student_expanded se
    RIGHT JOIN batch_context bc
      ON se.student_batch_id = bc.batch_id
),

-- add SAR and recommendation checks
final AS (
    SELECT 
        sb.*,
        CASE 
            WHEN recommedation_status = 'Processed' AND recommendation_report_status = 'Report processed' 
            THEN 1 ELSE 0 END AS total_stud_have_report,
        CASE 
            WHEN reality_1 IS NULL AND reality_2 IS NULL AND reality_3 IS NULL 
                 AND reality_4 IS NULL AND reality_5 IS NULL 
                 AND reality_6 IS NULL AND reality_7 IS NULL 
                 AND reality_8 IS NULL THEN 0 
            ELSE 1 
        END AS sar_atleast_one_reality
    FROM student_batch sb
),

t1 as (SELECT student_id , student_name , gender , birth_year , birth_date , student_grade , student_barcode , 
student_batch_id , whatsapp_no , alternate_no , first_barcode , current_barcode , current_batch_no , current_grade1 , 
current_grade2 , religion , caste , father_education , father_occupation , mother_education , mother_occupation , 
possible_career_report , career_tracks , clarity_report , current_aspiration , possible_careers_1 , possible_careers_2 , 
possible_careers_3 , followup_1_aspiration , followup_2_aspiration , reality_1 , reality_2 , reality_3 , reality_4 , reality_5 , 
reality_6 , reality_7 , reality_8 , g9_barcode , g10_barcode , g11_barcode , g12_barcode , recommedation_status , 
recommendation_report_status , student_details_2_submitted , g9_whatsapp_no , g10_whatsapp_no , g11_whatsapp_no , g12_whatsapp_no , 
g9_alternate_no , g10_alternate_no , g11_alternate_no , g12_alternate_no , 
CASE
    WHEN student_details_2_grade IS NULL THEN 'N/A'
    WHEN REGEXP_CONTAINS(student_details_2_grade, r'(9|IX)')
         THEN 'Grade 9'
    WHEN REGEXP_CONTAINS(student_details_2_grade, r'(10|X)')
         THEN 'Grade 10'
    WHEN REGEXP_CONTAINS(student_details_2_grade, r'(11|XI)')
         THEN 'Grade 11'
    WHEN REGEXP_CONTAINS(student_details_2_grade, r'(12|XII)')
         THEN 'Grade 12'
    ELSE 'N/A'
END AS student_details_2_grade, 
CASE 
    WHEN student_details_2_grade != 'N/A'
    THEN student_barcode
    ELSE NULL
END AS sd2_student_barcode,
Student_GRADE_COUNT , G9_Batch_Student_Flag , G10_Batch_Student_Flag , G11_Batch_Student_Flag , G12_Batch_Student_Flag , 
total_years_barcode_filled , batch_id , batch_no , batch_academic_year , batch_grade , batch_language , no_of_students_facilitated , 
fac_start_date , fac_end_date , allocation_email_sent , batch_facilitator_id , facilitator_academic_year , facilitator_id , 
facilitator_name , facilitator_email , batch_school_id , school_id , school_name , school_taluka , school_ward , school_district , 
school_state , school_academic_year , school_language , enrolled_g9 , enrolled_g10 , enrolled_g11 , enrolled_g12 , 
tagged_for_counselling , school_partner , school_area , batch_donor_id , donor_id , batch_donor , resolved_batch_id , student_age , 
is_unmapped_student , total_stud_have_report , sar_atleast_one_reality

FROM final
)

select * from t1


--where student_details_2_grade != 'N/A'

/*Case when(s.g9_batch_id is not null and s.student_details_2_grade like '%9%' and s.batch_grade = 'Grade 9') THEN 'Grade 9'
        when(s.g10_batch_id is not null and s.student_details_2_grade like '%10%' and s.batch_grade = 'Grade 10') THEN 'Grade 10'
        when(s.g11_batch_id is not null and s.student_details_2_grade like '%11%' and s.batch_grade = 'Grade 11') THEN 'Grade 11'
        when(s.g12_batch_id is not null and s.student_details_2_grade like '%12%' and s.batch_grade = 'Grade 12') THEN 'Grade 12' END AS 
        student_details_2_grade*/