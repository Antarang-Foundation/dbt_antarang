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
        s.recommedation_status,
        s.recommendation_report_status,
        s.student_details_2_submitted,
        s.g9_whatsapp_no, s.g10_whatsapp_no, s.g11_whatsapp_no, s.g12_whatsapp_no, s.g9_alternate_no, 
        s.g10_alternate_no, s.g11_alternate_no, s.g12_alternate_no,
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
        b.no_of_students_facilitated, b.fac_start_date, b.fac_end_date, b.allocation_email_sent, b.batch_facilitator_id, 
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
)

SELECT *
FROM final