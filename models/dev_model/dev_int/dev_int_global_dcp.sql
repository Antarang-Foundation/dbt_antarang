-- Step 1: Base batch, school, facilitator, and donor information
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

-- Step 2: Global DCP dataset with batch and school metadata
t0 AS (
    SELECT
        b.batch_id,
        b.batch_no,
        CAST(b.batch_academic_year AS INT) AS batch_academic_year,
        b.batch_grade, b.batch_language, b.no_of_students_facilitated, b.fac_start_date, b.fac_end_date, b.allocation_email_sent, b.batch_facilitator_id, f.facilitator_id, f.facilitator_name, f.facilitator_email,
        b.batch_school_id, s.school_id, s.school_name, s.school_taluka, s.school_ward, s.school_district, s.school_state, s.school_academic_year, s.school_language, s.enrolled_g9, s.enrolled_g10, s.enrolled_g11, 
        s.enrolled_g12, s.tagged_for_counselling, s.school_partner, s.school_area, b.batch_donor_id, d.donor_id,
        d.donor_name AS batch_donor
    FROM batches b
    LEFT JOIN schools s ON b.batch_school_id = s.school_id
    LEFT JOIN facilitators f ON b.batch_facilitator_id = f.facilitator_id
    LEFT JOIN donors d ON b.batch_donor_id = d.donor_id
),

-- Step 3: Flatten student data grade-wise and Student basic and grade-wise information
t1 AS (
    SELECT
        COALESCE(s.g9_barcode, s.g10_barcode, s.g11_barcode, s.g12_barcode) AS student_barcode,
        CASE 
            WHEN s.g9_barcode IS NOT NULL THEN s.g9_batch_id
            WHEN s.g10_barcode IS NOT NULL THEN s.g10_batch_id
            WHEN s.g11_barcode IS NOT NULL THEN s.g11_batch_id
            WHEN s.g12_barcode IS NOT NULL THEN s.g12_batch_id
        END AS student_batch_id,
        CASE 
            WHEN s.g9_barcode IS NOT NULL THEN 'Grade 9'
            WHEN s.g10_barcode IS NOT NULL THEN 'Grade 10'
            WHEN s.g11_barcode IS NOT NULL THEN 'Grade 11'
            WHEN s.g12_barcode IS NOT NULL THEN 'Grade 12'
        END AS student_grade,

        -- Explicit fields needed later
        s.student_id, s.student_name, s.first_barcode, s.gender, s.birth_year, s.birth_date, s.current_barcode, s.current_batch_no, s.g9_whatsapp_no, s.g10_whatsapp_no, s.g11_whatsapp_no, s.g12_whatsapp_no, s.g9_alternate_no, s.g10_alternate_no, s.g11_alternate_no, s.g12_alternate_no,
        s.current_grade1, s.current_grade2, s.religion, s.caste, s.father_education, s.father_occupation, s.mother_education, s.mother_occupation, s.possible_career_report, s.career_tracks, s.clarity_report, s.current_aspiration, s.possible_careers_1, s.possible_careers_2, s.possible_careers_3,
        s.followup_1_aspiration, s.followup_2_aspiration, s.reality_1, s.reality_2, s.reality_3, s.reality_4, s.reality_5, s.reality_6, s.reality_7, s.reality_8, s.recommedation_status, s.recommendation_report_status, s.student_details_2_submitted, s.g9_barcode, s.g10_barcode, s.g11_barcode, s.g12_barcode
 
    FROM {{ ref("dev_stg_student") }} s
    WHERE s.g9_barcode IS NOT NULL OR s.g10_barcode IS NOT NULL OR s.g11_barcode IS NOT NULL OR s.g12_barcode IS NOT NULL
)

-- Step 4: Join with base_dcp to get batch, facilitator, and school info
SELECT
        dcp.*, fs.student_id, fs.student_name, fs.student_grade, fs.student_barcode, fs.first_barcode, fs.gender, fs.birth_year, fs.birth_date, fs.current_barcode, fs.student_batch_id, fs.current_batch_no, fs.g9_whatsapp_no, fs.g10_whatsapp_no, fs.g11_whatsapp_no, fs.g12_whatsapp_no, fs.g9_alternate_no, fs.g10_alternate_no, fs.g11_alternate_no, fs.g12_alternate_no,
        fs.current_grade1, fs.current_grade2, fs.religion, fs.caste, fs.father_education, fs.father_occupation, fs.mother_education, fs.mother_occupation, fs.possible_career_report, fs.career_tracks, fs.clarity_report, fs.current_aspiration, fs.possible_careers_1, fs.possible_careers_2, fs.possible_careers_3, fs.followup_1_aspiration, fs.followup_2_aspiration,
        fs.reality_1, fs.reality_2, fs.reality_3, fs.reality_4, fs.reality_5, fs.reality_6, fs.reality_7, fs.reality_8, fs.recommedation_status, fs.recommendation_report_status, fs.student_details_2_submitted,
        (
            CASE WHEN fs.g9_barcode IS NOT NULL THEN 1
             WHEN fs.g10_barcode IS NOT NULL THEN 1 
             WHEN fs.g11_barcode IS NOT NULL THEN 1 
             WHEN fs.g12_barcode IS NOT NULL THEN 1 ELSE 0 END
        ) AS total_years_barcode_filled,
        CASE 
            WHEN fs.recommedation_status = 'Processed' AND fs.recommendation_report_status = 'Report processed' THEN 1
            ELSE 0
        END AS total_stud_have_report,
        CASE 
            WHEN fs.reality_1 IS NULL AND fs.reality_2 IS NULL AND fs.reality_3 IS NULL AND fs.reality_4 IS NULL AND 
                 fs.reality_5 IS NULL AND fs.reality_6 IS NULL AND fs.reality_7 IS NULL AND fs.reality_8 IS NULL THEN 0
            ELSE 1
        END AS sar_atleast_one_reality,
        (CAST(dcp.batch_academic_year AS INT) - CAST(fs.birth_year AS INT)) AS student_age
    FROM t1 fs
    LEFT JOIN t0 dcp ON fs.student_batch_id = dcp.batch_id

