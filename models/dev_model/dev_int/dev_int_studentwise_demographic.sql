with t0 AS (
    SELECT
        student_id, student_barcode, batch_no, batch_grade, batch_academic_year, school_state, school_district, school_taluka, 
        school_partner,
        school_area, batch_donor, school_name, reality_1, reality_2, reality_3, reality_4, reality_5, reality_6, reality_7, 
        reality_8,
      sar_atleast_one_reality, gender, student_age, total_stud_have_report, total_years_barcode_filled
    FROM {{ ref('dev_int_global_dcp') }} where student_barcode is not null
),

t1 AS (
    SELECT 
        assessment_barcode AS student_barcode, 
        r1s1, r2s2, r3s3, r4s4,
        r5f1, r6f2, r7f3, r8f4
    FROM {{ ref('dev_int_sar_latest') }}
),

t2 AS (
    SELECT
        t0.student_id, t0.student_barcode, t0.batch_no, t0.batch_grade, t0.batch_academic_year,
        t0.school_state, t0.school_district, t0.school_taluka, t0.school_partner, 
        t0.gender, t0.student_age,
        t0.school_area, t0.batch_donor, t0.school_name, t0.total_stud_have_report, t0.total_years_barcode_filled, 
        t0.sar_atleast_one_reality,

        COALESCE(t0.reality_1, t1.r1s1) AS reality_1,
        COALESCE(t0.reality_2, t1.r2s2) AS reality_2,
        COALESCE(t0.reality_3, t1.r3s3) AS reality_3,
        COALESCE(t0.reality_4, t1.r4s4) AS reality_4,
        COALESCE(t0.reality_5, t1.r5f1) AS reality_5,
        COALESCE(t0.reality_6, t1.r6f2) AS reality_6,
        COALESCE(t0.reality_7, t1.r7f3) AS reality_7,
        COALESCE(t0.reality_8, t1.r8f4) AS reality_8
    FROM t0
    LEFT JOIN t1 ON t0.student_barcode = t1.student_barcode
),

t3 AS (
    SELECT 
    t2.*,
    CASE 
        WHEN COALESCE(reality_1, reality_2, reality_3, reality_4, reality_5, reality_6, reality_7, reality_8) IS NOT NULL 
        THEN student_barcode 
        END AS assessment_barcode
    FROM t2
)

SELECT * FROM t3