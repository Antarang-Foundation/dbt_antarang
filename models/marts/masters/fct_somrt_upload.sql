{{ config(
    materialized='table',
    partition_by={
        "field": "batch_academic_year",
        "data_type": "int64",
        "range": {
            "start": 2026,
            "end": 2035,
            "interval": 1
        }
    }
) }}

WITH source AS (

    SELECT
        somrt_id,
        somrt_no,
        somrt_session_id,
        somrt_batch_id,
        omr_type,
        omr_assessment_object,
        omr_assessment_count,
        omr_assessment_record_type,
        somrt_batch_no,
        omr_received_count,
        omr_received_by,
        number_of_students_in_batch,
        omr_received_date,
        first_omr_upload_date
    FROM {{ ref('dev_stg_somrt') }}

),

int_student_global AS (

    SELECT DISTINCT
        batch_id,
        batch_no,
        batch_academic_year,
        batch_language,
        facilitator_id,
        facilitator_name,
        facilitator_email,
        school_id,
        school_name,
        school_taluka,
        school_ward,
        school_district,
        school_state,
        school_partner,
        school_area,
        donor_id,
        batch_donor,
        batch_grade,
        no_of_students_facilitated,
        fac_start_date,
        fac_end_date
    FROM {{ ref('dev_int_global_dcp') }}

),

session AS (

    SELECT
        session_id,
        session_name,
        session_date,
        total_student_present,
        omrs_received
    FROM {{ ref('stg_session') }}

),

t6 AS (

SELECT
    batch_id,
    SUM(DISTINCT no_of_students_facilitated) AS total_students,
    COUNT(DISTINCT bl_cdm1_no) AS bl_cdm1,
    COUNT(DISTINCT bl_cdm2_no) AS bl_cdm2,
    COUNT(DISTINCT bl_cp_no) AS bl_cp,
    COUNT(DISTINCT bl_cs_no) AS bl_cs,
    COUNT(DISTINCT bl_fp_no) AS bl_fp,
    COUNT(DISTINCT el_cdm1_no) AS el_cdm1,
    COUNT(DISTINCT el_cdm2_no) AS el_cdm2,
    COUNT(DISTINCT el_cp_no) AS el_cp,
    COUNT(DISTINCT el_cs_no) AS el_cs,
    COUNT(DISTINCT el_fp_no) AS el_fp,
    COUNT(DISTINCT saf_no) AS saf,
    COUNT(DISTINCT sar_no) AS sar,
    COUNT(DISTINCT sd2_barcode) AS total_sd2_students
FROM {{ ref('int_student_global_assessment_status') }}
GROUP BY batch_id

),

diagnostics AS (

SELECT
    batch_no,
    COUNT(DISTINCT bl_sd_no) AS bl_sd,
    COUNT(DISTINCT el_sd_no) AS el_sd
FROM {{ ref('int_student_diagnostic') }}
GROUP BY batch_no

),

cdm3 AS (

SELECT
    batch_no,
    COUNT(DISTINCT bl_ca2_no) AS bl_ca2,
    COUNT(DISTINCT el_ca2_no) AS el_ca2
FROM {{ ref('int_student_global_cdm2_ind') }}
GROUP BY batch_no

),

final as (SELECT

    b.batch_no,
    b.batch_academic_year,
    b.batch_language,
    b.facilitator_id,
    b.facilitator_name,
    b.facilitator_email,
    b.school_id,
    b.school_name,
    b.school_taluka,
    b.school_ward,
    b.school_district,
    b.school_state,
    b.school_partner,
    b.school_area,
    b.donor_id,
    b.batch_donor,
    b.batch_grade,
    b.no_of_students_facilitated,
    b.fac_start_date,
    b.fac_end_date,

    s.somrt_no,
    s.omr_assessment_record_type,
    s.omr_type,

    sess.session_date,
    sess.total_student_present,
    sess.omrs_received,

    s.omr_received_by,
    s.omr_received_date,
    s.omr_received_count,
    s.first_omr_upload_date,
    s.omr_assessment_count,

    CASE
    WHEN s.omr_type IN ('Self Awareness + Feedback', 'Quiz + Feedback') THEN t6.saf
    WHEN s.omr_type = 'Realities + Quiz' THEN t6.sar
    WHEN s.omr_type = 'Career Decision Making- 1A' THEN t6.bl_cdm1
    WHEN s.omr_type = 'Career Decision Making- 1B' THEN t6.el_cdm1
    WHEN s.omr_type = 'Career Decision Making- 2A' THEN t6.bl_cdm2
    WHEN s.omr_type = 'Career Decision Making- 2B' THEN t6.el_cdm2
    WHEN s.omr_type = 'Career Planning A' THEN t6.bl_cp
    WHEN s.omr_type = 'Career Planning B' THEN t6.el_cp
    WHEN s.omr_type = 'Career Skills A' THEN t6.bl_cs
    WHEN s.omr_type = 'Career Skills B' THEN t6.el_cs
    WHEN s.omr_type = 'Planning for Future A' THEN t6.bl_fp
    WHEN s.omr_type = 'Planning for Future B' THEN t6.el_fp
    WHEN s.omr_type = 'Student Details' THEN t6.total_students
    WHEN s.omr_type = 'Career Decision Making- 3A' THEN cdm3.bl_ca2
    WHEN s.omr_type = 'Career Decision Making- 3B' THEN cdm3.el_ca2
    WHEN s.omr_type = 'Student Diagnositcs A' THEN diagnostics.bl_sd
    WHEN s.omr_type = 'Student Diagnositcs B' THEN diagnostics.el_sd
    WHEN s.omr_type = 'Student Details 2' THEN t6.total_sd2_students
END AS omr_upload_count,
    ------------------------------------------------------------------
    -- TAT1 : Session Date → OMR Received Date
    ------------------------------------------------------------------
    CASE
        WHEN sess.session_date IS NOT NULL THEN
            CASE
                WHEN COALESCE(s.omr_received_date, CURRENT_DATE()) >= sess.session_date THEN
                    (
                        SELECT COUNTIF(EXTRACT(DAYOFWEEK FROM d) NOT IN (1,7))
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                sess.session_date,
                                COALESCE(s.omr_received_date, CURRENT_DATE())
                            )
                        ) d
                    )
                ELSE
                    -(
                        SELECT COUNTIF(EXTRACT(DAYOFWEEK FROM d) NOT IN (1,7))
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                COALESCE(s.omr_received_date, CURRENT_DATE()),
                                sess.session_date
                            )
                        ) d
                    )
            END
    END AS TAT1,

    ------------------------------------------------------------------
    -- TAT2 : Session Date → First OMR Upload Date
    ------------------------------------------------------------------
    CASE
        WHEN sess.session_date IS NOT NULL THEN
            CASE
                WHEN COALESCE(s.first_omr_upload_date, CURRENT_DATE()) >= sess.session_date THEN
                    (
                        SELECT COUNTIF(EXTRACT(DAYOFWEEK FROM d) NOT IN (1,7))
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                sess.session_date,
                                COALESCE(s.first_omr_upload_date, CURRENT_DATE())
                            )
                        ) d
                    )
                ELSE
                    -(
                        SELECT COUNTIF(EXTRACT(DAYOFWEEK FROM d) NOT IN (1,7))
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                COALESCE(s.first_omr_upload_date, CURRENT_DATE()),
                                sess.session_date
                            )
                        ) d
                    )
            END
    END AS TAT2

FROM int_student_global b

LEFT JOIN source s
    ON b.batch_id = s.somrt_batch_id

LEFT JOIN session sess
    ON s.somrt_session_id = sess.session_id

LEFT JOIN t6
    ON b.batch_id = t6.batch_id

LEFT JOIN diagnostics
    ON b.batch_no = diagnostics.batch_no

LEFT JOIN cdm3
    ON b.batch_no = cdm3.batch_no

WHERE b.batch_academic_year >= 2026

ORDER BY
    b.batch_no,
    sess.session_date,
    s.omr_type
)

select * from final