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

WHERE b.batch_academic_year >= 2026

ORDER BY
    b.batch_no,
    sess.session_date,
    s.omr_type
)

select * from final