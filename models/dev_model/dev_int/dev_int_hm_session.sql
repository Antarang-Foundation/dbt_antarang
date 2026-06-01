WITH int_global_dcp AS (
    SELECT 
        batch_language,
        batch_academic_year,
        school_name,
        school_taluka,
        school_ward,
        school_district,
        school_state,
        batch_donor,
        school_area,
        school_partner,
        school_id,
        facilitator_name,
        facilitator_email,
        ROW_NUMBER() OVER (
            PARTITION BY school_id, batch_academic_year 
            ORDER BY 
                CASE WHEN facilitator_name IS NOT NULL THEN 0 ELSE 1 END, 
                school_name DESC
        ) AS rn
    FROM {{ ref('dev_int_global_dcp') }}
    where facilitator_academic_year >= 2025 and batch_academic_year >= 2025 --AND school_name = 'Bhagwan Mahavir Govt. High School, Honda'
),

int_session AS (
    SELECT 
        MIN(CASE WHEN fac_start_date IS NOT NULL THEN fac_start_date END) AS fac_start_date,
        MIN(CASE WHEN fac_end_date IS NOT NULL THEN fac_end_date END) AS fac_end_date, 
        COUNT(CONCAT(session_name, '_', TRIM(batch_grade))) AS batch_expected_session,
        COUNT(CASE WHEN session_type = 'Student' THEN CONCAT(session_name, '_', TRIM(batch_grade)) END) AS total_student_present,
        COUNT(CASE WHEN session_type = 'Parent' THEN CONCAT(session_name, '_', TRIM(batch_grade)) END) AS total_parent_present,
        MIN(CASE WHEN session_date IS NOT NULL THEN session_date END) AS session_date,
        school_id,
        session_type,
        batch_academic_year,
        case when batch_completed_sessions = batch_expected_sessions then 'Yes' else 'No' end as is_batch_fully_completed
    FROM {{ ref('dev_int_global_session') }}
    where batch_academic_year >=  2025 --AND school_name = 'Bhagwan Mahavir Govt. High School, Honda' and session_type IN ( 'Student')
    group by school_id, batch_academic_year, session_type, case when batch_completed_sessions = batch_expected_sessions then 'Yes' else 'No' end
),

hm_session AS (
    SELECT
        hm_id,
        hm_session_name,
        hm_facilitator_name,
        hm_session_date,
        start_time,
        scheduling_type,
        rescheduled_counter,
        session_status,
        hm_attended,
        session_lead,
        session_academic_year,
        hm_school_id
    FROM {{ ref('dev_stg_hm_session') }}
),

joined_source AS (
    SELECT 
        igd.batch_language,
        igd.batch_academic_year,
        igd.school_name,
        igd.school_taluka,
        igd.school_ward,
        igd.school_district,
        igd.school_state,
        igd.batch_donor,
        igd.school_area,
        igd.school_partner,
        igd.school_id,
        igd.facilitator_name,
        igd.facilitator_email,
        s.fac_start_date,
        s.fac_end_date,
        s.total_student_present,
        s.total_parent_present,
        s.batch_expected_session,
        s.is_batch_fully_completed,
        s.session_type,
        s.session_date,
        hms.hm_id,
        hms.hm_session_name,
        hms.hm_facilitator_name,
        hms.hm_session_date,
        hms.start_time,
        hms.scheduling_type,
        hms.rescheduled_counter,
        hms.session_status,
        hms.hm_attended,
        hms.session_lead,
        hms.session_academic_year,
        hms.hm_school_id
    FROM hm_session hms
    INNER JOIN int_global_dcp igd 
        ON hms.hm_school_id = igd.school_id
    LEFT JOIN int_session s 
        ON hms.hm_school_id = s.school_id AND CAST(hms.session_academic_year AS INT64) = s.batch_academic_year
    WHERE igd.rn = 1
)

Select *
from joined_source





