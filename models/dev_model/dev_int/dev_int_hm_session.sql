WITH int_global_dcp AS (
    SELECT 
        batch_language,
        school_name,
        school_taluka,
        school_district,
        school_state,
        batch_donor,
        school_area,
        school_partner,
        school_id,
        facilitator_name,
        ROW_NUMBER() OVER (
            PARTITION BY school_id 
            ORDER BY 
                CASE WHEN facilitator_name IS NOT NULL THEN 0 ELSE 1 END, 
                school_name DESC
        ) AS rn
    FROM {{ ref('dev_int_global_dcp') }}
),

int_session AS (
    SELECT 
        MIN(CASE WHEN fac_start_date IS NOT NULL THEN fac_start_date END) AS fac_start_date,
        MAX(CASE WHEN fac_end_date IS NOT NULL THEN fac_end_date END) AS fac_end_date, 
        MAX(CASE WHEN batch_expected_sessions IS NOT NULL THEN batch_expected_sessions END) AS batch_expected_sessions,
        MAX(CASE WHEN total_student_present IS NOT NULL THEN total_student_present END) AS total_student_present,
        MAX(CASE WHEN total_parent_present IS NOT NULL THEN total_parent_present END) AS total_parent_present,
        school_id,
        session_type 
    FROM {{ ref('dev_int_global_session') }}
    where batch_academic_year >=  2025 and session_type IN ('Parent', 'Student')
    group by school_id, batch_academic_year, session_type
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
        igd.school_name,
        igd.school_taluka,
        igd.school_district,
        igd.school_state,
        igd.batch_donor,
        igd.school_area,
        igd.school_partner,
        igd.school_id,
        igd.facilitator_name,
        s.fac_start_date,
        s.fac_end_date,
        s.total_student_present,
        s.total_parent_present,
        s.batch_expected_sessions,
        s.session_type,
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
        ON hms.hm_school_id = s.school_id
    WHERE igd.rn = 1
)

Select * from joined_source

