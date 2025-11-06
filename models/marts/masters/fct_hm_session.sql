WITH ranked_sessions AS (
    SELECT
        hm_school_id,
        hm_session_name,
        facilitator_name,
        hm_session_date,
        start_time,
        scheduling_type,
        rescheduled_counter,
        session_status,
        hm_attended,
        session_lead,
        session_academic_year,
        batch_language,
        fac_start_date,
        fac_end_date,
        school_name,
        school_taluka,
        school_district,
        school_state,
        school_area,
        school_partner,
        session_type,
        ROW_NUMBER() OVER (
            PARTITION BY hm_school_id, hm_session_name
            ORDER BY hm_session_date DESC
        ) AS rn
    FROM {{ ref('dev_int_hm_session') }}
)

SELECT 
    hm_school_id,
    hm_session_name,
    facilitator_name,
    hm_session_date,
    start_time,
    scheduling_type,
    rescheduled_counter,
    session_status,
    hm_attended,
    session_lead,
    session_academic_year,
    batch_language,
    fac_start_date,
    fac_end_date,
    school_name,
    school_taluka,
    school_district,
    school_state,
    school_area,
    school_partner,
    session_type
FROM ranked_sessions
WHERE rn = 1 

