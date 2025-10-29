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
    school_partner
FROM {{ ref('dev_int_hm_session') }}

