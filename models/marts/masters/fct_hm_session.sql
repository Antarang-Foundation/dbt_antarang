with int_global_dcp as (
    select distinct
        batch_academic_year, 
        batch_language, 
        school_name, 
        school_taluka, 
        school_district, 
        school_state, 
        batch_donor, 
        school_area,
        school_partner,
        school_id,
        facilitator_name
    from {{ ref('dev_int_global_dcp') }}
),

hm_session as (
    select 
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
    from {{ ref('dev_stg_hm_session') }}
),

joined_source as (
    select 
        igd.*,
        hms.*
    from hm_session hms
    inner join int_global_dcp igd 
        on hms.hm_school_id = igd.school_id
)

select 
    hm_session_name, facilitator_name, hm_session_date, start_time, scheduling_type, rescheduled_counter, session_status, 
    hm_attended, session_lead, batch_academic_year, batch_language, school_name, school_taluka, school_district, school_state, 
    batch_donor, school_area, school_partner  from joined_source

    
    
