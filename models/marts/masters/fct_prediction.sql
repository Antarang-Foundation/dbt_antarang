with
    base as (

        select
            facilitator_name,
            school_name,
            school_district,
            school_state,
            batch_academic_year,
            batch_no,
            batch_grade,
            batch_donor,
            school_partner,
            school_area,
            school_taluka,
            fac_start_date,
            fac_end_date,
            session_date,
            session_name,
            session_code,
            total_student_present,

            case
                when session_date is not null and total_student_present is not null
                then session_code
            end as session_completion

        from {{ ref("int_global_session") }}

    ),

    summary as (

        select
            facilitator_name,
            school_name,
            school_district,
            school_state,
            batch_academic_year,
            batch_no,
            batch_grade,
            batch_donor,
            school_partner,
            school_area,
            school_taluka,
            fac_start_date,
            fac_end_date,

            count(distinct session_code) as total_sessions,
            count(distinct session_completion) as completed_sessions

        from base

        group by
            facilitator_name,
            school_name,
            school_district,
            school_state,
            batch_academic_year,
            batch_no,
            batch_grade,
            batch_donor,
            school_partner,
            school_area,
            school_taluka,
            fac_start_date,
            fac_end_date

    ),

    final as (
        select
            facilitator_name,
            school_name,
            school_district,
            school_state,
            batch_academic_year,
            batch_no,
            batch_grade,
            batch_donor,
            school_partner,
            school_area,
            school_taluka,
            fac_start_date,
            fac_end_date,
            total_sessions,
            completed_sessions,
            total_sessions - completed_sessions as pending_sessions,

            case
                when fac_start_date is null
                then
                    current_date("Asia/Kolkata")
                    + ((total_sessions - completed_sessions) * 7)
                else date(fac_start_date) + ((total_sessions - completed_sessions) * 7)
            end as five_days_tat

        from summary
    )

select *
from final
