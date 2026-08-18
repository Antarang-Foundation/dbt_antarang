with base as (

    select
        facilitator_name,
        facilitator_email,
        school_name,
        batch_academic_year,
        batch_no,
        batch_grade,
        batch_donor,
        fac_start_date,
        fac_end_date,
        session_date,
        session_name,
        session_code,
        school_state,
        school_district,
        school_partner,
        school_taluka,
        school_area,
        total_student_present,

        case
            when session_date is not null
             and total_student_present is not null
            then session_code
        end as session_completion

    from {{ ref("int_global_session") }}

),

summary as (

    select
        facilitator_name,
        facilitator_email,
        school_name,
        batch_academic_year,
        batch_no,
        batch_grade,
        fac_start_date,
        fac_end_date,
        school_state,
        school_district,
        school_taluka,
        school_area,
        school_partner,
        batch_donor,
        

        count(distinct session_code) as total_sessions,
        count(distinct session_completion) as completed_sessions

    from base

    group by
        facilitator_name,
        facilitator_email,
        school_name,
        batch_academic_year,
        batch_no,
        batch_grade,
        fac_start_date,
        fac_end_date,
        school_state,
        school_district,
        school_taluka,
        school_area,
        school_partner,
        batch_donor

),

final as (

    select
        school_state,
        school_district,
        school_taluka,
        school_area,
        school_partner,
        school_name,
        batch_no,
        batch_grade,
        facilitator_name,
        facilitator_email,
        batch_donor,
        batch_academic_year,
        fac_start_date,
        fac_end_date,
        total_sessions,
        completed_sessions,
        total_sessions - completed_sessions as pending_sessions,

        -- Predicted Date TAT
        case
            when fac_start_date is null then
                current_date("Asia/Kolkata")
                + (
                    (total_sessions - completed_sessions)
                    * case
                        when school_state = "Rajasthan" then 15
                        else 7
                      end
                  )
            else
                date(fac_start_date)
                + (
                    (total_sessions - completed_sessions)
                    * case
                        when school_state = "Rajasthan" then 15
                        else 7
                      end
                  )
        end as predicted_date_tat

    from summary

),

prediction as (select
    *,
    concat(
        case
            when extract(month from predicted_date_tat) = 1 then "Jan"
            when extract(month from predicted_date_tat) = 2 then "Feb"
            when extract(month from predicted_date_tat) = 3 then "Mar"
            when extract(month from predicted_date_tat) = 4 then "Apr"
            when extract(month from predicted_date_tat) = 5 then "May"
            when extract(month from predicted_date_tat) = 6 then "Jun"
            when extract(month from predicted_date_tat) = 7 then "Jul"
            when extract(month from predicted_date_tat) = 8 then "Aug"
            when extract(month from predicted_date_tat) = 9 then "Sep"
            when extract(month from predicted_date_tat) = 10 then "Oct"
            when extract(month from predicted_date_tat) = 11 then "Nov"
            else "Dec"
        end,
        " Week ",
        cast(floor((extract(day from predicted_date_tat) - 1) / 7) + 1 as string)
    ) as predicted_days_tat

from final
where batch_academic_year is not null
)

select * from prediction