with
    t1 as (
        select distinct
            school_name,
            school_district,
            school_area,
            school_taluka,
            school_partner,
            case
                when count(session_date is null or total_student_present is null) > 0
                then 'Not Complete'
                else 'Completed'
            end as is_batch_completed
        from {{ ref('fct_global_session') }}
        where batch_academic_year >= 2024
        group by
            school_name, school_district, school_area, school_taluka, school_partner
    )

select * from t1 where school_name is not null
