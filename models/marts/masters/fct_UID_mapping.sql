with source as (

    select
        school_state,
        school_district,
        school_taluka,
        school_ward,
        school_partner,
        batch_academic_year,
        first_barcode,
        student_barcode,
        g9_barcode,
        g10_barcode,
        g11_barcode,
        g12_barcode,
        student_name,
        gender,
        birth_year,
        school_name,
        batch_no,
        batch_grade
    from {{ ref("dev_int_global_dcp") }}
    where first_barcode is not null

),

mapping as (

    select
        source.*,

        case
            when batch_grade = "Grade 9" then "Grade 10"
            when batch_grade = "Grade 10" then "Grade 11"
            when batch_grade = "Grade 11" then "Grade 12"
            when batch_grade = "Grade 12" then "Passed Grade 12"
        end as current_grade,

        case
            when first_barcode != student_barcode
                or (
                    g9_barcode is not null
                    and g9_barcode != student_barcode
                )
                or (
                    g10_barcode is not null
                    and g10_barcode != student_barcode
                )
                or (
                    g11_barcode is not null
                    and g11_barcode != student_barcode
                )
                or (
                    g12_barcode is not null
                    and g12_barcode != student_barcode
                )
            then 'Linked'
            else 'Not Linked'
        end as linking_status

    from source

),

/* 
Get facilitator information from the current/latest year.
This is similar to the b table in the other query.
*/
current_facilitator as (

    select distinct
        school_name,
        batch_grade,
        batch_academic_year,
        facilitator_name as current_facilitator_name,
        facilitator_email
    from {{ ref("dev_int_global_dcp") }}
    where batch_academic_year = 2026
      and school_name is not null

),

final_mapping as (

    select
        m.*,

        cf.current_facilitator_name,
        cf.facilitator_email

    from mapping m

    left join current_facilitator cf
        on m.school_name = cf.school_name
        and m.current_grade = cf.batch_grade

),

final as (

    select
        *,
        
        first_value(current_facilitator_name ignore nulls) over (
            partition by school_name
            order by batch_academic_year desc
            rows between unbounded preceding and unbounded following
        ) as latest_facilitator_name,

        first_value(facilitator_email ignore nulls) over (
            partition by school_name
            order by batch_academic_year desc
            rows between unbounded preceding and unbounded following
        ) as latest_facilitator_email

    from final_mapping

)

select *
from final