with
    source as (
        select
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
            batch_grade,
            school_district,
            school_state
        from {{ ref("dev_int_global_dcp") }}
    ),
    mapping as (
        select
            source.*,
            case
                when batch_grade = "Grade 9"
                then "Grade 10"
                when batch_grade = "Grade 10"
                then "Grade 11"
                when batch_grade = "Grade 11"
                then "Grade 12"
                when batch_grade = "Grade 12"
                then "Passed Grade 12"
            end as current_grade,

            case
                when first_barcode != student_barcode then 'Linked' else 'Not Linked'
            end as linking_status
        from source
    )

select *
from mapping
