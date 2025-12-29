select *
    from {{ ref('dev_int_global_session') }}
    where batch_academic_year >= 2023


