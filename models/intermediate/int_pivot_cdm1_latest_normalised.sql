with source as (
    select 
        student_barcode,
        q1_baseline,
        q2_baseline,
        q3_baseline,
        q4_baseline as q4_b,
        q1_endline,
        q2_endline,
        q3_endline,
        q4_endline as q4_e,
        total_cdm1_baseline,
        total_cdm1_endline,
    from {{ ref('int_pivot_cdm1_latest') }}
),

normalised as (
    select 
    student_barcode,
    q1_baseline,
    q2_baseline,
    q3_baseline,
    (q4_b/4) as q4_baseline,
    (q1_baseline + q2_baseline + q3_baseline + (q4_b/4)) as total_cdm1_baseline,
    q1_endline,
    q2_endline,
    q3_endline,
    (q4_e/4) as q4_endline,
    (q1_endline + q2_endline + q3_endline + (q4_e/4)) as total_cdm1_endline
from source
)
select 
    *
from normalised