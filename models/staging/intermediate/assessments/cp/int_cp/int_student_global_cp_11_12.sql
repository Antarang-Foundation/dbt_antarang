with dcp AS (

    SELECT
        student_id,
        student_barcode,
        gender,
        batch_no,
        batch_academic_year,
        batch_language,
        facilitator_id,
        facilitator_name,
        facilitator_email,
        school_id,
        school_name,
        school_taluka,
        school_ward,
        school_district,
        school_state,
        school_partner,
        school_area,
        donor_id,
        batch_donor,
        batch_grade
    FROM {{ ref('dev_int_global_dcp') }}
    WHERE SAFE_CAST(batch_academic_year AS INT64) >= 2026 and batch_grade in ('Grade 11','Grade 12')

),
cp as (select 
assessment_barcode,
record_type,
created_on,
cp_no,
q1_c1 as q4_1_1,
q1_c1_ans as q4_1_1_answer,
q1_c1_faci as q4_1_1_facilitator,
q1_c2 as q4_1_2,
q1_c2_ans as q4_1_2_answer,
q1_c2_faci as q4_1_2_facilitator,
b_q2_c1 as q4_2_1,
b_q2_c1_ans as q4_2_1_ans,
b_q2_c2 as q4_2_2,
b_q2_c2_ans as q4_2_2_ans,
b_q3_c1 as q4_3_1,
b_q3_c2 as q4_3_2,
b_q4_c1 as q4_4_1,
b_q4_c1_ans as q4_4_1_ans,
b_q4_c2 as q4_4_2,
b_q4_c2_ans as q4_4_2_ans,
b_q5_c1 as q4_5_1,
b_q5_c1_ans as q4_5_1_ans,
b_q5_c2 as q4_5_2,
b_q5_c2_ans as q4_5_2_ans,
b_q6_c1 as q4_6_1,
b_q6_c1_ans as q4_6_1_name,
b_q6_c2 as q4_6_2,
b_q6_c2_ans as q4_6_2_name,
b_q7_c1 as q4_7_1,
b_q7_c2 as q4_7_2,
q2 as q4_8,
q2_other as q4_8_reason,
q3 as q4_9,
q4 as q4_10,
q4_ans as q4_10_ans
FROM {{ ref('stg_cp') }}
WHERE SAFE_CAST(assessment_academic_year AS INT64) >= 2026
),

bl as (

    select *
    from cp
    where record_type = 'Baseline'

),

el as (

    select *
    from cp
    where record_type = 'Endline'

),

final as (
select

    dcp.student_id,
    dcp.student_barcode,
    dcp.gender,
    dcp.batch_no,
    dcp.batch_academic_year,
    dcp.batch_language,
    dcp.facilitator_id,
    dcp.facilitator_name,
    dcp.facilitator_email,
    dcp.school_id,
    dcp.school_name,
    dcp.school_taluka,
    dcp.school_ward,
    dcp.school_district,
    dcp.school_state,
    dcp.school_partner,
    dcp.school_area,
    dcp.donor_id,
    dcp.batch_donor,
    dcp.batch_grade,

    COALESCE(bl.assessment_barcode, el.assessment_barcode) AS assessment_barcode,

    ---------------- Baseline ----------------

    bl.created_on as bl_createddate,
    bl.cp_no as bl_cp2_no,

    bl.q4_1_1 as bl_q4_1_1,
    bl.q4_1_1_answer as bl_q4_1_1_answer,
    bl.q4_1_1_facilitator as bl_q4_1_1_facilitator,
    bl.q4_1_2 as bl_q4_1_2,
    bl.q4_1_2_answer as bl_q4_1_2_answer,
    bl.q4_1_2_facilitator as bl_q4_1_2_facilitator,
    bl.q4_2_1 as bl_q4_2_1,
    bl.q4_2_1_ans as bl_q4_2_1_ans,
    bl.q4_2_2 as bl_q4_2_2,
    bl.q4_2_2_ans as bl_q4_2_2_ans,
    bl.q4_3_1 as bl_q4_3_1,
    bl.q4_3_2 as bl_q4_3_2,
    bl.q4_4_1 as bl_q4_4_1,
    bl.q4_4_1_ans as bl_q4_4_1_ans,
    bl.q4_4_2 as bl_q4_4_2,
    bl.q4_4_2_ans as bl_q4_4_2_ans,
    bl.q4_5_1 as bl_q4_5_1,
    bl.q4_5_1_ans as bl_q4_5_1_ans,
    bl.q4_5_2 as bl_q4_5_2,
    bl.q4_5_2_ans as bl_q4_5_2_ans,
    bl.q4_6_1 as bl_q4_6_1,
    bl.q4_6_1_name as bl_q4_6_1_name,
    bl.q4_6_2 as bl_q4_6_2,
    bl.q4_6_2_name as bl_q4_6_2_name,
    bl.q4_7_1 as bl_q4_7_1,
    bl.q4_7_2 as bl_q4_7_2,
    bl.q4_8 as bl_q4_8,
    bl.q4_8_reason as bl_q4_8_reason,
    bl.q4_9 as bl_q4_9,
    bl.q4_10 as bl_q4_10,
    bl.q4_10_ans as bl_q4_10_ans,

    ---------------- Endline ----------------

    el.created_on as el_createddate,
    el.cp_no as el_cp2_no,

    el.q4_1_1 as el_q4_1_1,
    el.q4_1_1_answer as el_q4_1_1_answer,
    el.q4_1_1_facilitator as el_q4_1_1_facilitator,
    el.q4_1_2 as el_q4_1_2,
    el.q4_1_2_answer as el_q4_1_2_answer,
    el.q4_1_2_facilitator as el_q4_1_2_facilitator,
    el.q4_2_1 as el_q4_2_1,
    el.q4_2_1_ans as el_q4_2_1_ans,
    el.q4_2_2 as el_q4_2_2,
    el.q4_2_2_ans as el_q4_2_2_ans,
    el.q4_3_1 as el_q4_3_1,
    el.q4_3_2 as el_q4_3_2,
    el.q4_4_1 as el_q4_4_1,
    el.q4_4_1_ans as el_q4_4_1_ans,
    el.q4_4_2 as el_q4_4_2,
    el.q4_4_2_ans as el_q4_4_2_ans,
    el.q4_5_1 as el_q4_5_1,
    el.q4_5_1_ans as el_q4_5_1_ans,
    el.q4_5_2 as el_q4_5_2,
    el.q4_5_2_ans as el_q4_5_2_ans,
    el.q4_6_1 as el_q4_6_1,
    el.q4_6_1_name as el_q4_6_1_name,
    el.q4_6_2 as el_q4_6_2,
    el.q4_6_2_name as el_q4_6_2_name,
    el.q4_7_1 as el_q4_7_1,
    el.q4_7_2 as el_q4_7_2,
    el.q4_8 as el_q4_8,
    el.q4_8_reason as el_q4_8_reason,
    el.q4_9 as el_q4_9,
    el.q4_10 as el_q4_10,
    el.q4_10_ans as el_q4_10_ans

from dcp

left join bl
    on dcp.student_barcode = bl.assessment_barcode 

left join el
    on dcp.student_barcode = el.assessment_barcode 
)

select * from final


