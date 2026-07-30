with
    source as (
        select
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
            batch_grade,
            assessment_barcode,
            bl_createddate,
            bl_cp1_no,
            bl_q4_1_1,
            bl_q4_1_1_answer,
            bl_q4_1_1_facilitator,
            bl_q4_1_2,
            bl_q4_1_2_answer,
            bl_q4_1_2_facilitator,
            bl_q4_2_1,
            bl_q4_2_2,
            bl_q4_3_1,
            bl_q4_3_1_ans,
            bl_q4_3_2,
            bl_q4_3_2_ans,
            bl_q4_4_1,
            bl_q4_4_1_ans,
            bl_q4_4_2,
            bl_q4_4_2_ans,
            bl_q4_5_1,
            bl_q4_5_2,
            bl_q4_6_1,
            bl_q4_6_1_ans,
            bl_q4_6_2,
            bl_q4_6_2_ans,
            bl_q4_7_1,
            bl_q4_7_1_ans,
            bl_q4_7_2,
            bl_q4_7_2_ans,
            bl_q4_8_1,
            bl_q4_8_1_name,
            bl_q4_8_2,
            bl_q4_8_2_name,
            bl_q4_9,
            bl_q4_9_reason,
            bl_q4_10,
            bl_q4_11,
            bl_q4_11_ans,
            el_createddate,
            el_cp1_no,
            el_q4_1_1,
            el_q4_1_1_answer,
            el_q4_1_1_facilitator,
            el_q4_1_2,
            el_q4_1_2_answer,
            el_q4_1_2_facilitator,
            el_q4_2_1,
            el_q4_2_2,
            el_q4_3_1,
            el_q4_3_1_ans,
            el_q4_3_2,
            el_q4_3_2_ans,
            el_q4_4_1,
            el_q4_4_1_ans,
            el_q4_4_2,
            el_q4_4_2_ans,
            el_q4_5_1,
            el_q4_5_2,
            el_q4_6_1,
            el_q4_6_1_ans,
            el_q4_6_2,
            el_q4_6_2_ans,
            el_q4_7_1,
            el_q4_7_1_ans,
            el_q4_7_2,
            el_q4_7_2_ans,
            el_q4_8_1,
            el_q4_8_1_name,
            el_q4_8_2,
            el_q4_8_2_name,
            el_q4_9,
            el_q4_9_reason,
            el_q4_10,
            el_q4_11,
            el_q4_11_ans
        from {{ ref("int_student_global_cp_9_10") }}
    ),

    bl_marks as (

        select
            source.*,
            case when trim(bl_q4_1_1_answer) = 'A' then 1 else 0 end as bl_q4_1_1_marks,

            case when trim(bl_q4_1_2_answer) = 'A' then 1 else 0 end as bl_q4_1_2_marks,

            case
                when bl_q4_2_1 is null or trim(bl_q4_2_1) = ''
                then 0
                when bl_q4_2_1 = 'M'
                then 0
                else 1
            end as bl_q4_2_1_marks,

            case
                when bl_q4_2_2 is null or trim(bl_q4_2_2) = ''
                then 0
                when bl_q4_2_2 = 'M'
                then 0
                else 1
            end as bl_q4_2_2_marks,

            case when trim(bl_q4_3_1_ans) = 'A' then 1 else 0 end as bl_q4_3_1_marks,

            case when trim(bl_q4_3_2_ans) = 'A' then 1 else 0 end as bl_q4_3_2_marks,

            case when trim(bl_q4_4_1_ans) = 'A' then 1 else 0 end as bl_q4_4_1_marks,

            case when trim(bl_q4_4_2_ans) = 'A' then 1 else 0 end as bl_q4_4_2_marks,

            case
                when bl_q4_5_1 is null or trim(bl_q4_5_1) = ''
                then 0
                when bl_q4_5_1 = 'I'
                then 0
                else 1
            end as bl_q4_5_1_marks,

            case
                when bl_q4_5_2 is null or trim(bl_q4_5_2) = ''
                then 0
                when bl_q4_5_2 = 'I'
                then 0
                else 1
            end as bl_q4_5_2_marks,

            case when trim(bl_q4_7_1_ans) = 'A' then 1 else 0 end as bl_q4_7_1_marks,

            case when trim(bl_q4_7_2_ans) = 'A' then 1 else 0 end as bl_q4_7_2_marks,

            case
                when coalesce(trim(bl_q4_8_1_name), '') = '' then 0 else 1
            end as bl_q4_8_1_marks,

            case
                when coalesce(trim(bl_q4_8_2_name), '') = '' then 0 else 1
            end as bl_q4_8_2_marks,
            case
                when bl_q4_9 is null or trim(bl_q4_9) = ''
                then null
                else
                    if(regexp_contains(bl_q4_9, r'(^|,\s*)A($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)B($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)C($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)D($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)E($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)F($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)G($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)H($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)I($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)J($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)K($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)L($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)M($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)N($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)O($|,\s*)'), 1, 0)
                    + if(regexp_contains(bl_q4_9, r'(^|,\s*)P($|,\s*)'), 0.5, 0)
            end as bl_q4_9_marks,
            case when trim(bl_q4_10) = 'A' then 1 else 0 end as bl_q4_10_marks
        from source

    ),

    bl_total as (

        select
            bl_marks.*,
            case
                when
                    bl_q4_1_1_marks is null
                    and bl_q4_2_1_marks is null
                    and bl_q4_3_1_marks is null
                    and bl_q4_4_1_marks is null
                    and bl_q4_5_1_marks is null
                    and bl_q4_7_1_marks is null
                    and bl_q4_8_1_marks is null
                    and bl_q4_9_marks is null
                    and bl_q4_10_marks is null
                then null

                else
                    coalesce(bl_q4_1_1_marks, 0)
                    + coalesce(bl_q4_2_1_marks, 0)
                    + coalesce(bl_q4_3_1_marks, 0)
                    + coalesce(bl_q4_4_1_marks, 0)
                    + coalesce(bl_q4_5_1_marks, 0)
                    + coalesce(bl_q4_7_1_marks, 0)
                    + coalesce(bl_q4_8_1_marks, 0)
                    + coalesce(bl_q4_9_marks, 0)
                    + coalesce(bl_q4_10_marks, 0)
            end as bl_q4_total_marks_cp1,

            case
                when
                    bl_q4_1_2_marks is null
                    and bl_q4_2_2_marks is null
                    and bl_q4_3_2_marks is null
                    and bl_q4_4_2_marks is null
                    and bl_q4_5_2_marks is null
                    and bl_q4_7_2_marks is null
                    and bl_q4_8_2_marks is null
                    and bl_q4_9_marks is null
                    and bl_q4_10_marks is null
                then null

                else
                    coalesce(bl_q4_1_2_marks, 0)
                    + coalesce(bl_q4_2_2_marks, 0)
                    + coalesce(bl_q4_3_2_marks, 0)
                    + coalesce(bl_q4_4_2_marks, 0)
                    + coalesce(bl_q4_5_2_marks, 0)
                    + coalesce(bl_q4_7_2_marks, 0)
                    + coalesce(bl_q4_8_2_marks, 0)
                    + coalesce(bl_q4_9_marks, 0)
                    + coalesce(bl_q4_10_marks, 0)
            end as bl_q4_total_marks_cp2

        from bl_marks
    ),

    bl_cri as (

        select
            bl_total.*,

            case
                when bl_q4_total_marks_cp1 is null
                then null
                else
                    safe_divide(
                        bl_q4_total_marks_cp1 - avg(bl_q4_total_marks_cp1) over (),
                        stddev(bl_q4_total_marks_cp1) over ()
                    )
            end as bl_cri_z

        from bl_total

    ),

    bl_final as (
        select
            *,

            case
                when bl_cri_z is null
                then null
                when bl_cri_z < -1
                then 'Low Readiness'
                when bl_cri_z > 1
                then 'High Readiness'
                else 'Moderate Readiness'
            end as bl_cri_level

        from bl_cri
    ),

    el_marks as (
        select
            bl_final.*,

            case when trim(el_q4_1_1_answer) = 'A' then 1 else 0 end as el_q4_1_1_marks,

            case when trim(el_q4_1_2_answer) = 'A' then 1 else 0 end as el_q4_1_2_marks,

            case
                when el_q4_2_1 is null or trim(el_q4_2_1) = ''
                then 0
                when el_q4_2_1 = 'M'
                then 0
                else 1
            end as el_q4_2_1_marks,

            case
                when el_q4_2_2 is null or trim(el_q4_2_2) = ''
                then 0
                when el_q4_2_2 = 'M'
                then 0
                else 1
            end as el_q4_2_2_marks,

            case when trim(el_q4_3_1_ans) = 'A' then 1 else 0 end as el_q4_3_1_marks,

            case when trim(el_q4_3_2_ans) = 'A' then 1 else 0 end as el_q4_3_2_marks,

            case when trim(el_q4_4_1_ans) = 'A' then 1 else 0 end as el_q4_4_1_marks,

            case when trim(el_q4_4_2_ans) = 'A' then 1 else 0 end as el_q4_4_2_marks,

            case
                when el_q4_5_1 is null or trim(el_q4_5_1) = ''
                then 0
                when el_q4_5_1 = 'I'
                then 0
                else 1
            end as el_q4_5_1_marks,

            case
                when el_q4_5_2 is null or trim(el_q4_5_2) = ''
                then 0
                when el_q4_5_2 = 'I'
                then 0
                else 1
            end as el_q4_5_2_marks,

            case when trim(el_q4_7_1_ans) = 'A' then 1 else 0 end as el_q4_7_1_marks,

            case when trim(el_q4_7_2_ans) = 'A' then 1 else 0 end as el_q4_7_2_marks,

            case
                when coalesce(trim(el_q4_8_1_name), '') = '' then 0 else 1
            end as el_q4_8_1_marks,

            case
                when coalesce(trim(el_q4_8_2_name), '') = '' then 0 else 1
            end as el_q4_8_2_marks,

            case
                when el_q4_9 is null or trim(el_q4_9) = ''
                then null
                else
                    if(regexp_contains(el_q4_9, r'(^|,\s*)A($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)B($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)C($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)D($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)E($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)F($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)G($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)H($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)I($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)J($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)K($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)L($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)M($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)N($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)O($|,\s*)'), 1, 0)
                    + if(regexp_contains(el_q4_9, r'(^|,\s*)P($|,\s*)'), 0.5, 0)
            end as el_q4_9_marks,

            case when trim(el_q4_10) = 'A' then 1 else 0 end as el_q4_10_marks

        from bl_final
    ),

    el_total as (

        select
            el_marks.*,

            case
                when
                    el_q4_1_1_marks is null
                    and el_q4_2_1_marks is null
                    and el_q4_3_1_marks is null
                    and el_q4_4_1_marks is null
                    and el_q4_5_1_marks is null
                    and el_q4_7_1_marks is null
                    and el_q4_8_1_marks is null
                    and el_q4_9_marks is null
                    and el_q4_10_marks is null
                then null

                else
                    coalesce(el_q4_1_1_marks, 0)
                    + coalesce(el_q4_2_1_marks, 0)
                    + coalesce(el_q4_3_1_marks, 0)
                    + coalesce(el_q4_4_1_marks, 0)
                    + coalesce(el_q4_5_1_marks, 0)
                    + coalesce(el_q4_7_1_marks, 0)
                    + coalesce(el_q4_8_1_marks, 0)
                    + coalesce(el_q4_9_marks, 0)
                    + coalesce(el_q4_10_marks, 0)
            end as el_q4_total_marks_cp1,

            case
                when
                    el_q4_1_2_marks is null
                    and el_q4_2_2_marks is null
                    and el_q4_3_2_marks is null
                    and el_q4_4_2_marks is null
                    and el_q4_5_2_marks is null
                    and el_q4_7_2_marks is null
                    and el_q4_8_2_marks is null
                    and el_q4_9_marks is null
                    and el_q4_10_marks is null
                then null

                else
                    coalesce(el_q4_1_2_marks, 0)
                    + coalesce(el_q4_2_2_marks, 0)
                    + coalesce(el_q4_3_2_marks, 0)
                    + coalesce(el_q4_4_2_marks, 0)
                    + coalesce(el_q4_5_2_marks, 0)
                    + coalesce(el_q4_7_2_marks, 0)
                    + coalesce(el_q4_8_2_marks, 0)
                    + coalesce(el_q4_9_marks, 0)
                    + coalesce(el_q4_10_marks, 0)
            end as el_q4_total_marks_cp2

        from el_marks

    ),

    el_cri as (

        select
            el_total.*,

            case
                when el_q4_total_marks_cp1 is null
                then null
                else
                    safe_divide(
                        el_q4_total_marks_cp1 - avg(el_q4_total_marks_cp1) over (),
                        stddev(el_q4_total_marks_cp1) over ()
                    )
            end as el_cri_z

        from el_total

    ),

    el_final as (

        select
            *,

            case
                when el_cri_z is null
                then null
                when el_cri_z < -1
                then 'Low Readiness'
                when el_cri_z > 1
                then 'High Readiness'
                else 'Moderate Readiness'
            end as el_cri_level

        from el_cri

    ),

    comparison as (

        select
            el_final.*,

            case
                when bl_q4_total_marks_cp1 is null or el_q4_total_marks_cp1 is null
                then null

                when (el_q4_total_marks_cp1 - bl_q4_total_marks_cp1) > 0
                then 'Improvement'

                when (el_q4_total_marks_cp1 - bl_q4_total_marks_cp1) < 0
                then 'Area for Growth'

                else 'No Change'
            end as bl_el_q4_marks,

            case
                when bl_cri_z is null or el_cri_z is null
                then null

                when (el_cri_z - bl_cri_z) > 0
                then 'Improvement'

                when (el_cri_z - bl_cri_z) < 0
                then 'Area for Growth'

                else 'No Change'
            end as bl_el_cri_z

        from el_final

    ),

    final as (
        select
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
            batch_grade,
            assessment_barcode,
            bl_createddate,
            bl_cp1_no,
            bl_q4_1_1_answer,
            bl_q4_1_1_facilitator,
            bl_q4_1_1_marks,
            bl_q4_1_2_answer,
            bl_q4_1_2_facilitator,
            bl_q4_1_2_marks,
            bl_q4_2_1,
            bl_q4_2_1_marks,
            bl_q4_2_2,
            bl_q4_2_2_marks,
            bl_q4_3_1_ans,
            bl_q4_3_1_marks,
            bl_q4_3_2_ans,
            bl_q4_3_2_marks,
            bl_q4_4_1_ans,
            bl_q4_4_1_marks,
            bl_q4_4_2_ans,
            bl_q4_4_2_marks,
            bl_q4_5_1,
            bl_q4_5_1_marks,
            bl_q4_5_2,
            bl_q4_5_2_marks,
            bl_q4_6_1_ans,
            bl_q4_6_2_ans,
            bl_q4_7_1_ans,
            bl_q4_7_1_marks,
            bl_q4_7_2_ans,
            bl_q4_7_2_marks,
            bl_q4_8_1,
            bl_q4_8_1_marks,
            bl_q4_8_2,
            bl_q4_8_2_marks,
            bl_q4_9,
            bl_q4_9_marks,
            bl_q4_10,
            bl_q4_10_marks,
            bl_q4_11_ans,
            bl_q4_total_marks_cp1,
            bl_q4_total_marks_cp2,
            bl_cri_z,
            bl_cri_level,
            el_createddate,
            el_cp1_no,
            el_q4_1_1_answer,
            el_q4_1_1_facilitator,
            el_q4_1_1_marks,
            el_q4_1_2_answer,
            el_q4_1_2_facilitator,
            el_q4_1_2_marks,
            el_q4_2_1,
            el_q4_2_1_marks,
            el_q4_2_2,
            el_q4_2_2_marks,
            el_q4_3_1_ans,
            el_q4_3_1_marks,
            el_q4_3_2_ans,
            el_q4_3_2_marks,
            el_q4_4_1_ans,
            el_q4_4_1_marks,
            el_q4_4_2_ans,
            el_q4_4_2_marks,
            el_q4_5_1,
            el_q4_5_1_marks,
            el_q4_5_2,
            el_q4_5_2_marks,
            el_q4_6_1_ans,
            el_q4_6_2_ans,
            el_q4_7_1_ans,
            el_q4_7_1_marks,
            el_q4_7_2_ans,
            el_q4_7_2_marks,
            el_q4_8_1,
            el_q4_8_1_marks,
            el_q4_8_2,
            el_q4_8_2_marks,
            el_q4_9,
            el_q4_9_marks,
            el_q4_10,
            el_q4_10_marks,
            el_q4_11_ans,
            el_q4_total_marks_cp1,
            el_q4_total_marks_cp2,
            el_cri_z,
            el_cri_level,
            bl_el_q4_marks,
            bl_el_cri_z
        from comparison
    )

select *
from final
