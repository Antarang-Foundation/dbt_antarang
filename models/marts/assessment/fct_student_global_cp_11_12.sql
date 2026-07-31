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
            bl_cp2_no,
            bl_q4_1_1,
            bl_q4_1_1_answer,
            bl_q4_1_1_facilitator,
            bl_q4_1_2,
            bl_q4_1_2_answer,
            bl_q4_1_2_facilitator,
            bl_q4_2_1,
            bl_q4_2_1_ans,
            bl_q4_2_2,
            bl_q4_2_2_ans,
            bl_q4_3_1,
            bl_q4_3_2,
            bl_q4_4_1,
            bl_q4_4_1_ans,
            bl_q4_4_2,
            bl_q4_4_2_ans,
            bl_q4_5_1,
            bl_q4_5_1_ans,
            bl_q4_5_2,
            bl_q4_5_2_ans,
            bl_q4_6_1,
            bl_q4_6_1_name,
            bl_q4_6_2,
            bl_q4_6_2_name,
            bl_q4_7_1,
            bl_q4_7_2,
            bl_q4_8,
            bl_q4_8_reason,
            bl_q4_9,
            bl_q4_10,
            bl_q4_10_ans,
            el_createddate,
            el_cp2_no,
            el_q4_1_1,
            el_q4_1_1_answer,
            el_q4_1_1_facilitator,
            el_q4_1_2,
            el_q4_1_2_answer,
            el_q4_1_2_facilitator,
            el_q4_2_1,
            el_q4_2_1_ans,
            el_q4_2_2,
            el_q4_2_2_ans,
            el_q4_3_1,
            el_q4_3_2,
            el_q4_4_1,
            el_q4_4_1_ans,
            el_q4_4_2,
            el_q4_4_2_ans,
            el_q4_5_1,
            el_q4_5_1_ans,
            el_q4_5_2,
            el_q4_5_2_ans,
            el_q4_6_1,
            el_q4_6_1_name,
            el_q4_6_2,
            el_q4_6_2_name,
            el_q4_7_1,
            el_q4_7_2,
            el_q4_8,
            el_q4_8_reason,
            el_q4_9,
            el_q4_10,
            el_q4_10_ans
        from {{ ref("int_student_global_cp_11_12") }}
    ),
    bl_marks as (

        select
            source.*,
            case when trim(bl_q4_1_1_answer) = 'A' then 1 else 0 end as bl_q4_1_1_marks,

            case when trim(bl_q4_1_2_answer) = 'A' then 1 else 0 end as bl_q4_1_2_marks,
            case when trim(bl_q4_2_1_ans) = 'A' then 1 else 0 end as bl_q4_2_1_marks,
            case when trim(bl_q4_2_2_ans) = 'A' then 1 else 0 end as bl_q4_2_2_marks,
            case when trim(bl_q4_4_1) = 'A' then 1 else 0 end as bl_q4_4_1_marks,
            case when trim(bl_q4_4_2) = 'A' then 1 else 0 end as bl_q4_4_2_marks,
            case when trim(bl_q4_5_1) = 'A' then 1 else 0 end as bl_q4_5_1_marks,
            case when trim(bl_q4_5_2) = 'A' then 1 else 0 end as bl_q4_5_2_marks,
            case
                when coalesce(trim(bl_q4_6_1), '') = '' then 0 else 1
            end as bl_q4_6_1_marks,
            case
                when coalesce(trim(bl_q4_6_2), '') = '' then 0 else 1
            end as bl_q4_6_2_marks,
            case
                when coalesce(trim(bl_q4_8), '') = '' then 0 else 1
            end as bl_q4_8_marks,
            case
                when coalesce(trim(bl_q4_9), '') = '' then 0 else 1
            end as bl_q4_9_marks,
            case
                when bl_q4_3_1 is null or trim(bl_q4_3_1) = ''
                then 0
                when bl_q4_3_1 = 'I'
                then 0
                else 1
            end as bl_q4_3_1_marks,
            case
                when bl_q4_3_2 is null or trim(bl_q4_3_2) = ''
                then 0
                when bl_q4_3_2 = 'I'
                then 0
                else 1
            end as bl_q4_3_2_marks,
            case
                when bl_q4_7_1 is null or trim(bl_q4_7_1) = ''
                then 0
                when bl_q4_7_1 = 'I'
                then 0
                else 1
            end as bl_q4_7_1_marks,
            case
                when bl_q4_7_2 is null or trim(bl_q4_7_2) = ''
                then 0
                when bl_q4_7_2 = 'I'
                then 0
                else 1
            end as bl_q4_7_2_marks
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
                    and bl_q4_6_1_marks is null
                    and bl_q4_7_1_marks is null
                    and bl_q4_8_marks is null
                    and bl_q4_9_marks is null
                then null
                else
                    coalesce(bl_q4_1_1_marks, 0)
                    + coalesce(bl_q4_2_1_marks, 0)
                    + coalesce(bl_q4_3_1_marks, 0)
                    + coalesce(bl_q4_4_1_marks, 0)
                    + coalesce(bl_q4_5_1_marks, 0)
                    + coalesce(bl_q4_6_1_marks, 0)
                    + coalesce(bl_q4_7_1_marks, 0)
                    + coalesce(bl_q4_8_marks, 0)
                    + coalesce(bl_q4_9_marks, 0)
            end as bl_q4_total_marks_cp1,

            case
                when
                    bl_q4_1_2_marks is null
                    and bl_q4_2_2_marks is null
                    and bl_q4_3_2_marks is null
                    and bl_q4_4_2_marks is null
                    and bl_q4_5_2_marks is null
                    and bl_q4_6_2_marks is null
                    and bl_q4_7_2_marks is null
                    and bl_q4_8_marks is null
                    and bl_q4_9_marks is null
                then null
                else
                    coalesce(bl_q4_1_2_marks, 0)
                    + coalesce(bl_q4_2_2_marks, 0)
                    + coalesce(bl_q4_3_2_marks, 0)
                    + coalesce(bl_q4_4_2_marks, 0)
                    + coalesce(bl_q4_5_2_marks, 0)
                    + coalesce(bl_q4_6_2_marks, 0)
                    + coalesce(bl_q4_7_2_marks, 0)
                    + coalesce(bl_q4_8_marks, 0)
                    + coalesce(bl_q4_9_marks, 0)
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
            end as bl_cri_z,

            case
                when bl_q4_total_marks_cp1 is null
                then null
                when
                    safe_divide(
                        bl_q4_total_marks_cp1 - avg(bl_q4_total_marks_cp1) over (),
                        stddev(bl_q4_total_marks_cp1) over ()
                    )
                    < -1
                then 'Low Readiness'
                when
                    safe_divide(
                        bl_q4_total_marks_cp1 - avg(bl_q4_total_marks_cp1) over (),
                        stddev(bl_q4_total_marks_cp1) over ()
                    )
                    > 1
                then 'High Readiness'
                else 'Moderate Readiness'
            end as bl_cri_level

        from bl_total

    ),

    el_marks as (

        select
            bl_cri.*,

            case when trim(el_q4_1_1_answer) = 'A' then 1 else 0 end as el_q4_1_1_marks,
            case when trim(el_q4_1_2_answer) = 'A' then 1 else 0 end as el_q4_1_2_marks,
            case when trim(el_q4_2_1_ans) = 'A' then 1 else 0 end as el_q4_2_1_marks,
            case when trim(el_q4_2_2_ans) = 'A' then 1 else 0 end as el_q4_2_2_marks,
            case when trim(el_q4_4_1) = 'A' then 1 else 0 end as el_q4_4_1_marks,
            case when trim(el_q4_4_2) = 'A' then 1 else 0 end as el_q4_4_2_marks,
            case when trim(el_q4_5_1) = 'A' then 1 else 0 end as el_q4_5_1_marks,
            case when trim(el_q4_5_2) = 'A' then 1 else 0 end as el_q4_5_2_marks,

            case
                when coalesce(trim(el_q4_6_1), '') = '' then 0 else 1
            end as el_q4_6_1_marks,
            case
                when coalesce(trim(el_q4_6_2), '') = '' then 0 else 1
            end as el_q4_6_2_marks,
            case
                when coalesce(trim(el_q4_8), '') = '' then 0 else 1
            end as el_q4_8_marks,
            case
                when coalesce(trim(el_q4_9), '') = '' then 0 else 1
            end as el_q4_9_marks,

            case
                when el_q4_3_1 is null or trim(el_q4_3_1) = ''
                then 0
                when el_q4_3_1 = 'I'
                then 0
                else 1
            end as el_q4_3_1_marks,

            case
                when el_q4_3_2 is null or trim(el_q4_3_2) = ''
                then 0
                when el_q4_3_2 = 'I'
                then 0
                else 1
            end as el_q4_3_2_marks,

            case
                when el_q4_7_1 is null or trim(el_q4_7_1) = ''
                then 0
                when el_q4_7_1 = 'I'
                then 0
                else 1
            end as el_q4_7_1_marks,

            case
                when el_q4_7_2 is null or trim(el_q4_7_2) = ''
                then 0
                when el_q4_7_2 = 'I'
                then 0
                else 1
            end as el_q4_7_2_marks

        from bl_cri

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
                    and el_q4_6_1_marks is null
                    and el_q4_7_1_marks is null
                    and el_q4_8_marks is null
                    and el_q4_9_marks is null
                then null
                else
                    coalesce(el_q4_1_1_marks, 0)
                    + coalesce(el_q4_2_1_marks, 0)
                    + coalesce(el_q4_3_1_marks, 0)
                    + coalesce(el_q4_4_1_marks, 0)
                    + coalesce(el_q4_5_1_marks, 0)
                    + coalesce(el_q4_6_1_marks, 0)
                    + coalesce(el_q4_7_1_marks, 0)
                    + coalesce(el_q4_8_marks, 0)
                    + coalesce(el_q4_9_marks, 0)
            end as el_q4_total_marks_cp1,

            case
                when
                    el_q4_1_2_marks is null
                    and el_q4_2_2_marks is null
                    and el_q4_3_2_marks is null
                    and el_q4_4_2_marks is null
                    and el_q4_5_2_marks is null
                    and el_q4_6_2_marks is null
                    and el_q4_7_2_marks is null
                    and el_q4_8_marks is null
                    and el_q4_9_marks is null
                then null
                else
                    coalesce(el_q4_1_2_marks, 0)
                    + coalesce(el_q4_2_2_marks, 0)
                    + coalesce(el_q4_3_2_marks, 0)
                    + coalesce(el_q4_4_2_marks, 0)
                    + coalesce(el_q4_5_2_marks, 0)
                    + coalesce(el_q4_6_2_marks, 0)
                    + coalesce(el_q4_7_2_marks, 0)
                    + coalesce(el_q4_8_marks, 0)
                    + coalesce(el_q4_9_marks, 0)
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
            end as el_cri_z,

            case
                when el_q4_total_marks_cp1 is null
                then null
                when
                    safe_divide(
                        el_q4_total_marks_cp1 - avg(el_q4_total_marks_cp1) over (),
                        stddev(el_q4_total_marks_cp1) over ()
                    )
                    < -1
                then 'Low Readiness'
                when
                    safe_divide(
                        el_q4_total_marks_cp1 - avg(el_q4_total_marks_cp1) over (),
                        stddev(el_q4_total_marks_cp1) over ()
                    )
                    > 1
                then 'High Readiness'
                else 'Moderate Readiness'
            end as el_cri_level

        from el_total

    ),

    comparison as (

        select
            el_cri.*,

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

        from el_cri

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
            bl_cp2_no,
            bl_q4_1_1_answer,
            bl_q4_1_1_facilitator,
            bl_q4_1_1_marks,
            bl_q4_1_2_answer,
            bl_q4_1_2_facilitator,
            bl_q4_1_2_marks,
            bl_q4_2_1_ans,
            bl_q4_2_1_marks,
            bl_q4_2_2_ans,
            bl_q4_2_2_marks,
            bl_q4_3_1,
            bl_q4_3_1_marks,
            bl_q4_3_2,
            bl_q4_3_2_marks,
            bl_q4_4_1,
            bl_q4_4_1_marks,
            bl_q4_4_2,
            bl_q4_4_2_marks,
            bl_q4_5_1,
            bl_q4_5_1_marks,
            bl_q4_5_2,
            bl_q4_5_2_marks,
            bl_q4_6_1,
            bl_q4_6_1_marks,
            bl_q4_6_2,
            bl_q4_6_2_marks,
            bl_q4_7_1,
            bl_q4_7_1_marks,
            bl_q4_7_2,
            bl_q4_7_2_marks,
            bl_q4_8,
            bl_q4_8_marks,
            bl_q4_9,
            bl_q4_9_marks,
            bl_q4_10,
            bl_q4_total_marks_cp1,
            bl_q4_total_marks_cp2,
            bl_cri_z,
            bl_cri_level,
            el_createddate,
            el_cp2_no,
            el_q4_1_1_answer,
            el_q4_1_1_facilitator,
            el_q4_1_1_marks,
            el_q4_1_2_answer,
            el_q4_1_2_facilitator,
            el_q4_1_2_marks,
            el_q4_2_1_ans,
            el_q4_2_1_marks,
            el_q4_2_2_ans,
            el_q4_2_2_marks,
            el_q4_3_1,
            el_q4_3_1_marks,
            el_q4_3_2,
            el_q4_3_2_marks,
            el_q4_4_1,
            el_q4_4_1_marks,
            el_q4_4_2,
            el_q4_4_2_marks,
            el_q4_5_1,
            el_q4_5_1_marks,
            el_q4_5_2,
            el_q4_5_2_marks,
            el_q4_6_1,
            el_q4_6_1_marks,
            el_q4_6_2,
            el_q4_6_2_marks,
            el_q4_7_1,
            el_q4_7_1_marks,
            el_q4_7_2,
            el_q4_7_2_marks,
            el_q4_8,
            el_q4_8_marks,
            el_q4_9,
            el_q4_9_marks,
            el_q4_10,
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
