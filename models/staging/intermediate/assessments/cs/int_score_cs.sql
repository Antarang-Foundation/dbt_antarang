with
    base as (
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
            bl_cs_no,
            bl_q4_1,
            bl_q4_1_marks,
            bl_q4_2,
            bl_q4_2_marks,
            bl_q4_3,
            bl_q4_3_marks,
            bl_q4_4,
            bl_q4_4_marks,
            bl_q4_5,
            bl_q4_5_marks,
            bl_q4_6,
            bl_q4_6_marks,
            bl_q4_7,
            bl_q4_7_marks,
            bl_q4_8,
            bl_q4_8_marks,
            bl_q4_9,
            bl_q4_9_marks,
            bl_q4_10,
            bl_q4_10_marks,
            bl_q4_marks,
            bl_q5_1,
            bl_q5_1_marks,
            bl_q5_2,
            bl_q5_2_marks,
            bl_q5_3,
            bl_q5_3_marks,
            bl_q5_4,
            bl_q5_4_marks,
            bl_q5_5,
            bl_q5_5_marks,
            bl_q5_6,
            bl_q5_6_marks,
            bl_q5_7,
            bl_q5_7_marks,
            bl_q5_8,
            bl_q5_8_marks,
            bl_q5_9,
            bl_q5_9_marks,
            bl_q5_marks,
            el_createddate,
            el_cs_no,
            el_q4_1,
            el_q4_1_marks,
            el_q4_2,
            el_q4_2_marks,
            el_q4_3,
            el_q4_3_marks,
            el_q4_4,
            el_q4_4_marks,
            el_q4_5,
            el_q4_5_marks,
            el_q4_6,
            el_q4_6_marks,
            el_q4_7,
            el_q4_7_marks,
            el_q4_8,
            el_q4_8_marks,
            el_q4_9,
            el_q4_9_marks,
            el_q4_10,
            el_q4_10_marks,
            el_q4_marks,
            el_q5_1,
            el_q5_1_marks,
            el_q5_2,
            el_q5_2_marks,
            el_q5_3,
            el_q5_3_marks,
            el_q5_4,
            el_q5_4_marks,
            el_q5_5,
            el_q5_5_marks,
            el_q5_6,
            el_q5_6_marks,
            el_q5_7,
            el_q5_7_marks,
            el_q5_8,
            el_q5_8_marks,
            el_q5_9,
            el_q5_9_marks,
            el_q5_marks
        from {{ ref("int_student_global_cs") }}
    ),

    bl_1 as (
        select
            base.*,
            -- BL Q4 total
            case
                when
                    bl_q4_1_marks is null
                    and bl_q4_2_marks is null
                    and bl_q4_3_marks is null
                    and bl_q4_4_marks is null
                    and bl_q4_5_marks is null
                    and bl_q4_6_marks is null
                    and bl_q4_7_marks is null
                    and bl_q4_8_marks is null
                    and bl_q4_9_marks is null
                    and bl_q4_10_marks is null
                then null
                else
                    (
                        ifnull(safe_cast(bl_q4_1_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_2_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_3_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_4_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_5_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_6_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_7_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_8_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_9_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_10_marks as int64), 0)
                    )
            end as bl_q4_total_marks,

            case
                when
                    bl_q4_2_marks is null
                    and bl_q4_8_marks is null
                    and bl_q4_9_marks is null
                then null
                else
                    safe_divide(
                        ifnull(safe_cast(bl_q4_2_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_8_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_9_marks as int64), 0),
                        3
                    )
            end as bl_direct_learning,

            case
                when
                    bl_q4_1_marks is null
                    and bl_q4_7_marks is null
                    and bl_q4_10_marks is null
                then null
                else
                    safe_divide(
                        ifnull(safe_cast(bl_q4_1_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_7_marks as int64), 0)
                        + ifnull(safe_cast(bl_q4_10_marks as int64), 0),
                        3
                    )
            end as bl_observational_learning,

            case
                when bl_q4_5_marks is null
                then null
                else safe_cast(bl_q4_5_marks as int64)
            end as bl_self_assessment_based_learning,

            case
                when
                    bl_q4_1_marks is null
                    and bl_q4_2_marks is null
                    and bl_q4_5_marks is null
                    and bl_q4_7_marks is null
                    and bl_q4_8_marks is null
                    and bl_q4_9_marks is null
                    and bl_q4_10_marks is null
                then null
                else
                    (
                        safe_divide(
                            ifnull(safe_cast(bl_q4_2_marks as int64), 0)
                            + ifnull(safe_cast(bl_q4_8_marks as int64), 0)
                            + ifnull(safe_cast(bl_q4_9_marks as int64), 0),
                            3
                        )
                        * 0.5
                    ) + (
                        safe_divide(
                            ifnull(safe_cast(bl_q4_1_marks as int64), 0)
                            + ifnull(safe_cast(bl_q4_7_marks as int64), 0)
                            + ifnull(safe_cast(bl_q4_10_marks as int64), 0),
                            3
                        )
                        * 0.3
                    )
                    + (ifnull(safe_cast(bl_q4_5_marks as int64), 0) * 0.2)
            end as bl_q4_overall_score
        from base
    ),
    bl as (
        select
            bl_1.*,
            case
                when bl_q4_total_marks = 0
                then '0 Experiential Opportunities'
                when bl_q4_total_marks between 1 and 2
                then '1-2 Experiential Opportunities'
                when bl_q4_total_marks between 3 and 4
                then '3-4 Experiential Opportunities'
                when bl_q4_total_marks between 5 and 7
                then '5-7 Experiential Opportunities'
                else 'DNA'
            end as bl_q4_marks_bucket,

            case
                when bl_q4_overall_score between 0 and 0.20
                then 'Pre-Exposure Stage'
                when bl_q4_overall_score > 0.20 and bl_q4_overall_score <= 0.40
                then 'Observational Learning Stage'
                when bl_q4_overall_score > 0.40 and bl_q4_overall_score <= 0.60
                then 'Self-Appraisal / Early Exploration'
                when bl_q4_overall_score > 0.60 and bl_q4_overall_score <= 0.80
                then 'Developing Self-Efficacy'
                when bl_q4_overall_score > 0.80
                then 'Goal-Directed Action Stage'
                else null
            end as bl_q4_bucket

        from bl_1
    ),

    el_1 as (
        select
            bl.*,

            case
                when
                    el_q4_1_marks is null
                    and el_q4_2_marks is null
                    and el_q4_3_marks is null
                    and el_q4_4_marks is null
                    and el_q4_5_marks is null
                    and el_q4_6_marks is null
                    and el_q4_7_marks is null
                    and el_q4_8_marks is null
                    and el_q4_9_marks is null
                    and el_q4_10_marks is null
                then null
                else
                    (
                        ifnull(safe_cast(el_q4_1_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_2_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_3_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_4_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_5_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_6_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_7_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_8_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_9_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_10_marks as int64), 0)
                    )
            end as el_q4_total_marks,

            case
                when
                    el_q4_2_marks is null
                    and el_q4_8_marks is null
                    and el_q4_9_marks is null
                then null
                else
                    safe_divide(
                        ifnull(safe_cast(el_q4_2_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_8_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_9_marks as int64), 0),
                        3
                    )
            end as el_direct_learning,

            case
                when
                    el_q4_1_marks is null
                    and el_q4_7_marks is null
                    and el_q4_10_marks is null
                then null
                else
                    safe_divide(
                        ifnull(safe_cast(el_q4_1_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_7_marks as int64), 0)
                        + ifnull(safe_cast(el_q4_10_marks as int64), 0),
                        3
                    )
            end as el_observational_learning,

            case
                when el_q4_5_marks is null
                then null
                else safe_cast(el_q4_5_marks as int64)
            end as el_self_assessment_based_learning,

            case
                when
                    el_q4_1_marks is null
                    and el_q4_2_marks is null
                    and el_q4_5_marks is null
                    and el_q4_7_marks is null
                    and el_q4_8_marks is null
                    and el_q4_9_marks is null
                    and el_q4_10_marks is null
                then null
                else
                    (
                        safe_divide(
                            ifnull(safe_cast(el_q4_2_marks as int64), 0)
                            + ifnull(safe_cast(el_q4_8_marks as int64), 0)
                            + ifnull(safe_cast(el_q4_9_marks as int64), 0),
                            3
                        )
                        * 0.5
                    ) + (
                        safe_divide(
                            ifnull(safe_cast(el_q4_1_marks as int64), 0)
                            + ifnull(safe_cast(el_q4_7_marks as int64), 0)
                            + ifnull(safe_cast(el_q4_10_marks as int64), 0),
                            3
                        )
                        * 0.3
                    )
                    + (ifnull(safe_cast(el_q4_5_marks as int64), 0) * 0.2)
            end as el_q4_overall_score

        from bl
    ),

    el as (
        select
            el_1.*,

            case
                when el_q4_total_marks = 0
                then '0 Experiential Opportunities'
                when el_q4_total_marks between 1 and 2
                then '1-2 Experiential Opportunities'
                when el_q4_total_marks between 3 and 4
                then '3-4 Experiential Opportunities'
                when el_q4_total_marks between 5 and 7
                then '5-7 Experiential Opportunities'
                else 'DNA'
            end as el_q4_marks_bucket,

            case
                when el_q4_overall_score between 0 and 0.20
                then 'Pre-Exposure Stage'
                when el_q4_overall_score > 0.20 and el_q4_overall_score <= 0.40
                then 'Observational Learning Stage'
                when el_q4_overall_score > 0.40 and el_q4_overall_score <= 0.60
                then 'Self-Appraisal / Early Exploration'
                when el_q4_overall_score > 0.60 and el_q4_overall_score <= 0.80
                then 'Developing Self-Efficacy'
                when el_q4_overall_score > 0.80
                then 'Goal-Directed Action Stage'
                else null
            end as el_q4_bucket

        from el_1
    )

select *
from el
