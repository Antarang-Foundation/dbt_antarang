with
    baseline as (

        select *
        from
            (

                select
                    assessment_barcode,
                    created_on,
                    cdm2_no,

                    q6_2 as q6_5,
                    q6_3 as q6_6,
                    q6_6 as q6_7,
                    q6_7 as q6_2,
                    q6_8 as q6_3,
                    q6_9 as q6_8,
                    q6_10 as q6_10,
                    q6_11 as q6_1,
                    q6_12 as q6_9,
                    q6_13 as q6_4,

                    q6_2_marks as q6_5_marks,
                    q6_3_marks as q6_6_marks,
                    q6_6_marks as q6_7_marks,
                    q6_7_marks as q6_2_marks,
                    q6_8_marks as q6_3_marks,
                    q6_9_marks as q6_8_marks,
                    q6_10_marks as q6_10_marks,
                    q6_11_marks as q6_1_marks,
                    q6_12_marks as q6_9_marks,
                    q6_13_marks as q6_4_marks,
                    row_number() over (
                        partition by assessment_barcode
                        order by created_on desc, cdm2_id desc
                    ) rn

                from {{ ref("stg_cdm2") }}
                where
                    lower(record_type) = 'baseline'
                    and safe_cast(assessment_academic_year as int64) >= 2026

            )
        where rn = 1

    ),

    endline as (

        select *
        from
            (

                select
                    assessment_barcode,
                    created_on,
                    cdm2_no,

                    q6_2 as q6_5,
                    q6_3 as q6_6,
                    q6_6 as q6_7,
                    q6_7 as q6_2,
                    q6_8 as q6_3,
                    q6_9 as q6_8,
                    q6_10 as q6_10,
                    q6_11 as q6_1,
                    q6_12 as q6_9,
                    q6_13 as q6_4,

                    q6_2_marks as q6_5_marks,
                    q6_3_marks as q6_6_marks,
                    q6_6_marks as q6_7_marks,
                    q6_7_marks as q6_2_marks,
                    q6_8_marks as q6_3_marks,
                    q6_9_marks as q6_8_marks,
                    q6_10_marks as q6_10_marks,
                    q6_11_marks as q6_1_marks,
                    q6_12_marks as q6_9_marks,
                    q6_13_marks as q6_4_marks,
                    row_number() over (
                        partition by assessment_barcode
                        order by created_on desc, cdm2_id desc
                    ) rn

                from {{ ref("stg_cdm2") }}
                where
                    lower(record_type) = 'endline'
                    and safe_cast(assessment_academic_year as int64) >= 2026

            )
        where rn = 1

    ),

    dcp as (

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
            batch_grade
        from {{ ref("dev_int_global_dcp") }}
        where safe_cast(batch_academic_year as int64) >= 2026

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

            coalesce(
                baseline.assessment_barcode, endline.assessment_barcode
            ) as assessment_barcode,

            baseline.created_on as bl_createddate,
            baseline.cdm2_no as bl_ca2_no,

            baseline.q6_1 as bl_q3_1,
            baseline.q6_2 as bl_q3_2,
            baseline.q6_3 as bl_q3_3,
            baseline.q6_4 as bl_q3_4,
            baseline.q6_5 as bl_q3_5,
            baseline.q6_6 as bl_q3_6,
            baseline.q6_7 as bl_q3_7,
            baseline.q6_8 as bl_q3_8,
            baseline.q6_9 as bl_q3_9,
            baseline.q6_10 as bl_q3_10,

            baseline.q6_1_marks as bl_q3_1_marks,
            baseline.q6_2_marks as bl_q3_2_marks,
            baseline.q6_3_marks as bl_q3_3_marks,
            baseline.q6_4_marks as bl_q3_4_marks,
            baseline.q6_5_marks as bl_q3_5_marks,
            baseline.q6_6_marks as bl_q3_6_marks,
            baseline.q6_7_marks as bl_q3_7_marks,
            baseline.q6_8_marks as bl_q3_8_marks,
            baseline.q6_9_marks as bl_q3_9_marks,
            baseline.q6_10_marks as bl_q3_10_marks,

            endline.created_on as el_createddate,
            endline.cdm2_no as el_ca2_no,

            endline.q6_1 as el_q3_1,
            endline.q6_2 as el_q3_2,
            endline.q6_3 as el_q3_3,
            endline.q6_4 as el_q3_4,
            endline.q6_5 as el_q3_5,
            endline.q6_6 as el_q3_6,
            endline.q6_7 as el_q3_7,
            endline.q6_8 as el_q3_8,
            endline.q6_9 as el_q3_9,
            endline.q6_10 as el_q3_10,

            endline.q6_1_marks as el_q3_1_marks,
            endline.q6_2_marks as el_q3_2_marks,
            endline.q6_3_marks as el_q3_3_marks,
            endline.q6_4_marks as el_q3_4_marks,
            endline.q6_5_marks as el_q3_5_marks,
            endline.q6_6_marks as el_q3_6_marks,
            endline.q6_7_marks as el_q3_7_marks,
            endline.q6_8_marks as el_q3_8_marks,
            endline.q6_9_marks as el_q3_9_marks,
            endline.q6_10_marks as el_q3_10_marks
        from dcp

        left join baseline on baseline.assessment_barcode = dcp.student_barcode

        left join endline on endline.assessment_barcode = dcp.student_barcode
        where
            student_id is not null
            and (
                baseline.assessment_barcode is not null
                or endline.assessment_barcode is not null
            )
    )

select *
from final
