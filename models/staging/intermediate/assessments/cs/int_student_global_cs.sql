with
    assessments as (

        select
            cs_id,
            assessment_barcode,
            record_type,
            created_on,
            cs_no,
            assessment_grade,
            assessment_academic_year,
            assessment_batch_id,

            q11_1 as q11_2,
            q11_2 as q11_3,
            q11_3 as q11_4,
            q11_4 as q11_5,
            q11_5 as q11_6,
            q11_6 as q11_7,
            q11_7 as q11_8,
            q11_8 as q11_9,
            q11_9 as q11_10,
            q11_10 as q11_1,
            q11_marks,

            q15_1,
            q15_2,
            q15_3,
            q15_4,
            q15_5,
            q15_6,
            q15_7,
            q15_8,
            q15_9,
            q15_marks

        from {{ ref("stg_cs") }}

        where
            data_cleanup = true
            and marks_recalculated = true
            and student_linked = true
            and safe_cast(assessment_academic_year as int64) >= 2026

    ),

    students as (

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

    bl as (

        select *
        from
            (

                select
                    *,
                    row_number() over (
                        partition by assessment_barcode order by created_on desc
                    ) as rn

                from assessments

                where lower(record_type) = 'baseline'

            )

        where rn = 1

    ),

    el as (

        select *
        from
            (

                select
                    *,
                    row_number() over (
                        partition by assessment_barcode order by created_on desc
                    ) as rn

                from assessments

                where lower(record_type) = 'endline'

            )

        where rn = 1

    ),

    final as (
        select

            s.student_id,
            s.student_barcode,
            s.gender,

            s.batch_no,
            s.batch_academic_year,
            s.batch_language,

            s.facilitator_id,
            s.facilitator_name,
            s.facilitator_email,

            s.school_id,
            s.school_name,
            s.school_taluka,
            s.school_ward,
            s.school_district,
            s.school_state,

            s.school_partner,
            s.school_area,

            s.donor_id,
            s.batch_donor,
            s.batch_grade,

            -- ------------------------------------------------
            -- BASELINE
            -- ------------------------------------------------
            coalesce(
                bl.assessment_barcode, el.assessment_barcode
            ) as assessment_barcode,
            bl.created_on as bl_createddate,
            bl.cs_no as bl_cs_no,

            bl.q11_1 as bl_q4_1,
            bl.q11_2 as bl_q4_2,
            bl.q11_3 as bl_q4_3,
            bl.q11_4 as bl_q4_4,
            bl.q11_5 as bl_q4_5,
            bl.q11_6 as bl_q4_6,
            bl.q11_7 as bl_q4_7,
            bl.q11_8 as bl_q4_8,
            bl.q11_9 as bl_q4_9,
            bl.q11_10 as bl_q4_10,
            bl.q11_marks as bl_q4_marks,

            bl.q15_1 as bl_q5_1,
            bl.q15_2 as bl_q5_2,
            bl.q15_3 as bl_q5_3,
            bl.q15_4 as bl_q5_4,
            bl.q15_5 as bl_q5_5,
            bl.q15_6 as bl_q5_6,
            bl.q15_7 as bl_q5_7,
            bl.q15_8 as bl_q5_8,
            bl.q15_9 as bl_q5_9,
            bl.q15_marks as bl_q5_marks,

            -- ------------------------------------------------
            -- ENDLINE
            -- ------------------------------------------------
            el.created_on as el_createddate,
            el.cs_no as el_cs_no,

            el.q11_1 as el_q4_1,
            el.q11_2 as el_q4_2,
            el.q11_3 as el_q4_3,
            el.q11_4 as el_q4_4,
            el.q11_5 as el_q4_5,
            el.q11_6 as el_q4_6,
            el.q11_7 as el_q4_7,
            el.q11_8 as el_q4_8,
            el.q11_9 as el_q4_9,
            el.q11_10 as el_q4_10,
            el.q11_marks as el_q4_marks,

            el.q15_1 as el_q5_1,
            el.q15_2 as el_q5_2,
            el.q15_3 as el_q5_3,
            el.q15_4 as el_q5_4,
            el.q15_5 as el_q5_5,
            el.q15_6 as el_q5_6,
            el.q15_7 as el_q5_7,
            el.q15_8 as el_q5_8,
            el.q15_9 as el_q5_9,
            el.q15_marks as el_q5_marks

        from students s

        left join bl on bl.assessment_barcode = s.student_barcode

        left join el on el.assessment_barcode = s.student_barcode
        where
            student_id is not null
            and (bl.assessment_barcode is not null or el.assessment_barcode is not null)
    )

select *
from final
