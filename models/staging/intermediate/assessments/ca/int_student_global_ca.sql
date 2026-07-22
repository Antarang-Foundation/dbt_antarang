with
    int_student_global as (

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
        where batch_grade in ('Grade 9', 'Grade 10')

    ),

    stg_ca as (select * from {{ ref("stg_ca") }}),

    bl as (

        select *
        from
            (

                select
                    *,
                    row_number() over (
                        partition by assessment_student_id
                        order by created_on desc, ca1_id desc
                    ) as rn
                from stg_ca
                where record_type = 'Baseline'

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
                        partition by assessment_student_id
                        order by created_on desc, ca1_id desc
                    ) as rn
                from stg_ca
                where record_type = 'Endline'

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

            coalesce(
                bl.assessment_barcode, el.assessment_barcode
            ) as assessment_barcode,

            /* ================= BL ================= */
            bl.created_on as bl_createddate,
            bl.ca1_no as bl_ca1_no,
            bl.q2_a_marks as bl_q2_a_marks,
            bl.q2_b_marks as bl_q2_b_marks,

            case when regexp_contains(bl.q2_a, r'(^|,)A(,|$)') then 1 end as bl_2a_a,
            case when regexp_contains(bl.q2_a, r'(^|,)B(,|$)') then 1 end as bl_2a_b,
            case when regexp_contains(bl.q2_a, r'(^|,)C(,|$)') then 1 end as bl_2a_c,
            case when regexp_contains(bl.q2_a, r'(^|,)D(,|$)') then 1 end as bl_2a_d,
            case when regexp_contains(bl.q2_a, r'(^|,)E(,|$)') then 1 end as bl_2a_e,
            case when regexp_contains(bl.q2_a, r'(^|,)F(,|$)') then 1 end as bl_2a_f,
            case when regexp_contains(bl.q2_a, r'(^|,)G(,|$)') then 1 end as bl_2a_g,
            case when regexp_contains(bl.q2_a, r'(^|,)H(,|$)') then 1 end as bl_2a_h,

            case when regexp_contains(bl.q2_b, r'(^|,)A(,|$)') then 1 end as bl_2b_a,
            case when regexp_contains(bl.q2_b, r'(^|,)B(,|$)') then 1 end as bl_2b_b,
            case when regexp_contains(bl.q2_b, r'(^|,)C(,|$)') then 1 end as bl_2b_c,
            case when regexp_contains(bl.q2_b, r'(^|,)D(,|$)') then 1 end as bl_2b_d,
            case when regexp_contains(bl.q2_b, r'(^|,)E(,|$)') then 1 end as bl_2b_e,
            case when regexp_contains(bl.q2_b, r'(^|,)F(,|$)') then 1 end as bl_2b_f,
            case when regexp_contains(bl.q2_b, r'(^|,)G(,|$)') then 1 end as bl_2b_g,

            /* ================= EL ================= */
            el.created_on as el_createddate,
            el.ca1_no as el_ca1_no,
            el.q2_a_marks as el_q2_a_marks,
            el.q2_b_marks as el_q2_b_marks,

            case when regexp_contains(el.q2_a, r'(^|,)A(,|$)') then 1 end as el_q2_a_a,
            case when regexp_contains(el.q2_a, r'(^|,)B(,|$)') then 1 end as el_q2_a_b,
            case when regexp_contains(el.q2_a, r'(^|,)C(,|$)') then 1 end as el_q2_a_c,
            case when regexp_contains(el.q2_a, r'(^|,)D(,|$)') then 1 end as el_q2_a_d,
            case when regexp_contains(el.q2_a, r'(^|,)E(,|$)') then 1 end as el_q2_a_e,
            case when regexp_contains(el.q2_a, r'(^|,)F(,|$)') then 1 end as el_q2_a_f,
            case when regexp_contains(el.q2_a, r'(^|,)G(,|$)') then 1 end as el_q2_a_g,
            case when regexp_contains(el.q2_a, r'(^|,)H(,|$)') then 1 end as el_q2_a_h,

            case when regexp_contains(el.q2_b, r'(^|,)A(,|$)') then 1 end as el_q2_b_a,
            case when regexp_contains(el.q2_b, r'(^|,)B(,|$)') then 1 end as el_q2_b_b,
            case when regexp_contains(el.q2_b, r'(^|,)C(,|$)') then 1 end as el_q2_b_c,
            case when regexp_contains(el.q2_b, r'(^|,)D(,|$)') then 1 end as el_q2_b_d,
            case when regexp_contains(el.q2_b, r'(^|,)E(,|$)') then 1 end as el_q2_b_e,
            case when regexp_contains(el.q2_b, r'(^|,)F(,|$)') then 1 end as el_q2_b_f,
            case when regexp_contains(el.q2_b, r'(^|,)G(,|$)') then 1 end as el_q2_b_g

        from int_student_global s

        left join bl on bl.assessment_barcode = s.student_barcode

        left join el on el.assessment_barcode = s.student_barcode

        where
            s.batch_academic_year >= 2026
            and (bl.assessment_barcode is not null or el.assessment_barcode is not null)
    )

select *
from final
