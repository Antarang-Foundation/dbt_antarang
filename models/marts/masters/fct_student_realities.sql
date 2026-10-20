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
        where safe_cast(batch_academic_year as int64) >= 2026

    ),

    stg_sar as (

        select
            -- Student and batch details
            student.*,

            -- SAR assessment details
            sar.assessment_barcode,
            sar.created_on,
            sar.sar_no,

            -- Mapped assessment responses and marks
            sar.r4s4 as r1s1,
            sar.r4s4_marks as r1s1_marks,

            sar.r5f1 as r2s2,
            sar.r5f1_marks as r2s2_marks,

            sar.r8f4 as r3s3,
            sar.r8f4_marks as r3s3_marks,

            sar.r2s2 as r4s4,
            sar.r2s2_marks as r4s4_marks,

            sar.r7f3 as r5s5,
            sar.r7f3_marks as r5s5_marks,

            sar.r9 as r6s6,
            sar.r9_marks as r6s6_marks,

            sar.r10 as r7s7,
            sar.r10_marks as r7s7_marks,

            sar.r1s1 as r8s8,
            sar.r1s1_marks as r8s8_marks,

            sar.r11 as r9s9,
            sar.r11_marks as r9s9_marks,

            sar.r6f2 as r10s10,
            sar.r6f2_marks as r10s10_marks,

            sar.r12 as r11s11,
            sar.r12_marks as r11s11_marks,

            sar.r3s3 as r12s12,
            sar.r3s3_marks as r12s12_marks

        from {{ ref("stg_sar") }} as sar

        left join
            int_student_global as student
            on sar.assessment_barcode = student.student_barcode

        where
            safe_cast(sar.assessment_academic_year as int64) >= 2026
            and assessment_barcode is not null

    )

select *
from stg_sar
