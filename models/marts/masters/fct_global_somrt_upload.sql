with
    t1 as (select * from {{ ref("int_global") }}),

    t2 as (
        select
            somrt_id,
            somrt_no,
            somrt_session_id,
            somrt_batch_id,
            omr_type,
            omr_assessment_object,
            omr_assessment_count,
            omr_assessment_record_type,
            somrt_batch_no,
            omr_received_count,
            omr_received_by,
            number_of_students_in_batch,
            omr_received_date,
            first_omr_upload_date
        from {{ ref("stg_somrt") }}
    ),

    t3 as (
        select
            session_id,
            session_facilitator_id,
            omr_required,
            omrs_received,
            total_student_present,
            total_parent_present,
            attendance_count,
            session_name,
            session_date,
            present_count
        from {{ ref("stg_session") }}
    ),

    t4 as (select * from t2 left join t3 on t2.somrt_session_id = t3.session_id),

    t5 as (select * from t1 left join t4 on t1.batch_id = t4.somrt_batch_id),

    t6 as (
        select
            batch_id,
            sum(distinct no_of_students_facilitated) as total_students,
            count(distinct bl_cdm1_no) as bl_cdm1,
            count(distinct el_cdm1_no) as el_cdm1,
            count(distinct bl_cdm2_no) as bl_cdm2,
            count(distinct el_cdm2_no) as el_cdm2,
            count(distinct bl_cp_no) as bl_cp,
            count(distinct el_cp_no) as el_cp,
            count(distinct bl_cs_no) as bl_cs,
            count(distinct el_cs_no) as el_cs,
            count(distinct bl_fp_no) as bl_fp,
            count(distinct el_fp_no) as el_fp,
            count(distinct saf_no) as saf,
            count(distinct sar_no) as sar,
            count(distinct sd2_barcode) as total_sd2_students
        from {{ ref("int_student_global_assessment_status") }}
        group by batch_id
    ),

    diagnostics as (
        select
            batch_no,
            count(distinct bl_sd_no) as bl_sd,
            count(distinct el_sd_no) as el_sd
        from {{ ref("int_student_diagnostic") }}
        group by batch_no
    ),

    cdm3 as (
        select
            batch_no,
            count(distinct bl_ca2_no) as bl_ca2,
            count(distinct el_ca2_no) as el_ca2
        from {{ ref("int_student_global_cdm2_ind") }}
        group by batch_no
    ),

    t7 as (
        select
            t5.*,

            case
                when t5.omr_type in ('Self Awareness + Feedback', 'Quiz + Feedback')
                then t6.saf

                when t5.omr_type = 'Realities + Quiz'
                then t6.sar

                when t5.omr_type = 'Career Decision Making- 1A'
                then t6.bl_cdm1
                when t5.omr_type = 'Career Decision Making- 1B'
                then t6.el_cdm1

                when t5.omr_type = 'Career Decision Making- 2A'
                then t6.bl_cdm2
                when t5.omr_type = 'Career Decision Making- 2B'
                then t6.el_cdm2

                when t5.omr_type = 'Career Planning A'
                then t6.bl_cp
                when t5.omr_type = 'Career Planning B'
                then t6.el_cp

                when t5.omr_type = 'Career Skills A'
                then t6.bl_cs
                when t5.omr_type = 'Career Skills B'
                then t6.el_cs

                when t5.omr_type = 'Planning for Future A'
                then t6.bl_fp
                when t5.omr_type = 'Planning for Future B'
                then t6.el_fp

                when t5.omr_type = 'Student Details'
                then t6.total_students

                when t5.omr_type = 'Career Decision Making- 3A'
                then cdm3.bl_ca2
                when t5.omr_type = 'Career Decision Making- 3B'
                then cdm3.el_ca2

                when t5.omr_type in ('Student Diagnostics A', 'Student Diagnositcs A')
                then diagnostics.bl_sd

                when t5.omr_type = 'Student Diagnositcs B'
                then diagnostics.el_sd

                when t5.omr_type = 'Student Details 2'
                then t6.total_sd2_students
            end as omr_upload_count

        from t5
        inner join t6 on t5.batch_id = t6.batch_id
        left join diagnostics on t5.batch_no = diagnostics.batch_no
        left join cdm3 on t5.batch_no = cdm3.batch_no
    ),

    t8 as (
        select
            t7.*,

            -- TAT1: Session to OMR received
            case
                when session_date is not null and total_student_present is not null
                then
                    case
                        when
                            coalesce(omr_received_date, current_date('Asia/Kolkata'))
                            >= session_date
                        then
                            (
                                select countif(extract(dayofweek from d) not in (1, 7))
                                from
                                    unnest(
                                        generate_date_array(
                                            date_add(session_date, interval 1 day),
                                            coalesce(
                                                omr_received_date,
                                                current_date('Asia/Kolkata')
                                            )
                                        )
                                    ) as d
                            )
                        else
                            - (
                                select countif(extract(dayofweek from d) not in (1, 7))
                                from
                                    unnest(
                                        generate_date_array(
                                            date_add(
                                                coalesce(
                                                    omr_received_date,
                                                    current_date('Asia/Kolkata')
                                                ),
                                                interval 1 day
                                            ),
                                            session_date
                                        )
                                    ) as d
                            )
                    end
            end as tat1,

            -- TAT2: OMR received to first upload
            case
                when omr_received_date is not null and total_student_present is not null
                then
                    case
                        when
                            coalesce(
                                first_omr_upload_date, current_date('Asia/Kolkata')
                            )
                            >= omr_received_date
                        then
                            (
                                select countif(extract(dayofweek from d) not in (1, 7))
                                from
                                    unnest(
                                        generate_date_array(
                                            date_add(omr_received_date, interval 1 day),
                                            coalesce(
                                                first_omr_upload_date,
                                                current_date('Asia/Kolkata')
                                            )
                                        )
                                    ) as d
                            )
                        else
                            - (
                                select countif(extract(dayofweek from d) not in (1, 7))
                                from
                                    unnest(
                                        generate_date_array(
                                            date_add(
                                                coalesce(
                                                    first_omr_upload_date,
                                                    current_date('Asia/Kolkata')
                                                ),
                                                interval 1 day
                                            ),
                                            omr_received_date
                                        )
                                    ) as d
                            )
                    end
            end as tat2,

            -- TAT3: Session to first upload
            case
                when session_date is not null and total_student_present is not null
                then
                    case
                        when
                            coalesce(
                                first_omr_upload_date, current_date('Asia/Kolkata')
                            )
                            >= session_date
                        then
                            (
                                select countif(extract(dayofweek from d) not in (1, 7))
                                from
                                    unnest(
                                        generate_date_array(
                                            date_add(session_date, interval 1 day),
                                            coalesce(
                                                first_omr_upload_date,
                                                current_date('Asia/Kolkata')
                                            )
                                        )
                                    ) as d
                            )
                        else
                            - (
                                select countif(extract(dayofweek from d) not in (1, 7))
                                from
                                    unnest(
                                        generate_date_array(
                                            date_add(
                                                coalesce(
                                                    first_omr_upload_date,
                                                    current_date('Asia/Kolkata')
                                                ),
                                                interval 1 day
                                            ),
                                            session_date
                                        )
                                    ) as d
                            )
                    end
            end as tat3

        from t7
    ),

    -- Aggregate student-level assessment numbers. These arrays preserve every
    -- non-NULL number for a batch rather than treating counts as assessment numbers.
    assessment_status_numbers as (
        select
            batch_id,
            array_agg(
                distinct cast(bl_cdm2_no as string) ignore nulls
            ) as bl_cdm2_numbers,
            array_agg(
                distinct cast(el_cdm2_no as string) ignore nulls
            ) as el_cdm2_numbers,
            array_agg(distinct cast(bl_cp_no as string) ignore nulls) as bl_cp_numbers,
            array_agg(distinct cast(el_cp_no as string) ignore nulls) as el_cp_numbers,
            array_agg(distinct cast(bl_cs_no as string) ignore nulls) as bl_cs_numbers,
            array_agg(distinct cast(el_cs_no as string) ignore nulls) as el_cs_numbers,
            array_agg(distinct cast(el_fp_no as string) ignore nulls) as el_fp_numbers
        from {{ ref("int_student_global_assessment_status") }}
        group by batch_id
    ),

    diagnostic_numbers as (
        select
            batch_no,
            array_agg(distinct cast(bl_sd_no as string) ignore nulls) as bl_sd_numbers,
            array_agg(distinct cast(el_sd_no as string) ignore nulls) as el_sd_numbers
        from {{ ref("int_student_diagnostic") }}
        group by batch_no
    ),

    ca1_numbers as (
        select
            batch_no,
            array_agg(
                distinct cast(bl_ca1_no as string) ignore nulls
            ) as bl_ca1_numbers,
            array_agg(distinct cast(el_ca1_no as string) ignore nulls) as el_ca1_numbers
        from {{ ref("fct_ca") }}
        group by batch_no
    ),

    ca2_numbers as (
        select
            batch_no,
            array_agg(
                distinct cast(bl_ca2_no as string) ignore nulls
            ) as bl_ca2_numbers,
            array_agg(distinct cast(el_ca2_no as string) ignore nulls) as el_ca2_numbers
        from {{ ref("fct_student_global_cdm2_ind") }}
        group by batch_no
    ),

    partner_source as (
        select
            t8.*,
            s.bl_cdm2_numbers,
            s.el_cdm2_numbers,
            s.bl_cp_numbers,
            s.el_cp_numbers,
            s.bl_cs_numbers,
            s.el_cs_numbers,
            s.el_fp_numbers,
            d.bl_sd_numbers,
            d.el_sd_numbers,
            ca1.bl_ca1_numbers,
            ca1.el_ca1_numbers,
            ca2.bl_ca2_numbers,
            ca2.el_ca2_numbers,
            t6.total_students as partner_total_students,
            t6.total_sd2_students as partner_total_sd2_students
        from t8
        left join t6 on t8.batch_id = t6.batch_id
        left join assessment_status_numbers s on t8.batch_id = s.batch_id
        left join diagnostic_numbers d on t8.batch_no = d.batch_no
        left join ca1_numbers ca1 on t8.batch_no = ca1.batch_no
        left join ca2_numbers ca2 on t8.batch_no = ca2.batch_no
        where
            t8.school_partner in (
                'Akanksha Foundation',
                'Learning Links Foundation',
                'Masoom',
                'KMCT',
                'Educate Girls'
            )
        qualify row_number() over (partition by t8.batch_id order by t8.omr_type) = 1
    ),

    -- Expand each batch-level array into one row per non-NULL assessment number.
    partner_assessments as (
        select
            p.*,
            a.assessment_type as generated_assessment_type,
            a.assessment_object as generated_assessment_object,
            a.record_type as generated_record_type,
            array_length(a.assessment_numbers) as generated_assessment_count
        from partner_source p
        cross join
            unnest(
                case
                    when cast(p.batch_academic_year as string) = '2026'
                    then
                        [
                            struct(
                                'Student Details' as assessment_type,
                                'Demographic' as assessment_object,
                                cast(null as string) as record_type,
                                array(
                                    select cast(n as string)
                                    from
                                        unnest(
                                            generate_array(
                                                1,
                                                greatest(
                                                    coalesce(
                                                        p.partner_total_students, 0
                                                    ),
                                                    0
                                                )
                                            )
                                        ) as n
                                ) as assessment_numbers
                            ),
                            struct(
                                'Student Details 2',
                                'Student Details 2',
                                cast(null as string),
                                array(
                                    select cast(n as string)
                                    from
                                        unnest(
                                            generate_array(
                                                1,
                                                greatest(
                                                    coalesce(
                                                        p.partner_total_sd2_students, 0
                                                    ),
                                                    0
                                                )
                                            )
                                        ) as n
                                )
                            ),
                            struct(
                                'Student Diagnostics A',
                                'Student Diagnostic',
                                'Baseline',
                                p.bl_sd_numbers
                            ),
                            struct(
                                'Student Diagnositcs B',
                                'Student Diagnostic',
                                'Endline',
                                p.el_sd_numbers
                            ),
                            struct(
                                'Career Skills A',
                                'Career Skill',
                                'Baseline',
                                p.bl_cs_numbers
                            ),
                            struct(
                                'Career Skills B',
                                'Career Skill',
                                'Endline',
                                p.el_cs_numbers
                            ),
                            struct(
                                'Career Planning A',
                                'Career Planning',
                                'Baseline',
                                p.bl_cp_numbers
                            ),
                            struct(
                                'Career Planning B',
                                'Career Planning',
                                'Endline',
                                p.el_cp_numbers
                            ),

                            struct(
                                case
                                    when p.batch_grade in ('Grade 9', 'Grade 10')
                                    then 'Career Awareness Part 2A'
                                    when p.batch_grade in ('Grade 11', 'Grade 12')
                                    then 'Career Awareness A'
                                    else 'Career Decision Making- 2A'
                                end as assessment_type,
                                'CDM2' as assessment_object,
                                'Baseline' as record_type,
                                case
                                    when p.batch_grade in ('Grade 9', 'Grade 10')
                                    then p.bl_cdm2_numbers
                                    when p.batch_grade in ('Grade 11', 'Grade 12')
                                    then p.bl_ca1_numbers
                                    else p.bl_cdm2_numbers
                                end as assessment_numbers
                            ),
                            struct(
                                case
                                    when p.batch_grade in ('Grade 9', 'Grade 10')
                                    then 'Career Awareness Part 2B'
                                    when p.batch_grade in ('Grade 11', 'Grade 12')
                                    then 'Career Awareness B'
                                    else 'Career Decision Making- 2B'
                                end as assessment_type,
                                'CDM2' as assessment_object,
                                'Endline' as record_type,
                                case
                                    when p.batch_grade in ('Grade 9', 'Grade 10')
                                    then p.el_cdm2_numbers
                                    when p.batch_grade in ('Grade 11', 'Grade 12')
                                    then p.el_ca1_numbers
                                    else p.el_cdm2_numbers
                                end as assessment_numbers
                            ),
                            struct(
                                'Career Awareness Part 1A' as assessment_type,
                                'CDM3' as assessment_object,
                                'Baseline' as record_type,
                                p.bl_ca2_numbers as assessment_numbers
                            ),
                            struct(
                                'Career Awareness Part 1B' as assessment_type,
                                'CDM3' as assessment_object,
                                'Endline' as record_type,
                                p.el_ca2_numbers as assessment_numbers
                            ),
                            struct(
                                'Realities' as assessment_type,
                                'Realities' as assessment_object,
                                cast(null as string) as record_type,
                                array<string>[] as assessment_numbers
                            ),
                            struct(
                                'Remedial' as assessment_type,
                                'Remedial' as assessment_object,
                                cast(null as string) as record_type,
                                array<string>[] as assessment_numbers
                            ),
                            struct(
                                'Feedback' as assessment_type,
                                'Future Planning' as assessment_object,
                                'Endline' as record_type,
                                p.el_fp_numbers as assessment_numbers
                            )
                        ]

                    else
                        [
                            -- Previous-year mapping: retain existing logic
                            struct(
                                'Student Details' as assessment_type,
                                'Demographic' as assessment_object,
                                cast(null as string) as record_type,
                                array(
                                    select cast(n as string)
                                    from
                                        unnest(
                                            generate_array(
                                                1,
                                                greatest(
                                                    coalesce(
                                                        p.partner_total_students, 0
                                                    ),
                                                    0
                                                )
                                            )
                                        ) as n
                                ) as assessment_numbers
                            ),
                            struct(
                                'Student Details 2',
                                'Student Details 2',
                                cast(null as string),
                                array(
                                    select cast(n as string)
                                    from
                                        unnest(
                                            generate_array(
                                                1,
                                                greatest(
                                                    coalesce(
                                                        p.partner_total_sd2_students, 0
                                                    ),
                                                    0
                                                )
                                            )
                                        ) as n
                                )
                            ),
                            struct(
                                'Career Planning A',
                                'Career Planning',
                                'Baseline',
                                p.bl_cp_numbers
                            ),
                            struct(
                                'Career Planning B',
                                'Career Planning',
                                'Endline',
                                p.el_cp_numbers
                            ),
                            struct(
                                'Career Skills A',
                                'Career Skill',
                                'Baseline',
                                p.bl_cs_numbers
                            ),
                            struct(
                                'Career Skills B',
                                'Career Skill',
                                'Endline',
                                p.el_cs_numbers
                            ),
                            struct(
                                'Career Awareness Part 2A',
                                'CDM2',
                                'Baseline',
                                p.bl_cdm2_numbers
                            ),
                            struct(
                                'Career Awareness Part 2B',
                                'CDM2',
                                'Endline',
                                p.el_cdm2_numbers
                            ),
                            struct(
                                'Career Awareness A',
                                'CDM2',
                                'Baseline',
                                p.bl_ca1_numbers
                            ),
                            struct(
                                'Career Awareness B',
                                'CDM2',
                                'Endline',
                                p.el_ca1_numbers
                            ),
                            struct(
                                'Career Awareness Part 1A',
                                'CDM3',
                                'Baseline',
                                p.bl_ca2_numbers
                            ),
                            struct(
                                'Career Awareness Part 1B',
                                'CDM3',
                                'Endline',
                                p.el_ca2_numbers
                            ),
                            struct(
                                'Student Diagnostics A',
                                'Student Diagnostic',
                                'Baseline',
                                p.bl_sd_numbers
                            ),
                            struct(
                                'Student Diagnositcs B',
                                'Student Diagnostic',
                                'Endline',
                                p.el_sd_numbers
                            ),
                            struct(
                                'Feedback',
                                'Future Planning',
                                'Endline',
                                p.el_fp_numbers
                            )
                        ]
                end
            ) as a
        where array_length(a.assessment_numbers) > 0
    ),

    partner_upload_counts as (
        select
            batch_id,
            generated_assessment_type,
            max(generated_assessment_count) as upload_count
        from partner_assessments
        group by batch_id, generated_assessment_type
    ),

    global_output as (
        select *
        from t8
        where
            school_partner not in (
                'Akanksha Foundation',
                'Learning Links Foundation',
                'Masoom',
                'KMCT',
                'Educate Girls'
            )
    ),

    partner_output as (
        select
            p.* except (
                somrt_id,
                somrt_no,
                somrt_session_id,
                somrt_batch_id,
                somrt_batch_no,
                omr_type,
                omr_assessment_object,
                omr_assessment_count,
                omr_assessment_record_type,
                omr_received_count,
                omr_received_by,
                number_of_students_in_batch,
                omr_received_date,
                first_omr_upload_date,
                session_id,
                session_facilitator_id,
                omr_required,
                omrs_received,
                total_student_present,
                total_parent_present,
                attendance_count,
                session_name,
                session_date,
                present_count,
                omr_upload_count,
                tat1,
                tat2,
                tat3,
                bl_cdm2_numbers,
                el_cdm2_numbers,
                bl_cp_numbers,
                el_cp_numbers,
                bl_cs_numbers,
                el_cs_numbers,
                el_fp_numbers,
                bl_sd_numbers,
                el_sd_numbers,
                bl_ca1_numbers,
                el_ca1_numbers,
                bl_ca2_numbers,
                el_ca2_numbers,
                partner_total_students,
                partner_total_sd2_students,
                generated_assessment_type,
                generated_assessment_object,
                generated_record_type,
                generated_assessment_count
            ),
            cast(null as string) as somrt_id,
            cast(null as string) as somrt_no,
            cast(null as string) as somrt_session_id,
            cast(null as string) as somrt_batch_id,
            cast(null as string) as somrt_batch_no,
            p.generated_assessment_type as omr_type,
            p.generated_assessment_object as omr_assessment_object,
            p.generated_assessment_count as omr_assessment_count,
            p.generated_record_type as omr_assessment_record_type,
            cast(null as int64) as omr_received_count,
            cast(null as string) as omr_received_by,
            cast(null as int64) as number_of_students_in_batch,
            cast(null as date) as omr_received_date,
            cast(null as date) as first_omr_upload_date,
            cast(null as string) as session_id,
            cast(null as string) as session_facilitator_id,
            cast(null as bool) as omr_required,
            cast(null as string) as omrs_received,
            cast(null as int64) as total_student_present,
            cast(null as int64) as total_parent_present,
            cast(null as int64) as attendance_count,
            cast(null as string) as session_name,
            cast(null as date) as session_date,
            cast(null as int64) as present_count,
            coalesce(u.upload_count, 0) as omr_upload_count,
            cast(null as int64) as tat1,
            cast(null as int64) as tat2,
            cast(null as int64) as tat3
        from partner_assessments p
        left join
            partner_upload_counts u
            on p.batch_id = u.batch_id
            and p.generated_assessment_type = u.generated_assessment_type
    ),

    final as (
        select *
        from global_output
        union all by name
        select *
        from partner_output
    )

select *
from final
