with
    score as (
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
            bl_ca2_no,
            bl_q3_1,
            bl_q3_2,
            bl_q3_3,
            bl_q3_4,
            bl_q3_5,
            bl_q3_6,
            bl_q3_7,
            bl_q3_8,
            bl_q3_9,
            bl_q3_10,
            bl_q3_1_marks,
            bl_q3_2_marks,
            bl_q3_3_marks,
            bl_q3_4_marks,
            bl_q3_5_marks,
            bl_q3_6_marks,
            bl_q3_7_marks,
            bl_q3_8_marks,
            bl_q3_9_marks,
            bl_q3_10_marks,
            el_createddate,
            el_ca2_no,
            el_q3_1,
            el_q3_2,
            el_q3_3,
            el_q3_4,
            el_q3_5,
            el_q3_6,
            el_q3_7,
            el_q3_8,
            el_q3_9,
            el_q3_10,
            el_q3_1_marks,
            el_q3_2_marks,
            el_q3_3_marks,
            el_q3_4_marks,
            el_q3_5_marks,
            el_q3_6_marks,
            el_q3_7_marks,
            el_q3_8_marks,
            el_q3_9_marks,
            el_q3_10_marks,
            bl_q3_total_marks,
            el_q3_total_marks,
            bl_q3_bucket,
            el_q3_bucket,
            bl_el_q3_total_marks,
            bl_el_q3_status,
            type_of_assessment_filled,
            bl_el_q3_1,
            bl_el_q3_2,
            bl_el_q3_3,
            bl_el_q3_4,
            bl_el_q3_5,
            bl_el_q3_6,
            bl_el_q3_7,
            bl_el_q3_8,
            bl_el_q3_9,
            bl_el_q3_10
        from {{ ref("int_score_cdm2_ind") }}
    ),

    seed_industry as (
        select
            state_id,
            state_name,
            question_no,
            industry_code,
            industry_name,
            option,
            career_option,
            is_correct,
            career_type
        from {{ ref("seed_industry") }}
    ),

    baseline as (

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
            bl_ca2_no,
            question_no,
            trim(option) as option

        from
            score unpivot (
                answers for question_no in (
                    bl_q3_1,
                    bl_q3_2,
                    bl_q3_3,
                    bl_q3_4,
                    bl_q3_5,
                    bl_q3_6,
                    bl_q3_7,
                    bl_q3_8,
                    bl_q3_9,
                    bl_q3_10
                )
            )

        cross join unnest(split(answers, ',')) as option

    ),

    baseline_seed as (

        select
            b.*,

            case
                b.question_no
                when 'bl_q3_1'
                then 'Q6.5'
                when 'bl_q3_2'
                then 'Q6.6'
                when 'bl_q3_3'
                then 'Q6.7'
                when 'bl_q3_4'
                then 'Q6.2'
                when 'bl_q3_5'
                then 'Q6.3'
                when 'bl_q3_6'
                then 'Q6.8'
                when 'bl_q3_7'
                then 'Q6.10'
                when 'bl_q3_8'
                then 'Q6.1'
                when 'bl_q3_9'
                then 'Q6.9'
                when 'bl_q3_10'
                then 'Q6.4'
            end as mapped_question_no,

            s.industry_code,
            s.industry_name,
            s.career_option,
            s.is_correct,
            s.career_type

        from baseline b

        left join
            seed_industry s
            on upper(trim(b.school_state)) = upper(trim(s.state_name))
            and trim(b.option) = trim(s.option)
            and (
                case
                    b.question_no
                    when 'bl_q3_1'
                    then 'Q6.5'
                    when 'bl_q3_2'
                    then 'Q6.6'
                    when 'bl_q3_3'
                    then 'Q6.7'
                    when 'bl_q3_4'
                    then 'Q6.2'
                    when 'bl_q3_5'
                    then 'Q6.3'
                    when 'bl_q3_6'
                    then 'Q6.8'
                    when 'bl_q3_7'
                    then 'Q6.10'
                    when 'bl_q3_8'
                    then 'Q6.1'
                    when 'bl_q3_9'
                    then 'Q6.9'
                    when 'bl_q3_10'
                    then 'Q6.4'
                end
            )
            = s.question_no

    ),

    baseline_summary as (

        select
            student_id,

            countif(is_correct = 1) as bl_student_correct_answers,

            countif(
                is_correct = 1 and career_type = 'Emerging'
            ) as bl_student_correct_answers_emerging,

            countif(
                is_correct = 1 and career_type = 'Steady'
            ) as bl_student_correct_answers_steady,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'II'
            ) as bl_ii_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'ADA'
            ) as bl_ada_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'ME'
            ) as bl_me_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'AGRI'
            ) as bl_agri_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'HC'
            ) as bl_healthcare_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'WF'
            ) as bl_wf_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'TH'
            ) as bl_th_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'IT'
            ) as bl_it_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'ENV'
            ) as bl_env_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'ENG'
            ) as bl_eng_emerging_count

        from baseline_seed

        group by student_id

    ),

    baseline_final as (

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
            s.assessment_barcode,
            s.bl_createddate,
            s.bl_ca2_no,

            b.* except (student_id),

            case
                when upper(s.school_state) = 'NAGALAND'
                then 28
                when upper(s.school_state) = 'MAHARASHTRA'
                then 32
                when upper(s.school_state) = 'GOA'
                then 27
                when upper(s.school_state) = 'HARYANA'
                then 27
                when upper(s.school_district) = 'YAMUNANAGAR'
                then 27
                when upper(s.school_state) = 'RAJASTHAN'
                then 35
            end as geography_wise_emerging_careers,

            case
                when upper(s.school_state) = 'NAGALAND'
                then 22
                when upper(s.school_state) = 'MAHARASHTRA'
                then 18
                when upper(s.school_state) = 'GOA'
                then 23
                when upper(s.school_state) = 'HARYANA'
                then 23
                when upper(s.school_district) = 'YAMUNANAGAR'
                then 23
                when upper(s.school_state) = 'RAJASTHAN'
                then 15
            end as geography_wise_steady_careers

        from score s

        left join baseline_summary b on s.student_id = b.student_id

    ),

    baseline_percentage as (

        select
            *,

            concat(
    cast(
        round(
            safe_divide(
                bl_student_correct_answers_emerging,
                geography_wise_emerging_careers
            ) * 100,
            2
        ) as string
    ),
    '%'
) as bl_student_correct_answers_emerging_pct,

concat(
    cast(
        round(
            safe_divide(
                bl_student_correct_answers_steady,
                geography_wise_steady_careers
            ) * 100,
            2
        ) as string
    ),
    '%'
) as bl_student_correct_answers_steady_pct
        from baseline_final

    ),

    endline as (

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
            el_createddate,
            el_ca2_no,
            question_no,
            trim(option) as option

        from
            score unpivot (
                answers for question_no in (
                    el_q3_1,
                    el_q3_2,
                    el_q3_3,
                    el_q3_4,
                    el_q3_5,
                    el_q3_6,
                    el_q3_7,
                    el_q3_8,
                    el_q3_9,
                    el_q3_10
                )
            )

        cross join unnest(split(answers, ',')) as option

    ),

    endline_seed as (

        select
            e.*,

            case
                e.question_no
                when 'el_q3_1'
                then 'Q6.5'
                when 'el_q3_2'
                then 'Q6.6'
                when 'el_q3_3'
                then 'Q6.7'
                when 'el_q3_4'
                then 'Q6.2'
                when 'el_q3_5'
                then 'Q6.3'
                when 'el_q3_6'
                then 'Q6.8'
                when 'el_q3_7'
                then 'Q6.10'
                when 'el_q3_8'
                then 'Q6.1'
                when 'el_q3_9'
                then 'Q6.9'
                when 'el_q3_10'
                then 'Q6.4'
            end as mapped_question_no,

            s.industry_code,
            s.industry_name,
            s.career_option,
            s.is_correct,
            s.career_type

        from endline e

        left join
            seed_industry s
            on upper(trim(e.school_state)) = upper(trim(s.state_name))
            and trim(e.option) = trim(s.option)
            and (
                case
                    e.question_no
                    when 'el_q3_1'
                    then 'Q6.5'
                    when 'el_q3_2'
                    then 'Q6.6'
                    when 'el_q3_3'
                    then 'Q6.7'
                    when 'el_q3_4'
                    then 'Q6.2'
                    when 'el_q3_5'
                    then 'Q6.3'
                    when 'el_q3_6'
                    then 'Q6.8'
                    when 'el_q3_7'
                    then 'Q6.10'
                    when 'el_q3_8'
                    then 'Q6.1'
                    when 'el_q3_9'
                    then 'Q6.9'
                    when 'el_q3_10'
                    then 'Q6.4'
                end
            )
            = s.question_no

    ),

    endline_summary as (

        select
            student_id,

            countif(is_correct = 1) as el_student_correct_answers,

            countif(
                is_correct = 1 and career_type = 'Emerging'
            ) as el_student_correct_answers_emerging,

            countif(
                is_correct = 1 and career_type = 'Steady'
            ) as el_student_correct_answers_steady,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'II'
            ) as el_ii_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'ADA'
            ) as el_ada_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'ME'
            ) as el_me_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'AGRI'
            ) as el_agri_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'HC'
            ) as el_healthcare_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'WF'
            ) as el_wf_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'TH'
            ) as el_th_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'IT'
            ) as el_it_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'ENV'
            ) as el_env_emerging_count,

            countif(
                is_correct = 1 and career_type = 'Emerging' and industry_code = 'ENG'
            ) as el_eng_emerging_count

        from endline_seed

        group by student_id

    ),

    endline_final as (

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
            s.assessment_barcode,
            s.el_createddate,
            s.el_ca2_no,

            e.* except (student_id),

            case
                when upper(s.school_state) = 'NAGALAND'
                then 28
                when upper(s.school_state) = 'MAHARASHTRA'
                then 32
                when upper(s.school_state) = 'GOA'
                then 27
                when upper(s.school_district) = 'YAMUNANAGAR'
                then 27
                when upper(s.school_state) = 'HARYANA'
                then 27
                when upper(s.school_state) = 'RAJASTHAN'
                then 35
            end as geography_wise_emerging_careers,

            case
                when upper(s.school_state) = 'NAGALAND'
                then 22
                when upper(s.school_state) = 'MAHARASHTRA'
                then 18
                when upper(s.school_state) = 'GOA'
                then 23
                when upper(s.school_district) = 'YAMUNANAGAR'
                then 23
                when upper(s.school_state) = 'HARYANA'
                then 23
                when upper(s.school_state) = 'RAJASTHAN'
                then 15
            end as geography_wise_steady_careers

        from score s

        left join endline_summary e on s.student_id = e.student_id

    ),


    endline_percentage as (

        select
            *,

            concat(
    cast(
        round(
            safe_divide(
                el_student_correct_answers_emerging,
                geography_wise_emerging_careers
            ) * 100,
            2
        ) as string
    ),
    '%'
) as el_student_correct_answers_emerging_pct,

concat(
    cast(
        round(
            safe_divide(
                el_student_correct_answers_steady,
                geography_wise_steady_careers
            ) * 100,
            2
        ) as string
    ),
    '%'
) as el_student_correct_answers_steady_pct
        from endline_final

    )

select

    -- Common Columns
    b.student_id,
    b.student_barcode,
    b.gender,
    b.batch_no,
    b.batch_academic_year,
    b.batch_language,
    b.facilitator_id,
    b.facilitator_name,
    b.facilitator_email,
    b.school_id,
    b.school_name,
    b.school_taluka,
    b.school_ward,
    b.school_district,
    b.school_state,
    b.school_partner,
    b.school_area,
    b.donor_id,
    b.batch_donor,
    b.batch_grade,
    b.assessment_barcode,

    -- Baseline
    b.bl_createddate,
    b.bl_ca2_no,
    b.geography_wise_emerging_careers,
    b.geography_wise_steady_careers,
    b.bl_student_correct_answers,
    b.bl_student_correct_answers_emerging,
    b.bl_student_correct_answers_emerging_pct,
    b.bl_student_correct_answers_steady,
    b.bl_student_correct_answers_steady_pct,
    b.bl_ii_emerging_count,
    b.bl_ada_emerging_count,
    b.bl_me_emerging_count,
    b.bl_agri_emerging_count,
    b.bl_healthcare_emerging_count,
    b.bl_wf_emerging_count,
    b.bl_th_emerging_count,
    b.bl_it_emerging_count,
    b.bl_env_emerging_count,
    b.bl_eng_emerging_count,

    -- Endline
    e.el_createddate,
    e.el_ca2_no,
    e.el_student_correct_answers,
    e.el_student_correct_answers_emerging,
    e.el_student_correct_answers_emerging_pct,
    e.el_student_correct_answers_steady,
    e.el_student_correct_answers_steady_pct,
    e.el_ii_emerging_count,
    e.el_ada_emerging_count,
    e.el_me_emerging_count,
    e.el_agri_emerging_count,
    e.el_healthcare_emerging_count,
    e.el_wf_emerging_count,
    e.el_th_emerging_count,
    e.el_it_emerging_count,
    e.el_env_emerging_count,
    e.el_eng_emerging_count

from baseline_percentage b

left join endline_percentage e on b.student_id = e.student_id
