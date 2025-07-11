with t1 as (
        select
            a.school_district,
            a.batch_grade,
            a.facilitator_name,facilitator_email,
            a.school_name,
            a.batch_no,
            batch_donor,
            a.batch_academic_year,
            batch_language,
            school_area,
            school_taluka,
            school_partner,
            a.stg_saf_sd,
            a.bl_saf_raw,
            a.saf_correct,
            a.tsp_saf_interest,
            a.tsp_saf_aptitude,
            a.tsp_saf_qf,
            saf_atleast_one_interest,
            saf_atleast_one_aptitude,
            saf_atleast_one_quiz,
            saf_atleast_one_feedback,
            stg_sar_sd,
            bl_sar_raw,
            sar_correct,
            sar_atleast_one_quiz,
            sar_atleast_one_reality,
            tsp_sar_quiz2,
            tsp_sar_reality
        from
            (
                select
                    school_district,
                    batch_grade,
                    facilitator_name,facilitator_email,
                    school_name,
                    batch_no,
                    batch_academic_year,
                    batch_donor,
                    batch_language,
                    school_area,
                    school_taluka,
                    school_partner,
                    sum(stg_saf_sd) stg_saf_sd,
                    sum(bl_saf_raw) bl_saf_raw,
                    sum(saf_correct) saf_correct,
                    sum(tsp_saf_interest) tsp_saf_interest,
                    sum(tsp_saf_aptitude) tsp_saf_aptitude,
                    sum(tsp_saf_qf) tsp_saf_qf,
                    sum(stg_sar_sd) stg_sar_sd,
                    sum(bl_sar_raw) bl_sar_raw,
                    sum(sar_correct) sar_correct,
                    sum(tsp_sar_reality) tsp_sar_reality,
                    sum(tsp_sar_quiz2) tsp_sar_quiz2
                from {{ref('fct_global_somrt_upload_form')}}
                where batch_academic_year >= 2023 and school_district = 'Nagaland'
                group by
                    school_district,
                    batch_grade,
                    facilitator_name,facilitator_email,
                    school_name,
                    batch_no,
                    batch_academic_year,
                    batch_donor,
                    batch_language,
                    school_area,
                    school_taluka,
                    school_partner
            ) a
        left join
            (
                select
                    school_district,
                    batch_grade,
                    facilitator_name,
                    school_name,
                    batch_no,
                    count(distinct student_barcode) student_barcode,
                    sum(saf_atleast_one_interest) saf_atleast_one_interest,
                    sum(saf_atleast_one_aptitude) saf_atleast_one_aptitude,
                    sum(saf_atleast_one_quiz) saf_atleast_one_quiz,
                    sum(saf_atleast_one_feedback) saf_atleast_one_feedback,
                    sum(sar_atleast_one_quiz) sar_atleast_one_quiz,
                    sum(sar_atleast_one_reality) sar_atleast_one_reality
                from {{ref('fct_student_global_assessment')}}
                where batch_academic_year >= 2023 and school_district = 'Nagaland'
                group by
                    school_district,
                    batch_grade,
                    facilitator_name,
                    school_name,
                    batch_no
            ) b
            on a.batch_no = b.batch_no
    )

select * from t1