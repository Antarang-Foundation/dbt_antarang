WITH 
base AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY hm_id
            ORDER BY hm_session_date DESC
        ) AS rn_last,
        CASE
            WHEN session_date IS NOT NULL 
            AND total_student_present IS NOT NULL 
            AND total_parent_present IS NOT NULL THEN 1 END
         AS completed_sessions
    FROM {{ ref('dev_int_hm_session') }}
),

-- Aggregate expected sessions BEFORE rn_last filter
expected_sessions_agg AS (
    SELECT 
        hm_id,
        
        SUM(batch_expected_session) AS total_expected_sessions,

        SUM(total_student_present) AS total_student_present_sum,

        SUM(total_parent_present) AS total_parent_present_sum,

        /*SUM(CASE WHEN session_type = 'Student' AND session_date IS NOT NULL 
            AND total_student_present IS NOT NULL AND total_parent_present IS NOT NULL 
            THEN total_student_present
        ELSE 0 END) AS no_of_student_sessions_attended_by_hm,

        SUM(CASE WHEN session_type = 'Parent' AND session_date IS NOT NULL 
            AND total_parent_present IS NOT NULL AND total_student_present IS NOT NULL 
            THEN total_parent_present
        ELSE 0 END) AS no_of_parent_sessions_attended_by_hm*/
    FROM {{ ref('dev_int_hm_session') }}
    GROUP BY hm_id
),

sessions_attended_agg AS (
    SELECT
        hm_id,

        SUM(
            CASE 
                WHEN session_type = 'Student'
                 AND session_date IS NOT NULL
                 AND total_student_present IS NOT NULL
                 AND LOWER(TRIM(hm_attended)) = 'yes'
                THEN total_student_present
                ELSE 0
            END
        ) AS no_of_student_sessions_attended_by_hm,

        SUM(
            CASE 
                WHEN session_type = 'Parent'
                 AND session_date IS NOT NULL
                 AND total_parent_present IS NOT NULL
                 AND LOWER(TRIM(hm_attended)) = 'yes'
                THEN total_parent_present
                ELSE 0
            END
        ) AS no_of_parent_sessions_attended_by_hm

    FROM {{ ref('dev_int_hm_session') }}
    GROUP BY hm_id
),
-- Now get hm_session details using only the latest row per hm_id for static info
hm_session AS (
    SELECT 
        b.hm_school_id, b.facilitator_name, b.session_academic_year, b.batch_language, 
        b.school_name, b.school_taluka, b.school_district, b.school_state, 
        b.school_area, b.school_partner, 

        es.total_expected_sessions AS no_of_expected_sessions,

        COUNT(DISTINCT b.hm_id) AS no_of_expected_hm_sessions,
    
        COUNT(DISTINCT CASE 
            WHEN b.hm_session_date IS NOT NULL THEN b.hm_session_date 
        END) AS no_of_session_scheduled,

        COUNT(DISTINCT CASE 
            WHEN LOWER(TRIM(b.hm_session_name)) IN ('check-in 1','check-in 2','check-in 3','check-in 4')
             AND LOWER(TRIM(b.session_status)) IN ('complete', 'completed')
            THEN b.hm_id 
        END) AS no_of_checkin_sessions_completed,

        COUNT(DISTINCT CASE 
            WHEN LOWER(TRIM(b.hm_session_name)) = 'year end annual report'
             AND LOWER(TRIM(b.session_status)) IN ('complete', 'completed')
            THEN b.hm_id 
        END) AS no_of_year_end_sessions_completed,

        COUNT(DISTINCT CASE 
            WHEN LOWER(TRIM(b.hm_session_name)) IN 
                ('check-in 1','check-in 2','check-in 3','check-in 4','year end annual report')
             AND LOWER(TRIM(b.hm_attended)) = 'yes'
            THEN b.hm_id 
        END) AS no_of_sessions_hm_attended,

        COUNT(*) AS total_sessions,
 
        is_batch_fully_completed AS school_completion_status,

        es.total_student_present_sum AS no_of_expected_student_sessions,
        es.total_parent_present_sum AS no_of_expected_parent_sessions,

        -- ✅ FIXED VALUES (correct & stable)
        sa.no_of_student_sessions_attended_by_hm,
        sa.no_of_parent_sessions_attended_by_hm

    FROM base b
    LEFT JOIN expected_sessions_agg es 
        ON b.hm_id = es.hm_id
    LEFT JOIN sessions_attended_agg sa
        ON b.hm_id = sa.hm_id
    WHERE b.rn_last = 1
    GROUP BY 
        b.hm_school_id, b.facilitator_name, b.session_academic_year, b.batch_language, 
        b.school_name, b.school_taluka, b.school_district, b.school_state, 
        b.school_area, b.school_partner, 
        es.total_expected_sessions, 
        es.total_student_present_sum, 
        es.total_parent_present_sum, 
        sa.no_of_student_sessions_attended_by_hm,
        sa.no_of_parent_sessions_attended_by_hm,
        is_batch_fully_completed
),



pre_latest AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY school_name
                ORDER BY pre_submission_time DESC
            ) AS rn_pre
        FROM {{ ref('dev_int_hm_assessment') }}
        WHERE pre_id IS NOT NULL
    )
    WHERE rn_pre = 1 --Cleaning the latest entry only
),

po_latest AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY school_name
                ORDER BY po_submission_time DESC
            ) AS rn_po
        FROM {{ ref('dev_int_hm_assessment') }}
        WHERE po_id IS NOT NULL
    )
    WHERE rn_po = 1
),

pp_latest AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY school_name
                ORDER BY pp_submission_time DESC
            ) AS rn_pp
        FROM {{ ref('dev_int_hm_assessment') }}
        WHERE pp_id IS NOT NULL
    )
    WHERE rn_pp = 1
),


hm_assessment AS (
    SELECT
        COALESCE (p.school_name, pp.school_name, po.school_name) AS ass_school_name ,

p.pre_start, p.pre_end, p.pre_date, p.Q4_pre__years_in_this_school, p.Q5_pre_organisation_last_year, p.Q5_pre_option_1, p.Q5_pre_option_2,
p.Q5_pre_option_3, p.Q6_pre_heard_of_antarang, p.Q7_pre_how_did_you_hear, p.Q7a_pre_newspapers, p.Q7b_pre_social_media__facebook__linked, 
p.Q7c_pre_from_government_meetings___oth, p.Q7d_pre_from_parents__students, p.Q7e_pre_search_engines___google_etc, 
p.Q7f_pre_worked_with_them_in_the_past, p.Q7g_pre_workshops__training_sessions__, p.Q7h_pre_other__please_specify, 
p.Q7i_pre_NA, p.Q7_pre_Other_Please_Specify,  p.Q8_pre_which_grade, p.Q8a_pre_grade_8,
p.Q8b_pre_grade_9, p.Q8c_pre_grade_10, p.Q8d_pre_grade_11, p.Q8e_pre_grade_12, p.Q8f_pre_none_of_the_above, p.Q8g_pre_i_don_t_know__not_sure, 
p.Q8h_pre_other__please_specify, p.Q8_pre_Others_Please_Specify, p.Q9_pre_which_follow, p.Q9a_pre_up__to__date_information_about_careers,
p.Q9b_pre_parents__caregivers_sessions, p.Q9c_pre_a_trained_career_teacher_in_school, p.Q9d_pre_individual_counselling,
p.Q9e_pre_psychometric_tests, p.Q9f_pre_records_of_students_career_plans, p.Q9g_pre_academic_subjects_teaching_careers,
p.Q9h_pre_information_on_education___training_path, p.Q9i_pre_visits__talks_with_educational_instituti,
p.Q9j_pre_records_of_student_learning_in_the_caree, p.Q9k_pre_records_of_students_career_plans_after_c, p.Q9l_pre_i_don_t_know__not_sure,
p.Q10_pre_which_follow, p.Q10a_pre_i_had_no_role_to_play_in_the_program, p.Q10b_pre_provided_a_fixed_time_for_the_career_ses,
p.Q10c_pre_observe_career_sessions_regularly, p.Q10d_pre_ensured_students_attended_the_career_pro, p.Q10e_pre_spoke_to_parents_about_the_career_progra,
p.Q10f_pre_shared_about_the_career_program_in_hm_me, p.Q10g_pre_provided_feedback__suggestions_for_the_p, p.Q10h_pre_provided_school_budget__money_for_the_ca,
p.Q10i_pre_regularly_spoke_to_the_career_teacher_ab, p.Q10j_pre_ensured_the_program_started_and_ended_on, p.Q10k_pre_arranged_additional_career_activities_in,
p.Q10l_pre_spoke_to_my_school_alumni_to_see_how_the, p.Q10m_pre_ensured_there_is_a_trained_career_teache, p.Q10n_pre_i_do_not_know__not_sure,
p.Q11_pre_think_of_career, p.Q12_pre_confident_in_career_program, p.Q13_pre_program_activities, p.Q14_pre_program_session,
p.Q15_pre_important_secondary_students, p.Q16A_pre_program_inc_tions_all_students, p.Q16B_pre_supporting_students_training_after_school,
p.Q16C_pre_encouraging_students_choosing_careers, p.Q17_pre_one_thing_for_you_students, p.pre_id, p.pre_uuid, p.pre_submission_time,

po.po_start, po.po_end, po.po_date, po.Q4_po_heard_of_antarang, po.Q5_po_grade, po.Q5a_po_grade_8, po.Q5b_po_grade_9, po.Q5c_po_grade_10, po.Q5d_po_grade_11,
po.Q5e_po_grade_12, po.Q5f_po_none_of_the_above, po.Q5g_po_i_don_t_know__not_sure, po.Q5h_po_other__please_specify, po.Q5_po_others_please_specify,
po.Q6_po_which_follow, po.Q6a_po_i_had_no_role_to_play_in_the_program, po.Q6b_po_provided_a_fixed_time_for_the_career_ses,
po.Q6c_po_observe_career_sessions_regularly, po.Q6d_po_ensured_students_attended_the_career_pro, po.Q6e_po_spoke_to_parents_about_the_career_progra,
po.Q6f_po_shared_about_the_career_program_in_hm_me, po.Q6g_po_provided_feedback__suggestions_for_the_p, po.Q6h_po_provided_school_budget__money_for_the_ca,
po.Q6i_po_regularly_spoke_to_the_career_teacher_ab, po.Q6j_po_ensured_the_program_started_and_ended_on, po.Q6k_po_arranged_additional_career_activities_in,
po.Q6l_po_spoke_to_my_school_alumni_to_see_how_the, po.Q6m_po_ensured_there_is_a_trained_career_teache, po.Q6n_po_i_do_not_know__not_sure,
po.Q7_po_role, po.Q7a_po_i_have_no_role_to_play, po.Q7b_po_provide_a_fixed_time_for_the_career_sess, po.Q7c_po_observe_career_sessions_regularly,
po.Q7d_po_ensure_students_attended_the_career_prog, po.Q7e_po_speak_to_parents_about_the_career_progra, po.Q7f_po_share_about_the_career_program_in_hm_mee,
po.Q7g_po_provide_feedback__suggestions_for_the_pr, po.Q7h_po_provide_school_budget__money_for_the_car, po.Q7i_po_regularly_speak_to_the_career_teacher_ab,
po.Q7j_po_ensuredthe_program_starts_and_ends_on_ti, po.Q7k_po_arrange_additional_career_activities_in_, po.Q7l_po_speak_to_my_school_alumni_to_see_how_the,
po.Q7m_po_ensure_there_is_a_trained_career_teacher, po.Q7n_po_i_do_not_know__not_sure, po.Q8_po_materials, po.Q8a_po_a_student_book,
po.Q8b_po_a_teacher_guide, po.Q8c_po_school_poster, po.Q8d_po_career_chatbot, po.Q8e_po_student_certificates, po.Q8f_po_student_assessments,
po.Q8g_po_career_videos, po.Q8h_po_parent_handouts, po.Q8i_po_counselling_report, po.Q9_po_confidence, po.Q10_po_secondary_students,
po.Q11A_po_orientation_session_covered, po.Q11B_po_content_was_easy_to_understand, po.Q11C_po_encouraging_students_choosing_careers,
po.Q12_po_thin_for_students, po.Q13_po_big_rom_this_orientation, po.Q14_po_nex_after_this_orientation, po.po_id, po.po_uuid, po.po_submission_time,

pp.pp_start, pp.pp_end, pp.pp_date, pp.Q4_pp_how_many_year_in_this_school, pp.Q5_pp_org_our_school_last_year, pp.Q5_pp_others_please_specify,
pp.Q6_pp_heard_of_antarng, pp.Q7_pp_how_did_you_hear, pp.Q7a_pp_Newspaper, pp.Q7b_pp_Social_media_Facebook_linkedin, pp.Q7c_pp_From_government_meetings__other,
pp.Q7d_pp_From_Parents_Students, pp.Q7e_pp_Search_Engines__Google_etc, pp.Q7f_pp_Peer_HMsTeachers, pp.Q7g_pp_Conducted_career_education_program_in_my_school,
pp.Q7h_pp_Workshops_training_sessions_or_webinars, pp.Q7i_pp_Word_of_mouth_from_alumni_or_beneficiaries, pp.Q7j_pp_Antarangs_website,
pp.Q7k_pp_other__please_specify, pp.Q7_pp_other_please_specify, pp.Q8_pp_in_which_grade, pp.Q8a_pp_grade_8, pp.Q8b_pp_grade_9, pp.Q8c_pp_grade_10,
pp.Q8d_pp_grade_11, pp.Q8e_pp_grade_12, pp.Q8f_pp_none_of_the_above, pp.Q8g_pp_i_don_t_know__not_sure, pp.Q8h_pp_other__please_specify,
pp.Q8_pp_Others_Please_Specify_001, pp.Q9_pp_which_of_the_following, pp.Q9a_pp_Up_to_date_info_about_careers, pp.Q9b_pp_Sessions_for_parents_or_caregivers,
pp.Q9c_pp_Trained_external_career_teacher_at_school, pp.Q9d_pp_Trained_career_teacher_in_charge, pp.Q9e_pp_One_on_one_career_counselling,
pp.Q9f_pp_Interest_and_aptitude_tests, pp.Q9g_pp_Record_of_students_career_plans, pp.Q9h_pp_Info_on_education_or_training_after_school,
pp.Q9i_pp_Career_expert_talks_during_subject_classes, pp.Q9j_pp_Talks_or_visits_to_colleges_and_workplaces,
pp.Q9k_pp_Data_collected_on_students_plans_after_school, pp.Q9l_pp_i_do_not_know__not_sure, pp.Q10_pp_answer_that_apply,
pp.Q10a_pp_Fixed_a_regular_time_for_career_sessions, pp.Q10b_pp_Visited_the_career_sessions, pp.Q10c_pp_Made_sure_students_attended,
pp.Q10d_pp_Spoke_to_parents_about_it, pp.Q10e_pp_Shared_updates_in_meetings, pp.Q10f_pp_Gave_ideas_or_suggestions_to_the_career_teacher,
pp.Q10g_pp_Shared_updates_in_meetings_or_reports, pp.Q10h_pp_Utilised_school_funds_for_the_program, pp.Q10i_pp_Talked_often_with_the_career_teacher,
pp.Q10j_pp_Started_and_ended_the_program_on_time, pp.Q10k_pp_Arranged_extra_career_activities, pp.Q10l_pp_invited_ex_students_to_share_their_job_stories,
pp.Q10m_pp_Made_sure_the_school_had_a_trained_career_teacher, pp.Q10n_pp_i_do_not_know__not_sure, pp.Q11_pp_thin_for_career_program,
pp.Q12_pp_confident, pp.Q13_pp_program_activities, pp.Q14_pp_program_sessions, pp.Q15_pp_important_secondary_students, pp.Q16_pp_career_decisions,
pp.Q17_pp_materials_that_apply, pp.Q17a_pp_A_student_book, pp.Q17b_pp_A_teacher_guide, pp.Q17c_pp_School_Poster, pp.Q17d_pp_Career_Chatbot,
pp.Q17e_pp_Student_Certificates, pp.Q17f_pp_Student_Assessments, pp.Q17g_pp_Career_Videos, pp.Q17h_pp_Parent_Handouts, pp.Q17i_pp_Counselling_Report,
pp.Q18A_pp_program_inc_of_all_students, pp.Q18B_pp_supporting_students_raining_after_school, pp.Q18C_pp_encouraging_students_choosing_careers,
pp.Q19_pp_leave_school, pp.Q20_pp_thin_for_students, pp.pp_id, pp.pp_uuid, pp.pp_submission_time
FROM pre_latest p
    FULL OUTER JOIN po_latest po ON p.school_name = po.school_name --Remove the latest join it should be 'A=B, A=C, A=D'
    FULL OUTER JOIN pp_latest pp ON p.school_name = pp.school_name
),

hm_assessment_dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY ass_school_name
                   ORDER BY
                       GREATEST(
                           pre_submission_time,
                           po_submission_time,
                           pp_submission_time
                       ) DESC
               ) AS rn
        FROM hm_assessment
    )
    WHERE rn = 1
),

int_global_session AS (
    SELECT 
        MIN(CASE WHEN fac_start_date IS NOT NULL THEN fac_start_date END) AS fac_start_date,
        MIN(CASE WHEN fac_end_date IS NOT NULL THEN fac_end_date END) AS fac_end_date,
        school_id FROM {{ ref('dev_int_global_session') }}
        where batch_academic_year >= 2025
        GROUP BY school_id
),

hm_orientation AS (SELECT date as orientation_date, state as hm_state, overall_attendance as orientation_attendance
        from {{ source('salesforce', 'Attendace_sheet') }}
        where year = '2025'
),

final as (
    SELECT h.hm_school_id, h.facilitator_name, h.session_academic_year, h.batch_language, --COALESCE (h.school_name, a.ass_school_name) AS school_name,
    h.school_name, h.school_taluka, h.school_district, 
    h.school_state, h.school_area, h.school_partner, o.orientation_date, o.orientation_attendance, s.fac_start_date, s.fac_end_date, 
    DATE_DIFF(DATE(fac_start_date), CAST(orientation_date AS DATE), DAY) AS TAT_1,
    h.no_of_expected_hm_sessions, 
    h.no_of_session_scheduled, h.no_of_checkin_sessions_completed, h.no_of_year_end_sessions_completed, h.no_of_sessions_hm_attended, 
    h.school_completion_status, h.no_of_expected_sessions, h.no_of_expected_student_sessions, h.no_of_expected_parent_sessions, 
     h.no_of_student_sessions_attended_by_hm,
     h.no_of_parent_sessions_attended_by_hm,

a.pre_start, a.pre_end, a.pre_date, a.Q4_pre__years_in_this_school, a.Q5_pre_organisation_last_year, a.Q5_pre_option_1, a.Q5_pre_option_2,
a.Q5_pre_option_3, a.Q6_pre_heard_of_antarang, a.Q7_pre_how_did_you_hear, a.Q7a_pre_newspapers, a.Q7b_pre_social_media__facebook__linked, 
a.Q7c_pre_from_government_meetings___oth, a.Q7d_pre_from_parents__students, a.Q7e_pre_search_engines___google_etc, 
a.Q7f_pre_worked_with_them_in_the_past, a.Q7g_pre_workshops__training_sessions__, a.Q7h_pre_other__please_specify, 
a.Q7i_pre_NA, a.Q7_pre_Other_Please_Specify,  a.Q8_pre_which_grade, a.Q8a_pre_grade_8,
a.Q8b_pre_grade_9, a.Q8c_pre_grade_10, a.Q8d_pre_grade_11, a.Q8e_pre_grade_12, a.Q8f_pre_none_of_the_above, a.Q8g_pre_i_don_t_know__not_sure, 
a.Q8h_pre_other__please_specify, a.Q8_pre_Others_Please_Specify, a.Q9_pre_which_follow, a.Q9a_pre_up__to__date_information_about_careers,
a.Q9b_pre_parents__caregivers_sessions, a.Q9c_pre_a_trained_career_teacher_in_school, a.Q9d_pre_individual_counselling,
a.Q9e_pre_psychometric_tests, a.Q9f_pre_records_of_students_career_plans, a.Q9g_pre_academic_subjects_teaching_careers,
a.Q9h_pre_information_on_education___training_path, a.Q9i_pre_visits__talks_with_educational_instituti,
a.Q9j_pre_records_of_student_learning_in_the_caree, a.Q9k_pre_records_of_students_career_plans_after_c, a.Q9l_pre_i_don_t_know__not_sure,
a.Q10_pre_which_follow, a.Q10a_pre_i_had_no_role_to_play_in_the_program, a.Q10b_pre_provided_a_fixed_time_for_the_career_ses,
a.Q10c_pre_observe_career_sessions_regularly, a.Q10d_pre_ensured_students_attended_the_career_pro, a.Q10e_pre_spoke_to_parents_about_the_career_progra,
a.Q10f_pre_shared_about_the_career_program_in_hm_me, a.Q10g_pre_provided_feedback__suggestions_for_the_p, a.Q10h_pre_provided_school_budget__money_for_the_ca,
a.Q10i_pre_regularly_spoke_to_the_career_teacher_ab, a.Q10j_pre_ensured_the_program_started_and_ended_on, a.Q10k_pre_arranged_additional_career_activities_in,
a.Q10l_pre_spoke_to_my_school_alumni_to_see_how_the, a.Q10m_pre_ensured_there_is_a_trained_career_teache, a.Q10n_pre_i_do_not_know__not_sure,
a.Q11_pre_think_of_career, a.Q12_pre_confident_in_career_program, a.Q13_pre_program_activities, a.Q14_pre_program_session,
a.Q15_pre_important_secondary_students, a.Q16A_pre_program_inc_tions_all_students, a.Q16B_pre_supporting_students_training_after_school,
a.Q16C_pre_encouraging_students_choosing_careers, a.Q17_pre_one_thing_for_you_students, a.pre_id, a.pre_uuid, a.pre_submission_time,

a.po_start, a.po_end, a.po_date, a.Q4_po_heard_of_antarang, a.Q5_po_grade, a.Q5a_po_grade_8, a.Q5b_po_grade_9, a.Q5c_po_grade_10, a.Q5d_po_grade_11,
a.Q5e_po_grade_12, a.Q5f_po_none_of_the_above, a.Q5g_po_i_don_t_know__not_sure, a.Q5h_po_other__please_specify, a.Q5_po_others_please_specify,
a.Q6_po_which_follow, a.Q6a_po_i_had_no_role_to_play_in_the_program, a.Q6b_po_provided_a_fixed_time_for_the_career_ses,
a.Q6c_po_observe_career_sessions_regularly, a.Q6d_po_ensured_students_attended_the_career_pro, a.Q6e_po_spoke_to_parents_about_the_career_progra,
a.Q6f_po_shared_about_the_career_program_in_hm_me, a.Q6g_po_provided_feedback__suggestions_for_the_p, a.Q6h_po_provided_school_budget__money_for_the_ca,
a.Q6i_po_regularly_spoke_to_the_career_teacher_ab, a.Q6j_po_ensured_the_program_started_and_ended_on, a.Q6k_po_arranged_additional_career_activities_in,
a.Q6l_po_spoke_to_my_school_alumni_to_see_how_the, a.Q6m_po_ensured_there_is_a_trained_career_teache, a.Q6n_po_i_do_not_know__not_sure,
a.Q7_po_role, a.Q7a_po_i_have_no_role_to_play, a.Q7b_po_provide_a_fixed_time_for_the_career_sess, a.Q7c_po_observe_career_sessions_regularly,
a.Q7d_po_ensure_students_attended_the_career_prog, a.Q7e_po_speak_to_parents_about_the_career_progra, a.Q7f_po_share_about_the_career_program_in_hm_mee,
a.Q7g_po_provide_feedback__suggestions_for_the_pr, a.Q7h_po_provide_school_budget__money_for_the_car, a.Q7i_po_regularly_speak_to_the_career_teacher_ab,
a.Q7j_po_ensuredthe_program_starts_and_ends_on_ti, a.Q7k_po_arrange_additional_career_activities_in_, a.Q7l_po_speak_to_my_school_alumni_to_see_how_the,
a.Q7m_po_ensure_there_is_a_trained_career_teacher, a.Q7n_po_i_do_not_know__not_sure, a.Q8_po_materials, a.Q8a_po_a_student_book,
a.Q8b_po_a_teacher_guide, a.Q8c_po_school_poster, a.Q8d_po_career_chatbot, a.Q8e_po_student_certificates, a.Q8f_po_student_assessments,
a.Q8g_po_career_videos, a.Q8h_po_parent_handouts, a.Q8i_po_counselling_report, a.Q9_po_confidence, a.Q10_po_secondary_students,
a.Q11A_po_orientation_session_covered, a.Q11B_po_content_was_easy_to_understand, a.Q11C_po_encouraging_students_choosing_careers,
a.Q12_po_thin_for_students, a.Q13_po_big_rom_this_orientation, a.Q14_po_nex_after_this_orientation, a.po_id, a.po_uuid, a.po_submission_time,

a.pp_start, a.pp_end, a.pp_date, a.Q4_pp_how_many_year_in_this_school, a.Q5_pp_org_our_school_last_year, a.Q5_pp_others_please_specify,
a.Q6_pp_heard_of_antarng, a.Q7_pp_how_did_you_hear, a.Q7a_pp_Newspaper, a.Q7b_pp_Social_media_Facebook_linkedin, a.Q7c_pp_From_government_meetings__other,
a.Q7d_pp_From_Parents_Students, a.Q7e_pp_Search_Engines__Google_etc, a.Q7f_pp_Peer_HMsTeachers, a.Q7g_pp_Conducted_career_education_program_in_my_school,
a.Q7h_pp_Workshops_training_sessions_or_webinars, a.Q7i_pp_Word_of_mouth_from_alumni_or_beneficiaries, a.Q7j_pp_Antarangs_website,
a.Q7k_pp_other__please_specify, a.Q7_pp_other_please_specify, a.Q8_pp_in_which_grade, a.Q8a_pp_grade_8, a.Q8b_pp_grade_9, a.Q8c_pp_grade_10,
a.Q8d_pp_grade_11, a.Q8e_pp_grade_12, a.Q8f_pp_none_of_the_above, a.Q8g_pp_i_don_t_know__not_sure, a.Q8h_pp_other__please_specify,
a.Q8_pp_Others_Please_Specify_001, a.Q9_pp_which_of_the_following, a.Q9a_pp_Up_to_date_info_about_careers, a.Q9b_pp_Sessions_for_parents_or_caregivers,
a.Q9c_pp_Trained_external_career_teacher_at_school, a.Q9d_pp_Trained_career_teacher_in_charge, a.Q9e_pp_One_on_one_career_counselling,
a.Q9f_pp_Interest_and_aptitude_tests, a.Q9g_pp_Record_of_students_career_plans, a.Q9h_pp_Info_on_education_or_training_after_school,
a.Q9i_pp_Career_expert_talks_during_subject_classes, a.Q9j_pp_Talks_or_visits_to_colleges_and_workplaces,
a.Q9k_pp_Data_collected_on_students_plans_after_school, a.Q9l_pp_i_do_not_know__not_sure, a.Q10_pp_answer_that_apply,
a.Q10a_pp_Fixed_a_regular_time_for_career_sessions, a.Q10b_pp_Visited_the_career_sessions, a.Q10c_pp_Made_sure_students_attended,
a.Q10d_pp_Spoke_to_parents_about_it, a.Q10e_pp_Shared_updates_in_meetings, a.Q10f_pp_Gave_ideas_or_suggestions_to_the_career_teacher,
a.Q10g_pp_Shared_updates_in_meetings_or_reports, a.Q10h_pp_Utilised_school_funds_for_the_program, a.Q10i_pp_Talked_often_with_the_career_teacher,
a.Q10j_pp_Started_and_ended_the_program_on_time, a.Q10k_pp_Arranged_extra_career_activities, a.Q10l_pp_invited_ex_students_to_share_their_job_stories,
a.Q10m_pp_Made_sure_the_school_had_a_trained_career_teacher, a.Q10n_pp_i_do_not_know__not_sure, a.Q11_pp_thin_for_career_program,
a.Q12_pp_confident, a.Q13_pp_program_activities, a.Q14_pp_program_sessions, a.Q15_pp_important_secondary_students, a.Q16_pp_career_decisions,
a.Q17_pp_materials_that_apply, a.Q17a_pp_A_student_book, a.Q17b_pp_A_teacher_guide, a.Q17c_pp_School_Poster, a.Q17d_pp_Career_Chatbot,
a.Q17e_pp_Student_Certificates, a.Q17f_pp_Student_Assessments, a.Q17g_pp_Career_Videos, a.Q17h_pp_Parent_Handouts, a.Q17i_pp_Counselling_Report,
a.Q18A_pp_program_inc_of_all_students, a.Q18B_pp_supporting_students_raining_after_school, a.Q18C_pp_encouraging_students_choosing_careers,
a.Q19_pp_leave_school, a.Q20_pp_thin_for_students, a.pp_id, a.pp_uuid, a.pp_submission_time
FROM hm_session h
--FULL OUTER JOIN hm_assessment a ON a.ass_school_name = h.school_name
FULL OUTER JOIN hm_assessment_dedup a ON a.ass_school_name = h.school_name
LEFT JOIN int_global_session s ON h.hm_school_id = s.school_id
LEFT JOIN hm_orientation o ON h.school_state = o.hm_state
),

final_dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY hm_school_id
                   ORDER BY session_academic_year DESC
               ) AS rn
        FROM final
    )
    WHERE rn = 1
)

select * from final_dedup
--Where school_name in ('Bhagwan Mahavir Govt. High School, Honda', 'GHSS Satakha')
--where school_name = 'Bhagwan Mahavir Govt. High School, Honda' --'GHSS Satakha' --'Bhagwan Mahavir Govt. High School, Honda'
--where school_state = 'Nagaland'
--select count(distinct hm_school_id) AS total_school_count, SUM(CASE WHEN hm_school_id IS NULL THEN 1 ELSE 0 END) AS school_missing from final
--where hm_school_id in ('0017F00000L5VXFQA3', '0017F00000L55KjQAJ', '0019C000003geeQQAQ')
--order by hm_school_id
--where complete_session_status >=5
--where hm_school_id = '0017F00000JeL7AQAV'
--where hm_school_id = '0019C000003PjIXQA0'
--order by hm_school_id


