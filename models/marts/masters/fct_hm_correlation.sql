WITH base AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY hm_id
            ORDER BY hm_session_date DESC
        ) AS rn_last
    FROM {{ ref('dev_int_hm_session') }}
),

hm_session AS (
    SELECT 
        hm_school_id, facilitator_name, session_academic_year, batch_language, school_name,
        school_taluka, school_district, school_state, school_area, school_partner, 

        COUNT(DISTINCT hm_id) AS total_hm_session_name,
    
        COUNT(DISTINCT CASE WHEN hm_session_date IS NOT NULL THEN hm_session_date END) AS total_hm_sessions_date,

        COUNT(DISTINCT CASE WHEN LOWER(TRIM(session_status)) IN ('complete', 'completed') THEN hm_id END) AS complete_session_status,
    
        --COUNT(DISTINCT CASE WHEN LOWER(TRIM(session_status)) IN ('complete', 'completed') THEN hm_id END) AS year_end_sessions_completed,

        CASE 
        WHEN COUNT(DISTINCT CASE 
                WHEN LOWER(TRIM(session_status)) IN ('complete','completed')
                THEN hm_id END
        ) = 5
        THEN 1 
        ELSE 0 
    END AS year_end_sessions_completed,

        SUM(CASE WHEN LOWER(TRIM(hm_attended)) = 'yes' THEN 1 ELSE 0 END) AS total_hm_attended,
        COUNT(*) AS total_sessions,
        batch_expected_sessions,
        SUM(CASE WHEN hm_session_date IS NOT NULL 
                  AND total_student_present IS NOT NULL 
                  AND total_parent_present IS NOT NULL THEN 1 ELSE 0 END) AS completed_sessions,

        --COUNT(CASE WHEN LOWER(TRIM(session_type)) = 'student' THEN 1 END) 
        total_student_present AS expected_student_sessions,

        --COUNT(CASE WHEN LOWER(TRIM(session_type)) = 'parent' THEN 1 END) 
        total_parent_present AS expected_parent_sessions,

        COUNT(CASE WHEN LOWER(TRIM(session_type)) = 'student' 
                  AND hm_session_date IS NOT NULL 
                  AND total_student_present IS NOT NULL 
                  --AND total_parent_present IS NOT NULL need to verify
                  THEN 1 ELSE 0 END) AS student_sessions_attended_by_hm,

        COUNT(CASE WHEN LOWER(TRIM(session_type)) = 'parent' 
                  AND hm_session_date IS NOT NULL 
                  AND total_student_present IS NOT NULL 
                  AND total_parent_present IS NOT NULL THEN 1 ELSE 0 END) AS parent_sessions_attended_by_hm

    FROM base
    where rn_last = 1
    --where hm_school_id = '0019C000003PjVdQAK'
    --where hm_school_id IN  ('0019C000003PjVdQAK', '0019C000003PjIXQA0') and session_type = 'Student'
    GROUP BY hm_school_id, facilitator_name,session_academic_year, batch_language, school_name,
        school_taluka, school_district, school_state, school_area, school_partner, batch_expected_sessions, total_student_present,
        total_parent_present
),

school_completion AS (
    SELECT 
        hm_school_id, facilitator_name, session_academic_year, batch_language, school_name,
        school_taluka, school_district, school_state, school_area, school_partner, total_hm_session_name, total_hm_sessions_date, 
        complete_session_status, batch_expected_sessions,year_end_sessions_completed, total_hm_attended, expected_student_sessions, 
        expected_parent_sessions, student_sessions_attended_by_hm, parent_sessions_attended_by_hm, completed_sessions, 
        total_sessions,

        -- ✅ Updated completion condition:
        CASE 
            WHEN completed_sessions = total_sessions THEN 'Yes'
            ELSE 'No'
        END AS school_completion_status
    FROM hm_session
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
    WHERE rn_pre = 1
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
        COALESCE(school_name, school_name, school_name) AS ass_school_name,

pre_start, pre_end, pre_date, Q4_pre__years_in_this_school, Q5_pre_organisation_last_year, Q5_pre_option_1, Q5_pre_option_2,
Q5_pre_option_3, Q6_pre_heard_of_antarang, Q7_pre_how_did_you_hear, Q7a_pre_newspapers, Q7b_pre_social_media__facebook__linked, 
Q7c_pre_from_government_meetings___oth, Q7d_pre_from_parents__students, Q7e_pre_search_engines___google_etc, 
Q7f_pre_worked_with_them_in_the_past, Q7g_pre_workshops__training_sessions__, Q7h_pre_other__please_specify, 
Q7i_pre_NA, Q7_pre_Other_Please_Specify,  Q8_pre_which_grade, Q8a_pre_grade_8,
Q8b_pre_grade_9, Q8c_pre_grade_10, Q8d_pre_grade_11, Q8e_pre_grade_12, Q8f_pre_none_of_the_above, Q8g_pre_i_don_t_know__not_sure, 
Q8h_pre_other__please_specify, Q8_pre_Others_Please_Specify, Q9_pre_which_follow, Q9a_pre_up__to__date_information_about_careers,
Q9b_pre_parents__caregivers_sessions, Q9c_pre_a_trained_career_teacher_in_school, Q9d_pre_individual_counselling,
Q9e_pre_psychometric_tests, Q9f_pre_records_of_students_career_plans, Q9g_pre_academic_subjects_teaching_careers,
Q9h_pre_information_on_education___training_path, Q9i_pre_visits__talks_with_educational_instituti,
Q9j_pre_records_of_student_learning_in_the_caree, Q9k_pre_records_of_students_career_plans_after_c, Q9l_pre_i_don_t_know__not_sure,
Q10_pre_which_follow, Q10a_pre_i_had_no_role_to_play_in_the_program, Q10b_pre_provided_a_fixed_time_for_the_career_ses,
Q10c_pre_observe_career_sessions_regularly, Q10d_pre_ensured_students_attended_the_career_pro, Q10e_pre_spoke_to_parents_about_the_career_progra,
Q10f_pre_shared_about_the_career_program_in_hm_me, Q10g_pre_provided_feedback__suggestions_for_the_p, Q10h_pre_provided_school_budget__money_for_the_ca,
Q10i_pre_regularly_spoke_to_the_career_teacher_ab, Q10j_pre_ensured_the_program_started_and_ended_on, Q10k_pre_arranged_additional_career_activities_in,
Q10l_pre_spoke_to_my_school_alumni_to_see_how_the, Q10m_pre_ensured_there_is_a_trained_career_teache, Q10n_pre_i_do_not_know__not_sure,
Q11_pre_think_of_career, Q12_pre_confident_in_career_program, Q13_pre_program_activities, Q14_pre_program_session,
Q15_pre_important_secondary_students, Q16A_pre_program_inc_tions_all_students, Q16B_pre_supporting_students_training_after_school,
Q16C_pre_encouraging_students_choosing_careers, Q17_pre_one_thing_for_you_students, pre_id, pre_uuid, pre_submission_time,

po_start, po_end, po_date, Q4_po_heard_of_antarang, Q5_po_grade, Q5a_po_grade_8, Q5b_po_grade_9, Q5c_po_grade_10, Q5d_po_grade_11,
Q5e_po_grade_12, Q5f_po_none_of_the_above, Q5g_po_i_don_t_know__not_sure, Q5h_po_other__please_specify, Q5_po_others_please_specify,
Q6_po_which_follow, Q6a_po_i_had_no_role_to_play_in_the_program, Q6b_po_provided_a_fixed_time_for_the_career_ses,
Q6c_po_observe_career_sessions_regularly, Q6d_po_ensured_students_attended_the_career_pro, Q6e_po_spoke_to_parents_about_the_career_progra,
Q6f_po_shared_about_the_career_program_in_hm_me, Q6g_po_provided_feedback__suggestions_for_the_p, Q6h_po_provided_school_budget__money_for_the_ca,
Q6i_po_regularly_spoke_to_the_career_teacher_ab, Q6j_po_ensured_the_program_started_and_ended_on, Q6k_po_arranged_additional_career_activities_in,
Q6l_po_spoke_to_my_school_alumni_to_see_how_the, Q6m_po_ensured_there_is_a_trained_career_teache, Q6n_po_i_do_not_know__not_sure,
Q7_po_role, Q7a_po_i_have_no_role_to_play, Q7b_po_provide_a_fixed_time_for_the_career_sess, Q7c_po_observe_career_sessions_regularly,
Q7d_po_ensure_students_attended_the_career_prog, Q7e_po_speak_to_parents_about_the_career_progra, Q7f_po_share_about_the_career_program_in_hm_mee,
Q7g_po_provide_feedback__suggestions_for_the_pr, Q7h_po_provide_school_budget__money_for_the_car, Q7i_po_regularly_speak_to_the_career_teacher_ab,
Q7j_po_ensuredthe_program_starts_and_ends_on_ti, Q7k_po_arrange_additional_career_activities_in_, Q7l_po_speak_to_my_school_alumni_to_see_how_the,
Q7m_po_ensure_there_is_a_trained_career_teacher, Q7n_po_i_do_not_know__not_sure, Q8_po_materials, Q8a_po_a_student_book,
Q8b_po_a_teacher_guide, Q8c_po_school_poster, Q8d_po_career_chatbot, Q8e_po_student_certificates, Q8f_po_student_assessments,
Q8g_po_career_videos, Q8h_po_parent_handouts, Q8i_po_counselling_report, Q9_po_confidence, Q10_po_secondary_students,
Q11A_po_orientation_session_covered, Q11B_po_content_was_easy_to_understand, Q11C_po_encouraging_students_choosing_careers,
Q12_po_thin_for_students, Q13_po_big_rom_this_orientation, Q14_po_nex_after_this_orientation, po_id, po_uuid, po_submission_time,

pp_start, pp_end, pp_date, Q4_pp_how_many_year_in_this_school, Q5_pp_org_our_school_last_year, Q5_pp_others_please_specify,
Q6_pp_heard_of_antarng, Q7_pp_how_did_you_hear, Q7a_pp_Newspaper, Q7b_pp_Social_media_Facebook_linkedin, Q7c_pp_From_government_meetings__other,
Q7d_pp_From_Parents_Students, Q7e_pp_Search_Engines__Google_etc, Q7f_pp_Peer_HMsTeachers, Q7g_pp_Conducted_career_education_program_in_my_school,
Q7h_pp_Workshops_training_sessions_or_webinars, Q7i_pp_Word_of_mouth_from_alumni_or_beneficiaries, Q7j_pp_Antarangs_website,
Q7k_pp_other__please_specify, Q7_pp_other_please_specify, Q8_pp_in_which_grade, Q8a_pp_grade_8, Q8b_pp_grade_9, Q8c_pp_grade_10,
Q8d_pp_grade_11, Q8e_pp_grade_12, Q8f_pp_none_of_the_above, Q8g_pp_i_don_t_know__not_sure, Q8h_pp_other__please_specify,
Q8_pp_Others_Please_Specify_001, Q9_pp_which_of_the_following, Q9a_pp_Up_to_date_info_about_careers, Q9b_pp_Sessions_for_parents_or_caregivers,
Q9c_pp_Trained_external_career_teacher_at_school, Q9d_pp_Trained_career_teacher_in_charge, Q9e_pp_One_on_one_career_counselling,
Q9f_pp_Interest_and_aptitude_tests, Q9g_pp_Record_of_students_career_plans, Q9h_pp_Info_on_education_or_training_after_school,
Q9i_pp_Career_expert_talks_during_subject_classes, Q9j_pp_Talks_or_visits_to_colleges_and_workplaces,
Q9k_pp_Data_collected_on_students_plans_after_school, Q9l_pp_i_do_not_know__not_sure, Q10_pp_answer_that_apply,
Q10a_pp_Fixed_a_regular_time_for_career_sessions, Q10b_pp_Visited_the_career_sessions, Q10c_pp_Made_sure_students_attended,
Q10d_pp_Spoke_to_parents_about_it, Q10e_pp_Shared_updates_in_meetings, Q10f_pp_Gave_ideas_or_suggestions_to_the_career_teacher,
Q10g_pp_Shared_updates_in_meetings_or_reports, Q10h_pp_Utilised_school_funds_for_the_program, Q10i_pp_Talked_often_with_the_career_teacher,
Q10j_pp_Started_and_ended_the_program_on_time, Q10k_pp_Arranged_extra_career_activities, Q10l_pp_invited_ex_students_to_share_their_job_stories,
Q10m_pp_Made_sure_the_school_had_a_trained_career_teacher, Q10n_pp_i_do_not_know__not_sure, Q11_pp_thin_for_career_program,
Q12_pp_confident, Q13_pp_program_activities, Q14_pp_program_sessions, Q15_pp_important_secondary_students, Q16_pp_career_decisions,
Q17_pp_materials_that_apply, Q17a_pp_A_student_book, Q17b_pp_A_teacher_guide, Q17c_pp_School_Poster, Q17d_pp_Career_Chatbot,
Q17e_pp_Student_Certificates, Q17f_pp_Student_Assessments, Q17g_pp_Career_Videos, Q17h_pp_Parent_Handouts, Q17i_pp_Counselling_Report,
Q18A_pp_program_inc_of_all_students, Q18B_pp_supporting_students_raining_after_school, Q18C_pp_encouraging_students_choosing_careers,
Q19_pp_leave_school, Q20_pp_thin_for_students, pp_id, pp_uuid, pp_submission_time
from {{ ref('dev_int_hm_assessment') }}
),

int_global_session AS (
    SELECT 
        MIN(CASE WHEN fac_start_date IS NOT NULL THEN fac_start_date END) AS fac_start_date,
        MIN(CASE WHEN fac_end_date IS NOT NULL THEN fac_end_date END) AS fac_end_date,
        school_id FROM {{ ref('dev_int_global_session') }}
        where batch_academic_year >= 2025
        GROUP BY school_id
),

final as (
    SELECT h.hm_school_id, h.facilitator_name, h.session_academic_year, h.batch_language, h.school_name, h.school_taluka, h.school_district, 
    h.school_state, h.school_area, h.school_partner, s.fac_start_date, s.fac_end_date, h.total_hm_session_name, 
    h.total_hm_sessions_date, h.complete_session_status, h.year_end_sessions_completed, h.total_hm_attended, 
    h.school_completion_status, h.batch_expected_sessions, h.expected_student_sessions, h.expected_parent_sessions, 
    h.student_sessions_attended_by_hm, h.parent_sessions_attended_by_hm, 


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
    FROM hm_assessment a
LEFT JOIN school_completion h ON a.ass_school_name = h.school_name
LEFT JOIN int_global_session s ON h.hm_school_id = s.school_id

)
select * from final
--where complete_session_status >=5
--where hm_school_id = '0017F00000JeL7AQAV'
--where hm_school_id = '0019C000003PjIXQA0'
--order by hm_school_id


