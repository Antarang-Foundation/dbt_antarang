WITH 
post_program_orientation AS 
(
    SELECT 
        start_ as post_start, end_ as post_end, date_ AS post_date, name as post_name, school_name, district_name, contact_number, heard_of_antarang, 
        program_grades, other_specify, Q6, Q7_role, Q8_materials_provided, Q9_confidence_level, Q10_importance, Q11_issues, 
        Q11_issues_header, instruct_all_students, program_supports_training, program_encourage_students, student_outcome_goal, 
        biggest_learning, next_step_after_orientation 
    FROM {{ source('kobo', 'Post_Program_Orientation_form_table') }}
),

Post_Program_Questionare AS
(
    SELECT 
        start_ as ques_start, end_ as ques_end, date_ AS ques_date, name as ques_name, school_name, district_name, contact_number, years_in_school, 
        career_program_organisation, other_organisation_specify, heard_of_antarang, Q7_Hear_Newspaper, Q7_Hear_Social_Media, 
        Q7_Hear_Facebook, Q7_Hear_LinkedIn, Q7_Hear_Instagram, Q7_Hear_Govt_Meetings, Q7_Hear_Other_NGOs, Q7_Hear_From_Parents, 
        Q7_Hear_From_Students, Q7_Hear_Search_Engines, Q7_Hear_Google, Q7_Hear_Peer_HMs, Q7_Hear_Teachers, Q7_Hear_Career_Ed_Program, 
        Q7_Hear_Workshops, Q7_Hear_Training_Sessions,Q7_Hear_Webinars, Q7_Hear_Alumni_WordOfMouth, Q7_Hear_Beneficiaries_WordOfMouth, 
        Q7_Hear_Antarang_Website, heard_other_specify, program_grades, program_grades_other_specify, program_components, 
        hm_roles, program_goal, confidence_in_communication, planning_activities, observation_frequency, importance_of_program, 
        belief_on_career_decisions, materials_provided, agree_with_issues_summary, issues_header_text, program_broadens_horizons, 
        program_supports_transition, program_breaks_stereotypes, track_students_post_school, student_outcome_goal 
    FROM {{ source('kobo', 'Post_Program_Questionare_table') }}
),

pre_program_orientation AS
(
    SELECT 
        start_ as pre_start, end_ as pre_end, date_ AS pre_date, Q1_name, Q2_school_name, Q3_district_name, contact_number, Q4_years_in_school, 
        Q5_last_year_career_program_organization, career_program_org_2, career_program_org_3, career_program_org_4, 
        Q6_heard_of_antarang, Q7_how_did_you_hear_about_antarang, Q7_heard_antarang_newspapers, Q7_heard_antarang_social_media, 
        Q7_heard_antarang_parents_students, Q7_heard_antarang_search_engine, Q7_heard_antarang_past, 
        Q7_heard_antarang_workshops, Q7_heard_antarang_other, Q7_other_specify, Q8_career_education_grade_last_year, 
        Q8_program_grade_8, Q8_program_grade_9, Q8_program_grade_10, Q8_program_grade_11, Q8_program_grade_12, Q8_program_grade_none, 
        Q8_program_grade_other, program_grade_other_specify, Q9_program_components_summary, Q9_component_info_careers, 
        Q9_component_parent_sessions, Q9_component_trained_teacher, Q9_component_individual_counselling, Q9_component_psychometric_tests, 
        Q9_component_student_career_records, Q9_component_subjects_teaching_careers, Q9_component_education_pathways, 
        Q9_component_visits_talks, Q9_component_learning_records, Q9_component_post_school_plans, Q10_hm_role_summary, 
        Q10_hm_role_no_role, Q10_hm_role_fixed_time, Q10_hm_role_observed, Q10_hm_role_ensured_attendance,
        Q10_hm_role_spoke_parents, Q10_hm_role_shared_in_meetings, Q10_hm_role_feedback, Q10_hm_role_budget, 
        Q10_hm_role_regular_checkin, Q10_hm_role_timeline, Q10_hm_role_extra_activities, Q10_hm_role_alumni, 
        Q10_hm_role_trained_teacher, Q10_hm_role_not_sure, Q11_career_program_goal, Q12_confidence_in_communication, 
        Q13_planning_activities, Q14_session_observation_frequency, Q15_importance_rating, program_broadens_aspirations, 
        program_supports_transition, program_gender_equality, Q17_student_outcome_goal, id, uuid, submission_time 
    FROM {{ source('kobo', 'Pre_Program_Orientation_form_table') }}
),

int_global_dcp AS 
(
    SELECT DISTINCT
        batch_academic_year, batch_language, school_name, school_taluka, school_district, school_state, batch_donor, 
        school_area, school_partner, school_id, facilitator_name
    FROM {{ ref('dev_int_global_dcp') }}
),

hm_session AS 
(
    SELECT *
    FROM (
        SELECT 
            hm_session_name, hm_facilitator_name, hm_session_date, start_time, scheduling_type, rescheduled_counter, 
            session_status, hm_attended, session_lead, session_academic_year, hm_school_id,
            ROW_NUMBER() OVER (
                PARTITION BY hm_school_id 
                ORDER BY hm_session_date DESC   
            ) as rn
        FROM {{ ref('dev_stg_hm_session') }}
    )
    WHERE rn = 1
),

assessment as (SELECT 
   -- POST PROGRAM ORIENTATION
    post.post_start,
    post.post_end,
    post.post_date,
    post.post_name,
    post.heard_of_antarang AS post_heard_of_antarang,
    post.program_grades AS post_program_grades,
    post.other_specify AS post_other_specify,
    post.Q6 AS post_Q6,
    post.Q7_role AS post_Q7_role,
    post.Q8_materials_provided AS post_Q8_materials_provided,
    post.Q9_confidence_level AS post_confidence_level,
    post.Q10_importance AS post_importance,
    post.Q11_issues AS post_Q11_issues,
    post.Q11_issues_header AS post_Q11_issues_header,
    post.instruct_all_students AS post_instruct_all_students,
    post.program_supports_training AS post_program_supports_training,
    post.program_encourage_students AS post_program_encourage_students,
    post.student_outcome_goal AS post_student_outcome_goal,
    post.biggest_learning AS post_biggest_learning,
    post.next_step_after_orientation AS post_next_step_after_orientation,

    -- QUESTIONNAIRE
    ques.ques_start,
    ques.ques_end,
    ques.ques_date,
    ques.years_in_school AS ques_years_in_school,
    ques.career_program_organisation AS ques_career_program_organisation,
    ques.other_organisation_specify AS ques_other_organisation_specify,
    ques.heard_of_antarang AS ques_heard_of_antarang,
    ques.Q7_Hear_Newspaper AS ques_Q7_Hear_Newspaper,
    ques.Q7_Hear_Social_Media AS ques_Q7_Hear_Social_Media,
    ques.Q7_Hear_Facebook AS ques_Q7_Hear_Facebook,
    ques.Q7_Hear_LinkedIn AS ques_Q7_Hear_LinkedIn,
    ques.Q7_Hear_Instagram AS ques_Q7_Hear_Instagram,
    ques.Q7_Hear_Govt_Meetings AS ques_Q7_Hear_Govt_Meetings,
    ques.Q7_Hear_Other_NGOs AS ques_Q7_Hear_Other_NGOs,
    ques.Q7_Hear_From_Parents AS ques_Q7_Hear_From_Parents,
    ques.Q7_Hear_From_Students AS ques_Q7_Hear_From_Students,
    ques.Q7_Hear_Search_Engines AS ques_Q7_Hear_Search_Engines,
    ques.Q7_Hear_Google AS ques_Q7_Hear_Google,
    ques.Q7_Hear_Peer_HMs AS ques_Q7_Hear_Peer_HMs,
    ques.Q7_Hear_Teachers AS ques_Q7_Hear_Teachers,
    ques.Q7_Hear_Career_Ed_Program AS ques_Q7_Hear_Career_Ed_Program,
    ques.Q7_Hear_Workshops AS ques_Q7_Hear_Workshops,
    ques.Q7_Hear_Training_Sessions AS ques_Q7_Hear_Training_Sessions,
    ques.Q7_Hear_Webinars AS ques_Q7_Hear_Webinars,
    ques.Q7_Hear_Alumni_WordOfMouth AS ques_Q7_Hear_Alumni_WordOfMouth,
    ques.Q7_Hear_Beneficiaries_WordOfMouth AS ques_Q7_Hear_Beneficiaries_WordOfMouth,
    ques.Q7_Hear_Antarang_Website AS ques_Q7_Hear_Antarang_Website,
    ques.heard_other_specify AS ques_heard_other_specify,
    ques.program_grades AS ques_program_grades,
    ques.program_grades_other_specify AS ques_program_grades_other_specify,
    ques.program_components AS ques_program_components,
    ques.hm_roles AS ques_hm_roles,
    ques.program_goal AS ques_program_goal,
    ques.confidence_in_communication AS ques_confidence_in_communication,
    ques.planning_activities AS ques_planning_activities,
    ques.observation_frequency AS ques_observation_frequency,
    ques.importance_of_program AS ques_importance_of_program,
    ques.belief_on_career_decisions AS ques_belief_on_career_decisions,
    ques.materials_provided AS ques_materials_provided,
    ques.agree_with_issues_summary AS ques_agree_with_issues_summary,
    ques.issues_header_text AS ques_issues_header_text,
    ques.program_broadens_horizons AS ques_program_broadens_horizons,
    ques.program_supports_transition AS ques_program_supports_transition,
    ques.program_breaks_stereotypes AS ques_program_breaks_stereotypes,
    ques.track_students_post_school AS ques_track_students_post_school,
    ques.student_outcome_goal AS ques_student_outcome_goal,

    -- PRE PROGRAM ORIENTATION
    pre.pre_start,
    pre.pre_end,
    pre.pre_date,
    pre.Q4_years_in_school AS pre_Q4_years_in_school,
    pre.Q5_last_year_career_program_organization AS pre_Q5_last_year_career_program_organization,
    pre.career_program_org_2 AS pre_career_program_org_2,
    pre.career_program_org_3 AS pre_career_program_org_3,
    pre.career_program_org_4 AS pre_career_program_org_4,
    pre.Q6_heard_of_antarang AS pre_Q6_heard_of_antarang,
    pre.Q7_how_did_you_hear_about_antarang AS pre_Q7_how_did_you_hear_about_antarang,
    pre.Q7_heard_antarang_newspapers AS pre_Q7_heard_antarang_newspapers,
    pre.Q7_heard_antarang_social_media AS pre_Q7_heard_antarang_social_media,
    pre.Q7_heard_antarang_parents_students AS pre_Q7_heard_antarang_parents_students,
    pre.Q7_heard_antarang_search_engine AS pre_Q7_heard_antarang_search_engine,
    pre.Q7_heard_antarang_past AS pre_Q7_heard_antarang_past,
    pre.Q7_heard_antarang_workshops AS pre_Q7_heard_antarang_workshops,
    pre.Q7_heard_antarang_other AS pre_Q7_heard_antarang_other,
    pre.Q7_other_specify AS pre_Q7_other_specify,
    pre.Q8_career_education_grade_last_year AS pre_Q8_career_education_grade_last_year,
    pre.Q8_program_grade_8 AS pre_Q8_program_grade_8,
    pre.Q8_program_grade_9 AS pre_Q8_program_grade_9,
    pre.Q8_program_grade_10 AS pre_Q8_program_grade_10,
    pre.Q8_program_grade_11 AS pre_Q8_program_grade_11,
    pre.Q8_program_grade_12 AS pre_Q8_program_grade_12,
    pre.Q8_program_grade_none AS pre_Q8_program_grade_none,
    pre.Q8_program_grade_other AS pre_Q8_program_grade_other,
    pre.program_grade_other_specify AS pre_program_grade_other_specify,
    pre.Q9_program_components_summary AS pre_Q9_program_components_summary,
    pre.Q9_component_info_careers AS pre_Q9_component_info_careers,
    pre.Q9_component_parent_sessions AS pre_Q9_component_parent_sessions,
    pre.Q9_component_trained_teacher AS pre_Q9_component_trained_teacher,
    pre.Q9_component_individual_counselling AS pre_Q9_component_individual_counselling,
    pre.Q9_component_psychometric_tests AS pre_Q9_component_psychometric_tests,
    pre.Q9_component_student_career_records AS pre_Q9_component_student_career_records,
    pre.Q9_component_subjects_teaching_careers AS pre_Q9_component_subjects_teaching_careers,
    pre.Q9_component_education_pathways AS pre_Q9_component_education_pathways,
    pre.Q9_component_visits_talks AS pre_Q9_component_visits_talks,
    pre.Q9_component_learning_records AS pre_Q9_component_learning_records,
    pre.Q9_component_post_school_plans AS pre_Q9_component_post_school_plans,
    pre.Q10_hm_role_summary AS pre_Q10_hm_role_summary,
    pre.Q10_hm_role_no_role AS pre_Q10_hm_role_no_role,
    pre.Q10_hm_role_fixed_time AS pre_Q10_hm_role_fixed_time,
    pre.Q10_hm_role_observed AS pre_Q10_hm_role_observed,
    pre.Q10_hm_role_ensured_attendance AS pre_Q10_hm_role_ensured_attendance,
    pre.Q10_hm_role_spoke_parents AS pre_Q10_hm_role_spoke_parents,
    pre.Q10_hm_role_shared_in_meetings AS pre_Q10_hm_role_shared_in_meetings,
    pre.Q10_hm_role_feedback AS pre_Q10_hm_role_feedback,
    pre.Q10_hm_role_budget AS pre_Q10_hm_role_budget,
    pre.Q10_hm_role_regular_checkin AS pre_Q10_hm_role_regular_checkin,
    pre.Q10_hm_role_timeline AS pre_Q10_hm_role_timeline,
    pre.Q10_hm_role_extra_activities AS pre_Q10_hm_role_extra_activities,
    pre.Q10_hm_role_alumni AS pre_Q10_hm_role_alumni,
    pre.Q10_hm_role_trained_teacher AS pre_Q10_hm_role_trained_teacher,
    pre.Q10_hm_role_not_sure AS pre_Q10_hm_role_not_sure,
    pre.Q11_career_program_goal AS pre_Q11_career_program_goal,
    pre.Q12_confidence_in_communication AS pre_confidence_in_communication,
    pre.Q13_planning_activities AS pre_planning_activities,
    pre.Q14_session_observation_frequency AS pre_observation_frequency,
    pre.Q15_importance_rating AS pre_importance_rating,
    pre.program_broadens_aspirations AS pre_program_broadens_aspirations,
    pre.program_supports_transition AS pre_program_supports_transition,
    pre.program_gender_equality AS pre_program_gender_equality,
    pre.Q17_student_outcome_goal AS pre_student_outcome_goal,
    pre.id AS pre_id,
    pre.uuid AS pre_uuid,
    pre.submission_time AS pre_submission_time,

    -- DCP
    dcp.batch_academic_year,
    dcp.batch_language,
    dcp.school_name,
    dcp.school_taluka,
    dcp.school_district,
    dcp.school_state,
    dcp.batch_donor,
    dcp.school_area,
    dcp.school_partner,
    dcp.school_id,
    dcp.facilitator_name,

    -- HM SESSION
    hms.hm_session_name,
    hms.hm_facilitator_name,
    hms.hm_session_date,
    hms.start_time,
    hms.scheduling_type,
    hms.rescheduled_counter,
    hms.session_status,
    hms.hm_attended,
    hms.session_lead,
    hms.session_academic_year

FROM pre_program_orientation pre
LEFT JOIN post_program_orientation post 
    ON pre.Q2_school_name = post.school_name 
   AND pre.contact_number = post.contact_number
LEFT JOIN Post_Program_Questionare ques 
    ON pre.Q2_school_name = ques.school_name 
   AND pre.contact_number = ques.contact_number
LEFT JOIN int_global_dcp dcp 
    ON pre.Q2_school_name = dcp.school_name 
   AND pre.Q3_district_name = dcp.school_district
LEFT JOIN hm_session hms 
    ON dcp.school_id = hms.hm_school_id
)

SELECT post_start, post_end, post_date, post_name, post_heard_of_antarang, post_program_grades, 
post_other_specify, post_Q6, post_Q7_role, post_Q8_materials_provided, post_confidence_level,
post_importance, post_Q11_issues, post_Q11_issues_header, post_instruct_all_students, 
post_program_supports_training,  post_program_encourage_students, post_student_outcome_goal, 
post_biggest_learning, post_next_step_after_orientation, 

ques_years_in_school, ques_career_program_organisation, 
ques_other_organisation_specify, ques_Q7_Hear_Newspaper, ques_Q7_Hear_Social_Media, 
ques_Q7_Hear_Facebook, ques_Q7_Hear_LinkedIn, ques_Q7_Hear_Instagram, ques_Q7_Hear_Govt_Meetings, 
ques_Q7_Hear_Other_NGOs, ques_Q7_Hear_From_Parents, ques_Q7_Hear_From_Students, ques_Q7_Hear_Search_Engines, 
ques_Q7_Hear_Google, ques_Q7_Hear_Peer_HMs, ques_Q7_Hear_Teachers, ques_Q7_Hear_Career_Ed_Program, 
ques_Q7_Hear_Workshops, ques_Q7_Hear_Training_Sessions, ques_Q7_Hear_Webinars, ques_Q7_Hear_Alumni_WordOfMouth, 
ques_Q7_Hear_Beneficiaries_WordOfMouth, ques_Q7_Hear_Antarang_Website, ques_heard_other_specify, 
ques_program_grades_other_specify, ques_program_components, ques_hm_roles, 
ques_program_goal, ques_confidence_in_communication, ques_planning_activities, ques_observation_frequency, 
ques_importance_of_program, ques_belief_on_career_decisions, ques_materials_provided, 
ques_agree_with_issues_summary, ques_issues_header_text, ques_program_broadens_horizons, 
ques_program_supports_transition, ques_program_breaks_stereotypes, ques_track_students_post_school,

pre_Q5_last_year_career_program_organization, 
pre_career_program_org_2, pre_career_program_org_3, pre_career_program_org_4, 
pre_Q7_how_did_you_hear_about_antarang, pre_Q7_heard_antarang_newspapers, pre_Q7_heard_antarang_social_media, 
pre_Q7_heard_antarang_parents_students, pre_Q7_heard_antarang_search_engine, pre_Q7_heard_antarang_past, 
pre_Q7_heard_antarang_workshops, pre_Q7_heard_antarang_other, pre_Q7_other_specify, 
pre_Q8_career_education_grade_last_year, pre_Q8_program_grade_8, pre_Q8_program_grade_9, 
pre_Q8_program_grade_10, pre_Q8_program_grade_11, pre_Q8_program_grade_12, pre_Q8_program_grade_none,  
pre_Q8_program_grade_other, pre_program_grade_other_specify, pre_Q9_program_components_summary, 
pre_Q9_component_info_careers, pre_Q9_component_parent_sessions, pre_Q9_component_trained_teacher, 
pre_Q9_component_individual_counselling, pre_Q9_component_psychometric_tests, pre_Q9_component_student_career_records, 
pre_Q9_component_subjects_teaching_careers, pre_Q9_component_education_pathways, pre_Q9_component_visits_talks, 
pre_Q9_component_learning_records, pre_Q9_component_post_school_plans, pre_Q10_hm_role_summary, 
pre_Q10_hm_role_no_role, pre_Q10_hm_role_fixed_time, pre_Q10_hm_role_observed, 
pre_Q10_hm_role_ensured_attendance, pre_Q10_hm_role_spoke_parents, pre_Q10_hm_role_shared_in_meetings, 
pre_Q10_hm_role_feedback, pre_Q10_hm_role_budget, pre_Q10_hm_role_regular_checkin, pre_Q10_hm_role_timeline, 
pre_Q10_hm_role_extra_activities, pre_Q10_hm_role_alumni, pre_Q10_hm_role_trained_teacher, 
pre_Q10_hm_role_not_sure, pre_Q11_career_program_goal, pre_importance_rating, pre_program_broadens_aspirations, 
pre_program_supports_transition, pre_program_gender_equality, pre_id, pre_uuid, pre_submission_time,

batch_academic_year, batch_language, school_name, school_taluka, school_district, school_state, batch_donor, 
school_area, school_partner, facilitator_name,

hm_session_name, hm_session_date, start_time, scheduling_type, rescheduled_counter, session_status, 
hm_attended, session_lead
FROM assessment 