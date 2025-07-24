with 
post_program_orientation AS 
(Select start_, end_, date_ as post_date , name, school_name, district_name, contact_number, heard_of_antarang, program_grades, other_specify, 
Q6, Q7_role, Q8_materials_provided, Q9_confidence_level, Q10_importance, Q11_issues, Q11_issues_header, instruct_all_students, 
program_supports_training, program_encourage_students, student_outcome_goal, biggest_learning, next_step_after_orientation 
FROM {{ source('kobo', 'Post_Program_Orientation_form_table') }}
),

Post_Program_Questionare AS
(Select start_, end_, date_ as ques_date, name, school_name, district_name, contact_number, years_in_school, career_program_organisation, 
other_organisation_specify, heard_of_antarang, Q7_Hear_Newspaper, Q7_Hear_Social_Media, Q7_Hear_Facebook, Q7_Hear_LinkedIn,
Q7_Hear_Instagram, Q7_Hear_Govt_Meetings, Q7_Hear_Other_NGOs, Q7_Hear_From_Parents, Q7_Hear_From_Students, Q7_Hear_Search_Engines,
Q7_Hear_Google, Q7_Hear_Peer_HMs, Q7_Hear_Teachers, Q7_Hear_Career_Ed_Program, Q7_Hear_Workshops, Q7_Hear_Training_Sessions,
Q7_Hear_Webinars, Q7_Hear_Alumni_WordOfMouth, Q7_Hear_Beneficiaries_WordOfMouth, Q7_Hear_Antarang_Website, heard_other_specify, 
program_grades, program_grades_other_specify, program_components, hm_roles, program_goal, confidence_in_communication, 
planning_activities, observation_frequency, importance_of_program, belief_on_career_decisions, materials_provided, agree_with_issues_summary,
issues_header_text, program_broadens_horizons, program_supports_transition, program_breaks_stereotypes, track_students_post_school, 
student_outcome_goal FROM {{ source('kobo', 'Post_Program_Questionare_table') }}
),

pre_program_orientation AS
(Select start_, end_, date_ as pre_date, Q1_name, Q2_school_name, Q3_district_name, contact_number, Q4_years_in_school, 
Q5_last_year_career_program_organization, career_program_org_2, career_program_org_3, career_program_org_4, 
Q6_heard_of_antarang, Q7_how_did_you_hear_about_antarang, Q7_heard_antarang_newspapers, Q7_heard_antarang_social_media, 
Q7_heard_antarang_parents_students, Q7_heard_antarang_search_engine, Q7_heard_antarang_past, Q7_heard_antarang_workshops, 
Q7_heard_antarang_other, Q7_other_specify, Q8_career_education_grade_last_year, Q8_program_grade_8, Q8_program_grade_9, 
Q8_program_grade_10, Q8_program_grade_11, Q8_program_grade_12, Q8_program_grade_none, Q8_program_grade_other, 
program_grade_other_specify, Q9_program_components_summary, Q9_component_info_careers, Q9_component_parent_sessions,
Q9_component_trained_teacher, Q9_component_individual_counselling, Q9_component_psychometric_tests, Q9_component_student_career_records,
Q9_component_subjects_teaching_careers, Q9_component_education_pathways, Q9_component_visits_talks, Q9_component_learning_records,
Q9_component_post_school_plans, Q10_hm_role_summary, Q10_hm_role_no_role, Q10_hm_role_fixed_time, Q10_hm_role_observed, Q10_hm_role_ensured_attendance,
Q10_hm_role_spoke_parents, Q10_hm_role_shared_in_meetings, Q10_hm_role_feedback, Q10_hm_role_budget, Q10_hm_role_regular_checkin,
Q10_hm_role_timeline, Q10_hm_role_extra_activities, Q10_hm_role_alumni, Q10_hm_role_trained_teacher, Q10_hm_role_not_sure,
Q11_career_program_goal, Q12_confidence_in_communication, Q13_planning_activities, Q14_session_observation_frequency,
Q15_importance_rating, program_broadens_aspirations, program_supports_transition, program_gender_equality, Q17_student_outcome_goal,
id, uuid, submission_time FROM {{ source('kobo', 'Pre_Program_Orientation_form_table') }}
),

int_global_dcp as 
(select batch_academic_year, batch_language, school_name, school_taluka, school_district, school_state, batch_donor, school_area,
school_partner, school_id from {{ ref('dev_int_global_dcp') }}
),

hm_session as 
(select hm_session_name, facilitator_name, hm_session_date, start_time, scheduling_type, rescheduled_counter, session_status, 
hm_attended, session_lead, session_academic_year, hm_school_id from {{ ref('dev_stg_hm_session') }}
)

SELECT 
  pre.pre_date, post.post_date, ques.ques_date,
  pre.Q1_name AS respondent_name,
  pre.Q2_school_name AS school_name,
  pre.Q3_district_name AS district,
  pre.contact_number, pre.Q11_career_program_goal AS pre_goal,
  ques.program_goal AS ques_goal,
  post.student_outcome_goal AS post_outcome,
  ques.student_outcome_goal AS ques_outcome, 
  dcp.batch_academic_year, dcp.batch_language, dcp.school_state, dcp.batch_donor,
  hms.hm_session_name, hms.hm_session_date, hms.facilitator_name
FROM pre_program_orientation pre
INNER JOIN post_program_orientation post
  ON pre.Q2_school_name = post.school_name 
  AND pre.contact_number = post.contact_number

INNER JOIN Post_Program_Questionare ques
  ON pre.Q2_school_name = ques.school_name 
  AND pre.contact_number = ques.contact_number

INNER JOIN int_global_dcp dcp
  ON pre.Q2_school_name = dcp.school_name 
  AND pre.Q3_district_name = dcp.school_district

INNER JOIN hm_session hms
  ON dcp.school_id = hms.hm_school_id

