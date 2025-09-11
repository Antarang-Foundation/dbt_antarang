WITH post_orientation AS (
    SELECT contact_number,
           ques_date,
           name,
           secondary_students,
           program_inc_student,
           encouraging_students_choosing_careers,
           support_students_raining_after_school,
           thin_for_students,
           big_rom_this_orientation,
           nex_after_this_orientation,
           district_name,
           q5_grade,
           q6,
           q7_role,
           q8_materials,
           q9_confidence,
           version,
           attachments,
           geolocation,
           id,
           notes,
           status,
           submission_time,
           submitted_by,
           tags,
           uuid,
           validation_status,
           ques_end,
           school_name AS post_ori_school_name,
           ques_start,
           ROW_NUMBER() OVER (
               PARTITION BY contact_number, school_name
               ORDER BY ques_date DESC
           ) AS rn
    FROM {{ source('kobo', 'Post_Orientation_Questionaire_form') }}
),
post_program_questionnaire AS (
    SELECT contact_number,
           post_date,
           name,
           q10_answer_that_apply,
           q11_thin_for_career_program,
           q12_confident,
           q13_program_activities,
           q14_program_sessions,
           q15_important_secondary_students,
           q16_career_decisions,
           q17_materials_that_apply,
           program_inc_of_all_students,
           encouraging_students_choosing_careers,
           supporting_students_raining_after_school,
           q19_leave_school,
           q20_thin_for_students,
           district_name,
           how_many_year_in_this_school,
           org_our_school_last_year,
           how_did_you_hear,
           in_which_grade,
           q9_which_of_the_following,
           version,
           geolocation,
           notes,
           status,
           submission_time,
           submitted_by,
           tags,
           uuid,
           post_end,
           post_start,
           school_Rajasthan AS post_prog_school_Rajasthan,
           school_Mumbai AS post_prog_school_Mumbai,
           school_Goa AS post_prog_school_Goa,
           school_name AS post_prog_school_name,
           ROW_NUMBER() OVER (
               PARTITION BY contact_number, school_name
               ORDER BY post_date DESC
           ) AS rn
    FROM {{ source('kobo', 'Post_Program_Questionaire_form') }}
),
pre_program_orientation AS (
    SELECT contact_number,
           name,
           district_name,
           Q4_years_in_this_school,
           Q6_heard_of_antarang,
           Q8_which_grade,
           Q9,
           Q10,
           Q11_think_of_career,
           Q12_confident_in_career_program,
           Q13_program_activities,
           Q14_program_session,
           Q15_important_secondary_students,
           Q16program_inclution_of_all_students,
           Q16_encouraging_students_choosing_careers,
           Q16_supporting_students_training_after_school,
           Q19_one_thing_for_you_students,
           version,
           attachments,
           geolocation,
           id,
           notes,
           ststus,
           submission_time,
           submitted_by,
           uuid,
           pre_end,
           pre_start,
           school_Rajasthan AS pre_school_Rajasthan,
           school_Mumbai AS pre_school_Mumbai,
           school_Goa AS pre_school_Goa,
           school_name AS pre_school_name
    FROM {{ source('kobo', 'Pre_Program_Questionaire_form') }}
),
int_global_dcp AS (
    SELECT batch_language, 
           school_name, 
           school_taluka, 
           school_district, 
           school_state, 
           batch_donor, 
           school_area,
           school_partner,
           school_id,
           facilitator_name
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY school_id 
                   ORDER BY school_name DESC
               ) AS rn
        FROM {{ ref('dev_int_global_dcp') }}
    )
    WHERE rn = 1
),
hm_session_agg AS (
    SELECT
        hm_school_id,
        STRING_AGG(hm_session_name, ', ') AS all_session_names,
        STRING_AGG(CAST(hm_session_date AS STRING), ', ') AS all_session_dates,
        COUNT(*) AS total_sessions
    FROM {{ ref('dev_stg_hm_session') }}
    GROUP BY hm_school_id
)

SELECT
    pre.*,
    post_ori.ques_date AS post_ori_ques_date,
    post_prog.post_date AS post_prog_date,
    dcp.*,
    hm.*
FROM pre_program_orientation AS pre
LEFT JOIN post_orientation AS post_ori
    ON pre.contact_number = post_ori.contact_number
    AND pre.pre_school_name = post_ori.post_ori_school_name
    AND post_ori.rn = 1
LEFT JOIN post_program_questionnaire AS post_prog
    ON pre.contact_number = post_prog.contact_number
    AND pre.pre_school_name = post_prog.post_prog_school_name
    AND post_prog.rn = 1
LEFT JOIN int_global_dcp AS dcp
    ON pre.pre_school_name = dcp.school_name
LEFT JOIN hm_session_agg AS hm
    ON dcp.school_id = hm.hm_school_id