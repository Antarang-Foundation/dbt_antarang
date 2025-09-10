WITH post_orientation AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY contact_number, school_name
               ORDER BY ques_date DESC
           ) AS rn
    FROM {{ source('kobo', 'Post_Orientation_Questionaire_form') }}
),
post_program_questionnaire AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY contact_number, school_name
               ORDER BY post_date DESC
           ) AS rn
    FROM {{ source('kobo', 'Post_Program_Questionaire_form') }}
),
pre_program_orientation AS (
    SELECT *
    FROM {{ source('kobo', 'Pre_Program_Questionaire_form') }}
),

int_global_dcp as (
    select 
        batch_language, 
        school_name, 
        school_taluka, 
        school_district, 
        school_state, 
        batch_donor, 
        school_area,
        school_partner,
        school_id,
        facilitator_name,
        ROW_NUMBER() OVER (
      PARTITION BY school_id 
      ORDER BY school_name DESC
        ) AS rn
    from {{ ref('dev_int_global_dcp') }}
),

hm_session_agg AS (
    SELECT
        hm_school_id,
        STRING_AGG(hm_session_name, ', ') AS all_session_names,
        STRING_AGG(CAST(hm_session_date AS STRING), ', ') AS all_session_dates
    FROM {{ ref('dev_stg_hm_session') }}
    GROUP BY hm_school_id
)

SELECT
    pre.*,
    post_ori.*,
    post_prog.*,
    dcp.*,       -- remove the rn column
    hm.* 
FROM pre_program_orientation AS pre
LEFT JOIN post_orientation AS post_ori
    ON pre.contact_number = post_ori.contact_number
    AND pre.school_name = post_ori.school_name
    AND post_ori.rn = 1

-- Latest post-program per participant
LEFT JOIN post_program_questionnaire AS post_prog
    ON pre.contact_number = post_prog.contact_number
    AND pre.school_name = post_prog.school_name
    AND post_prog.rn = 1

-- DCP info per school
LEFT JOIN int_global_dcp AS dcp
    ON pre.school_name = dcp.school_name
    AND dcp.rn = 1

-- Aggregate HM sessions
LEFT JOIN hm_session_agg AS hm
    ON dcp.school_id = hm.hm_school_id
