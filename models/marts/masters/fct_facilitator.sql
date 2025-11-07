with
stg_facilitator AS (
    SELECT facilitator_id, facilitator_name, facilitator_language, facilitator_work_status, facilitator_area, 
    facilitator_academic_year FROM {{ ref('dev_stg_facilitator') }}
),

stg_school AS (
    SELECT school_id, school_name, school_language, school_academic_year, school_state, school_district, school_ward, 
    school_taluka FROM {{ ref('dev_stg_school') }}
),

stg_donor AS (
    SELECT donor_id, donor_name FROM {{ ref('dev_stg_donor') }}
),

stg_batch AS (
    SELECT batch_no, batch_school_id, batch_academic_year, batch_grade, batch_completed, batch_language, batch_facilitator_id,
batch_donor_id, no_of_students_facilitated FROM {{ ref('dev_stg_batch') }}
),

final AS (
    SELECT facilitator_id, facilitator_name, facilitator_language, facilitator_work_status, facilitator_area, 
    facilitator_academic_year, batch_no, batch_school_id, batch_academic_year, batch_grade, batch_completed, 
    batch_language, batch_facilitator_id, no_of_students_facilitated, school_id, school_name, school_language, 
    school_academic_year, school_state, school_district, school_ward, school_taluka, donor_name
    FROM stg_batch b
    LEFT JOIN stg_facilitator f ON b.batch_facilitator_id = f.facilitator_id
    LEFT JOIN stg_school s ON b.batch_school_id = s.school_id
    LEFT JOIN stg_donor d ON b.batch_donor_id = d.donor_id
)

SELECT * FROM final
order by facilitator_name 