
WITH t1 AS (
    SELECT *
    FROM {{ ref('int_global') }}
),

t2 AS (
    SELECT
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
    FROM {{ ref('stg_somrt') }}
),

t3 AS (
    SELECT
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
    FROM {{ ref('stg_session') }}
),

t4 AS (
    SELECT *
    FROM t2
    LEFT JOIN t3
        ON t2.somrt_session_id = t3.session_id
),

t5 AS (
    SELECT *
    FROM t1
    LEFT JOIN t4
        ON t1.batch_id = t4.somrt_batch_id
),

t6 AS (
    SELECT
        batch_id,
        SUM(DISTINCT no_of_students_facilitated) AS total_students,
        COUNT(DISTINCT bl_cdm1_no) AS bl_cdm1,
        COUNT(DISTINCT el_cdm1_no) AS el_cdm1,
        COUNT(DISTINCT bl_cdm2_no) AS bl_cdm2,
        COUNT(DISTINCT el_cdm2_no) AS el_cdm2,
        COUNT(DISTINCT bl_cp_no) AS bl_cp,
        COUNT(DISTINCT el_cp_no) AS el_cp,
        COUNT(DISTINCT bl_cs_no) AS bl_cs,
        COUNT(DISTINCT el_cs_no) AS el_cs,
        COUNT(DISTINCT bl_fp_no) AS bl_fp,
        COUNT(DISTINCT el_fp_no) AS el_fp,
        COUNT(DISTINCT saf_no) AS saf,
        COUNT(DISTINCT sar_no) AS sar,
        COUNT(DISTINCT sd2_barcode) AS total_sd2_students
    FROM {{ ref('int_student_global_assessment_status') }}
    GROUP BY batch_id
),

diagnostics AS (
    SELECT
        batch_no,
        COUNT(DISTINCT bl_sd_no) AS bl_sd,
        COUNT(DISTINCT el_sd_no) AS el_sd
    FROM {{ ref('int_student_diagnostic') }}
    GROUP BY batch_no
),

cdm3 AS (
    SELECT
        batch_no,
        COUNT(DISTINCT bl_ca2_no) AS bl_ca2,
        COUNT(DISTINCT el_ca2_no) AS el_ca2
    FROM {{ ref('int_student_global_cdm2_ind') }}
    GROUP BY batch_no
),

t7 AS (
    SELECT
        t5.*,

        CASE
            WHEN t5.omr_type IN (
                'Self Awareness + Feedback',
                'Quiz + Feedback'
            ) THEN t6.saf

            WHEN t5.omr_type = 'Realities + Quiz'
                THEN t6.sar

            WHEN t5.omr_type = 'Career Decision Making- 1A'
                THEN t6.bl_cdm1
            WHEN t5.omr_type = 'Career Decision Making- 1B'
                THEN t6.el_cdm1

            WHEN t5.omr_type = 'Career Decision Making- 2A'
                THEN t6.bl_cdm2
            WHEN t5.omr_type = 'Career Decision Making- 2B'
                THEN t6.el_cdm2

            WHEN t5.omr_type = 'Career Planning A'
                THEN t6.bl_cp
            WHEN t5.omr_type = 'Career Planning B'
                THEN t6.el_cp

            WHEN t5.omr_type = 'Career Skills A'
                THEN t6.bl_cs
            WHEN t5.omr_type = 'Career Skills B'
                THEN t6.el_cs

            WHEN t5.omr_type = 'Planning for Future A'
                THEN t6.bl_fp
            WHEN t5.omr_type = 'Planning for Future B'
                THEN t6.el_fp

            WHEN t5.omr_type = 'Student Details'
                THEN t6.total_students

            WHEN t5.omr_type = 'Career Decision Making- 3A'
                THEN cdm3.bl_ca2
            WHEN t5.omr_type = 'Career Decision Making- 3B'
                THEN cdm3.el_ca2

            WHEN t5.omr_type IN (
                'Student Diagnostics A',
                'Student Diagnositcs A'
            ) THEN diagnostics.bl_sd

            WHEN t5.omr_type = 'Student Diagnositcs B'
                THEN diagnostics.el_sd

            WHEN t5.omr_type = 'Student Details 2'
                THEN t6.total_sd2_students
        END AS omr_upload_count

    FROM t5
    INNER JOIN t6
        ON t5.batch_id = t6.batch_id
    LEFT JOIN diagnostics
        ON t5.batch_no = diagnostics.batch_no
    LEFT JOIN cdm3
        ON t5.batch_no = cdm3.batch_no
),

t8 AS (
    SELECT
        t7.*,

        -- TAT1: Session to OMR received
        CASE
            WHEN session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN
                CASE
                    WHEN COALESCE(
                        omr_received_date,
                        CURRENT_DATE('Asia/Kolkata')
                    ) >= session_date
                    THEN (
                        SELECT COUNTIF(
                            EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)
                        )
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                DATE_ADD(session_date, INTERVAL 1 DAY),
                                COALESCE(
                                    omr_received_date,
                                    CURRENT_DATE('Asia/Kolkata')
                                )
                            )
                        ) AS d
                    )
                    ELSE -(
                        SELECT COUNTIF(
                            EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)
                        )
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                DATE_ADD(
                                    COALESCE(
                                        omr_received_date,
                                        CURRENT_DATE('Asia/Kolkata')
                                    ),
                                    INTERVAL 1 DAY
                                ),
                                session_date
                            )
                        ) AS d
                    )
                END
        END AS TAT1,

        -- TAT2: OMR received to first upload
        CASE
            WHEN omr_received_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN
                CASE
                    WHEN COALESCE(
                        first_omr_upload_date,
                        CURRENT_DATE('Asia/Kolkata')
                    ) >= omr_received_date
                    THEN (
                        SELECT COUNTIF(
                            EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)
                        )
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                DATE_ADD(
                                    omr_received_date,
                                    INTERVAL 1 DAY
                                ),
                                COALESCE(
                                    first_omr_upload_date,
                                    CURRENT_DATE('Asia/Kolkata')
                                )
                            )
                        ) AS d
                    )
                    ELSE -(
                        SELECT COUNTIF(
                            EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)
                        )
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                DATE_ADD(
                                    COALESCE(
                                        first_omr_upload_date,
                                        CURRENT_DATE('Asia/Kolkata')
                                    ),
                                    INTERVAL 1 DAY
                                ),
                                omr_received_date
                            )
                        ) AS d
                    )
                END
        END AS TAT2,

        -- TAT3: Session to first upload
        CASE
            WHEN session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN
                CASE
                    WHEN COALESCE(
                        first_omr_upload_date,
                        CURRENT_DATE('Asia/Kolkata')
                    ) >= session_date
                    THEN (
                        SELECT COUNTIF(
                            EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)
                        )
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                DATE_ADD(session_date, INTERVAL 1 DAY),
                                COALESCE(
                                    first_omr_upload_date,
                                    CURRENT_DATE('Asia/Kolkata')
                                )
                            )
                        ) AS d
                    )
                    ELSE -(
                        SELECT COUNTIF(
                            EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)
                        )
                        FROM UNNEST(
                            GENERATE_DATE_ARRAY(
                                DATE_ADD(
                                    COALESCE(
                                        first_omr_upload_date,
                                        CURRENT_DATE('Asia/Kolkata')
                                    ),
                                    INTERVAL 1 DAY
                                ),
                                session_date
                            )
                        ) AS d
                    )
                END
        END AS TAT3

    FROM t7
),

-- Aggregate student-level assessment numbers. These arrays preserve every
-- non-NULL number for a batch rather than treating counts as assessment numbers.
assessment_status_numbers AS (
    SELECT
        batch_id,
        ARRAY_AGG(DISTINCT CAST(bl_cdm2_no AS STRING) IGNORE NULLS) AS bl_cdm2_numbers,
        ARRAY_AGG(DISTINCT CAST(el_cdm2_no AS STRING) IGNORE NULLS) AS el_cdm2_numbers,
        ARRAY_AGG(DISTINCT CAST(bl_cp_no AS STRING) IGNORE NULLS) AS bl_cp_numbers,
        ARRAY_AGG(DISTINCT CAST(el_cp_no AS STRING) IGNORE NULLS) AS el_cp_numbers,
        ARRAY_AGG(DISTINCT CAST(bl_cs_no AS STRING) IGNORE NULLS) AS bl_cs_numbers,
        ARRAY_AGG(DISTINCT CAST(el_cs_no AS STRING) IGNORE NULLS) AS el_cs_numbers,
        ARRAY_AGG(DISTINCT CAST(el_fp_no AS STRING) IGNORE NULLS) AS el_fp_numbers
    FROM {{ ref('int_student_global_assessment_status') }}
    GROUP BY batch_id
),

diagnostic_numbers AS (
    SELECT
        batch_no,
        ARRAY_AGG(DISTINCT CAST(bl_sd_no AS STRING) IGNORE NULLS) AS bl_sd_numbers,
        ARRAY_AGG(DISTINCT CAST(el_sd_no AS STRING) IGNORE NULLS) AS el_sd_numbers
    FROM {{ ref('int_student_diagnostic') }}
    GROUP BY batch_no
),

ca1_numbers AS (
    SELECT
        batch_no,
        ARRAY_AGG(DISTINCT CAST(bl_ca1_no AS STRING) IGNORE NULLS) AS bl_ca1_numbers,
        ARRAY_AGG(DISTINCT CAST(el_ca1_no AS STRING) IGNORE NULLS) AS el_ca1_numbers
    FROM {{ ref('fct_ca') }}
    GROUP BY batch_no
),

ca2_numbers AS (
    SELECT
        batch_no,
        ARRAY_AGG(DISTINCT CAST(bl_ca2_no AS STRING) IGNORE NULLS) AS bl_ca2_numbers,
        ARRAY_AGG(DISTINCT CAST(el_ca2_no AS STRING) IGNORE NULLS) AS el_ca2_numbers
    FROM {{ ref('fct_student_global_cdm2_ind') }}
    GROUP BY batch_no
),

partner_source AS (
    SELECT
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
        t6.total_students AS partner_total_students,
        t6.total_sd2_students AS partner_total_sd2_students
    FROM t8
    LEFT JOIN t6
        ON t8.batch_id = t6.batch_id
    LEFT JOIN assessment_status_numbers s
        ON t8.batch_id = s.batch_id
    LEFT JOIN diagnostic_numbers d
        ON t8.batch_no = d.batch_no
    LEFT JOIN ca1_numbers ca1
        ON t8.batch_no = ca1.batch_no
    LEFT JOIN ca2_numbers ca2
        ON t8.batch_no = ca2.batch_no
    WHERE t8.school_partner IN (
        'Akanksha Foundation',
        'Learning Links Foundation',
        'Masoom',
        'KMCT',
        'Educate Girls'
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t8.batch_id
        ORDER BY t8.omr_type
    ) = 1
),

-- Expand each batch-level array into one row per non-NULL assessment number.
partner_assessments AS (
    SELECT
        p.*,
        a.assessment_type AS generated_assessment_type,
        a.assessment_object AS generated_assessment_object,
        a.record_type AS generated_record_type,
        ARRAY_LENGTH(a.assessment_numbers) AS generated_assessment_count
    FROM partner_source p
    CROSS JOIN UNNEST([
        STRUCT('Student Details' AS assessment_type, 'Demographic' AS assessment_object, CAST(NULL AS STRING) AS record_type,
              ARRAY(SELECT CAST(n AS STRING) FROM UNNEST(GENERATE_ARRAY(1, GREATEST(COALESCE(p.partner_total_students, 0), 0))) AS n) AS assessment_numbers),
        STRUCT('Student Details 2', 'Student Details 2', CAST(NULL AS STRING),
              ARRAY(SELECT CAST(n AS STRING) FROM UNNEST(GENERATE_ARRAY(1, GREATEST(COALESCE(p.partner_total_sd2_students, 0), 0))) AS n)),
        STRUCT('Career Planning A', 'Career Planning', 'Baseline', p.bl_cp_numbers),
        STRUCT('Career Planning B', 'Career Planning', 'Endline', p.el_cp_numbers),
        STRUCT('Career Skills A', 'Career Skill', 'Baseline', p.bl_cs_numbers),
        STRUCT('Career Skills B', 'Career Skill', 'Endline', p.el_cs_numbers),
        STRUCT('Career Awareness Part 2A', 'CDM2', 'Baseline', p.bl_cdm2_numbers),
        STRUCT('Career Awareness Part 2B', 'CDM2', 'Endline', p.el_cdm2_numbers),
        STRUCT('Career Awareness A', 'CDM2', 'Baseline', p.bl_ca1_numbers),
        STRUCT('Career Awareness B', 'CDM2', 'Endline', p.el_ca1_numbers),
        STRUCT('Career Awareness Part 1A', 'CDM3', 'Baseline', p.bl_ca2_numbers),
        STRUCT('Career Awareness Part 1B', 'CDM3', 'Endline', p.el_ca2_numbers),
        STRUCT('Student Diagnostics A', 'Student Diagnostic', 'Baseline', p.bl_sd_numbers),
        STRUCT('Student Diagnositcs B', 'Student Diagnostic', 'Endline', p.el_sd_numbers),
        STRUCT('Feedback', 'Future Planning', 'Endline', p.el_fp_numbers)
    ]) a
    WHERE ARRAY_LENGTH(a.assessment_numbers) > 0
),

partner_upload_counts AS (
    SELECT
        batch_id,
        generated_assessment_type,
        MAX(generated_assessment_count) AS upload_count
    FROM partner_assessments
    GROUP BY batch_id, generated_assessment_type
),

global_output AS (
    SELECT *
    FROM t8
    WHERE school_partner NOT IN (
        'Akanksha Foundation',
        'Learning Links Foundation',
        'Masoom',
        'KMCT',
        'Educate Girls'
    )
),

partner_output AS (
    SELECT
        p.* EXCEPT (
            somrt_id, somrt_no, somrt_session_id, somrt_batch_id, somrt_batch_no,
            omr_type, omr_assessment_object, omr_assessment_count,
            omr_assessment_record_type, omr_received_count, omr_received_by,
            number_of_students_in_batch, omr_received_date, first_omr_upload_date,
            session_id, session_facilitator_id, omr_required, omrs_received,
            total_student_present, total_parent_present, attendance_count,
            session_name, session_date, present_count, omr_upload_count,
            TAT1, TAT2, TAT3,
            bl_cdm2_numbers, el_cdm2_numbers, bl_cp_numbers, el_cp_numbers,
            bl_cs_numbers, el_cs_numbers, el_fp_numbers,
            bl_sd_numbers, el_sd_numbers, bl_ca1_numbers, el_ca1_numbers,
            bl_ca2_numbers, el_ca2_numbers, partner_total_students, partner_total_sd2_students,
            generated_assessment_type, generated_assessment_object,
            generated_record_type, generated_assessment_count
        ),
        CAST(NULL AS STRING) AS somrt_id,
        CAST(NULL AS STRING) AS somrt_no,
        CAST(NULL AS STRING) AS somrt_session_id,
        CAST(NULL AS STRING) AS somrt_batch_id,
        CAST(NULL AS STRING) AS somrt_batch_no,
        p.generated_assessment_type AS omr_type,
        p.generated_assessment_object AS omr_assessment_object,
        p.generated_assessment_count AS omr_assessment_count,
        p.generated_record_type AS omr_assessment_record_type,
        CAST(NULL AS INT64) AS omr_received_count,
        CAST(NULL AS STRING) AS omr_received_by,
        CAST(NULL AS INT64) AS number_of_students_in_batch,
        CAST(NULL AS DATE) AS omr_received_date,
        CAST(NULL AS DATE) AS first_omr_upload_date,
        CAST(NULL AS STRING) AS session_id,
        CAST(NULL AS STRING) AS session_facilitator_id,
        CAST(NULL AS BOOL) AS omr_required,
        CAST(NULL AS STRING) AS omrs_received,
        CAST(NULL AS INT64) AS total_student_present,
        CAST(NULL AS INT64) AS total_parent_present,
        CAST(NULL AS INT64) AS attendance_count,
        CAST(NULL AS STRING) AS session_name,
        CAST(NULL AS DATE) AS session_date,
        CAST(NULL AS INT64) AS present_count,
        COALESCE(u.upload_count, 0) AS omr_upload_count,
        CAST(NULL AS INT64) AS TAT1,
        CAST(NULL AS INT64) AS TAT2,
        CAST(NULL AS INT64) AS TAT3
    FROM partner_assessments p
    LEFT JOIN partner_upload_counts u
        ON p.batch_id = u.batch_id
        AND p.generated_assessment_type = u.generated_assessment_type
),

final AS (
    SELECT * FROM global_output
    UNION ALL BY NAME
    SELECT * FROM partner_output
)

SELECT * FROM final