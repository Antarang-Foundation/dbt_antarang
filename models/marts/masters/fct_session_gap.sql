WITH holiday_calendar_raw AS (

    -- Rajasthan
    SELECT 'Rajasthan' AS geography, holiday_date
    FROM UNNEST([
        DATE '2026-04-03', DATE '2026-04-11', DATE '2026-04-14',
        DATE '2026-04-19', DATE '2026-05-28', DATE '2026-06-17',
        DATE '2026-06-26', DATE '2026-08-09', DATE '2026-08-15',
        DATE '2026-08-26', DATE '2026-08-28', DATE '2026-09-04',
        DATE '2026-08-30', DATE '2026-10-02', DATE '2026-10-11',
        DATE '2026-10-19', DATE '2026-10-20', DATE '2026-11-24',
        DATE '2026-12-25', DATE '2027-01-26', DATE '2027-03-06',
        DATE '2027-03-10', DATE '2027-03-21', DATE '2027-03-22',
        DATE '2027-03-26'
    ]) AS holiday_date

    UNION ALL

    SELECT 'Rajasthan', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-05-15', DATE '2026-06-20'
    )) AS holiday_date

    UNION ALL

    SELECT 'Rajasthan', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-11-04', DATE '2026-11-14'
    )) AS holiday_date

    UNION ALL

    SELECT 'Rajasthan', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-12-26', DATE '2027-01-05'
    )) AS holiday_date

    -- Maharashtra
    UNION ALL

    SELECT 'Maharashtra', holiday_date
    FROM UNNEST([
        DATE '2026-07-25', DATE '2026-08-15', DATE '2026-08-26',
        DATE '2026-08-28', DATE '2026-09-04', DATE '2026-09-05',
        DATE '2026-09-25', DATE '2026-10-02', DATE '2026-10-11',
        DATE '2026-10-20', DATE '2026-11-24', DATE '2026-12-04',
        DATE '2026-12-25', DATE '2027-01-01', DATE '2027-01-15',
        DATE '2027-01-26', DATE '2027-02-19', DATE '2027-03-06',
        DATE '2027-03-10', DATE '2027-03-22', DATE '2027-03-26'
    ]) AS holiday_date

    UNION ALL

    SELECT 'Maharashtra', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-09-14', DATE '2026-09-19'
    )) AS holiday_date

    UNION ALL

    SELECT 'Maharashtra', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-11-06', DATE '2026-11-18'
    )) AS holiday_date

    UNION ALL

    SELECT 'Maharashtra', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-12-25', DATE '2027-01-04'
    )) AS holiday_date

    -- Haryana
    UNION ALL

    SELECT 'Haryana', holiday_date
    FROM UNNEST([
        DATE '2026-08-15', DATE '2026-08-28', DATE '2026-09-04',
        DATE '2026-09-23', DATE '2026-10-02', DATE '2026-10-20',
        DATE '2026-10-26', DATE '2026-11-24', DATE '2026-12-25',
        DATE '2027-01-26', DATE '2027-02-26'
    ]) AS holiday_date

    UNION ALL

    SELECT 'Haryana', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-11-07', DATE '2026-11-13'
    )) AS holiday_date

    UNION ALL

    SELECT 'Haryana', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2027-01-01', DATE '2027-01-17'
    )) AS holiday_date

    -- Goa
    UNION ALL

    SELECT 'Goa', holiday_date
    FROM UNNEST([
        DATE '2026-08-15', DATE '2026-08-26', DATE '2026-10-02',
        DATE '2026-10-20', DATE '2026-12-03', DATE '2026-12-19',
        DATE '2027-01-26', DATE '2027-03-10', DATE '2027-03-22'
    ]) AS holiday_date

    UNION ALL

    SELECT 'Goa', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-09-14', DATE '2026-09-19'
    )) AS holiday_date

    UNION ALL

    SELECT 'Goa', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-11-02', DATE '2026-11-21'
    )) AS holiday_date

    UNION ALL

    SELECT 'Goa', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-12-24', DATE '2027-01-02'
    )) AS holiday_date

    -- Nagaland
    UNION ALL

    SELECT 'Nagaland', holiday_date
    FROM UNNEST([
        DATE '2026-01-01', DATE '2026-01-26', DATE '2026-03-04',
        DATE '2026-03-21', DATE '2026-05-28', DATE '2026-08-15',
        DATE '2026-08-26', DATE '2026-09-04', DATE '2026-10-02',
        DATE '2026-11-08', DATE '2026-11-24', DATE '2026-12-01'
    ]) AS holiday_date

    UNION ALL

    SELECT 'Nagaland', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-04-03', DATE '2026-04-05'
    )) AS holiday_date

    UNION ALL

    SELECT 'Nagaland', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-10-19', DATE '2026-10-21'
    )) AS holiday_date

    UNION ALL

    SELECT 'Nagaland', holiday_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE '2026-12-22', DATE '2026-12-31'
    )) AS holiday_date
),

holiday_calendar AS (
    SELECT DISTINCT geography, holiday_date
    FROM holiday_calendar_raw
),

session_data AS (
    SELECT DISTINCT
        school_state,
        school_district,
        school_taluka,
        school_partner,
        school_ward,
        school_area,
        batch_academic_year,
        batch_no,
        batch_grade,
        facilitator_name,
        fac_start_date,
        fac_end_date,
        session_name,
        session_code,
        SAFE_CAST(session_date AS DATE) AS session_date,
        total_student_present,

        CASE
            WHEN session_date IS NULL
              OR total_student_present IS NULL
            THEN session_code
        END AS remaining_session,

        CASE
            WHEN session_date IS NOT NULL
             AND total_student_present IS NOT NULL
            THEN 1
            ELSE 0
        END AS Sessions_completed,

        CASE
            WHEN session_date IS NOT NULL
             AND total_student_present IS NOT NULL
            THEN session_code
        END AS Session_Completion

    FROM {{ ref('int_global_session') }}
    WHERE session_type = 'Student'
      AND batch_academic_year IS NOT NULL
),

batch_data AS (
    SELECT
        batch_academic_year,
        batch_no,
        batch_grade,
        school_district,
        school_taluka,
        school_state,
        school_partner,
        school_ward,
        school_area,
        facilitator_name,
        fac_start_date,
        fac_end_date,

        STRING_AGG(DISTINCT session_code, ', ') AS session_code,
        COUNT(DISTINCT remaining_session) AS remaining_session,

        MAX(IF(
            session_name LIKE '01.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S1_date,

        MAX(IF(
            session_name LIKE '02.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S2_date,

        MAX(IF(
            session_name LIKE '03.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S3_date,

        MAX(IF(
            session_name LIKE '04.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S4_date,

        MAX(IF(
            session_name LIKE '05.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S5_date,

        MAX(IF(
            session_name LIKE '06.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S6_date,

        MAX(IF(
            session_name LIKE '07.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S7_date,

        MAX(IF(
            session_name LIKE '08.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S8_date,

        MAX(IF(
            session_name LIKE '09.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S9_date,

        MAX(IF(
            session_name LIKE '10.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S10_date,

        MAX(IF(
            session_name LIKE '11.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S11_date,

        MAX(IF(
            session_name LIKE '12.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S12_date,

        MAX(IF(
            session_name LIKE '13.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S13_date,

        MAX(IF(
            session_name LIKE '14.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S14_date,

        MAX(IF(
            session_name LIKE '15.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S15_date,

        MAX(IF(
            session_name LIKE '16.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S16_date,

        MAX(IF(
            session_name LIKE '17.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S17_date,

        MAX(IF(
            session_name LIKE '18.%'
            AND Sessions_completed = 1,
            session_date, NULL
        )) AS S18_date,

        SUM(Sessions_completed) AS Sessions_completed,
        COUNT(DISTINCT session_code) AS total_sessions,
        COUNT(DISTINCT Session_Completion) AS completed_sessions

    FROM session_data
    GROUP BY
        batch_academic_year,
        batch_no,
        batch_grade,
        school_district,
        school_taluka,
        school_state,
        school_partner,
        school_ward,
        school_area,
        facilitator_name,
        fac_start_date,
        fac_end_date
),

-- Only sessions with both a date and attendance are included.
-- Deduplicate dates so multiple sessions on the same day
-- do not create artificial gaps.
completed_session_dates AS (
    SELECT DISTINCT
        school_state,
        school_district,
        school_taluka,
        school_partner,
        school_ward,
        school_area,
        batch_academic_year,
        batch_no,
        batch_grade,
        facilitator_name,
        fac_start_date,
        fac_end_date,
        session_date
    FROM session_data
    WHERE session_date IS NOT NULL
      AND Sessions_completed = 1
),

ordered_completed_sessions AS (
    SELECT
        *,
        LAG(session_date) OVER (
            PARTITION BY
                school_state,
                school_district,
                school_taluka,
                school_partner,
                school_ward,
                school_area,
                batch_academic_year,
                batch_no,
                batch_grade,
                facilitator_name,
                fac_start_date,
                fac_end_date
            ORDER BY session_date
        ) AS previous_session_date
    FROM completed_session_dates
),

session_gap_agg AS (
    SELECT
        school_state,
        school_district,
        school_taluka,
        school_partner,
        school_ward,
        school_area,
        batch_academic_year,
        batch_no,
        batch_grade,
        facilitator_name,
        fac_start_date,
        fac_end_date,

        COUNTIF(previous_session_date IS NOT NULL) AS gap_count,

        AVG(
            DATE_DIFF(session_date, previous_session_date, DAY)
        ) AS avg_gap_days,

        MAX(
            DATE_DIFF(session_date, previous_session_date, DAY)
        ) AS max_gap_days,

        MIN(
            DATE_DIFF(session_date, previous_session_date, DAY)
        ) AS min_gap_days,

        COUNTIF(
            previous_session_date IS NOT NULL
            AND DATE_DIFF(session_date, previous_session_date, DAY) = 0
        ) AS same_day_sessions_count,

        COUNTIF(
            previous_session_date IS NOT NULL
            AND DATE_DIFF(session_date, previous_session_date, DAY)
                BETWEEN 1 AND 4
        ) AS count_sessions_in_4_days,

        COUNTIF(
            previous_session_date IS NOT NULL
            AND DATE_DIFF(session_date, previous_session_date, DAY)
                BETWEEN 5 AND 14
        ) AS count_sessions_in_14_days,

        COUNTIF(
            previous_session_date IS NOT NULL
            AND DATE_DIFF(session_date, previous_session_date, DAY)
                BETWEEN 15 AND 30
        ) AS count_sessions_in_30_days,

        COUNTIF(
            previous_session_date IS NOT NULL
            AND DATE_DIFF(session_date, previous_session_date, DAY) > 30
        ) AS count_sessions_more_than_30_days

    FROM ordered_completed_sessions
    GROUP BY
        school_state,
        school_district,
        school_taluka,
        school_partner,
        school_ward,
        school_area,
        batch_academic_year,
        batch_no,
        batch_grade,
        facilitator_name,
        fac_start_date,
        fac_end_date
),

diff_data AS (
    SELECT
        *,
        DATE_DIFF(S2_date, S1_date, DAY) AS S2_S1Diff,
        DATE_DIFF(S3_date, S2_date, DAY) AS S3_S2Diff,
        DATE_DIFF(S4_date, S3_date, DAY) AS S4_S3Diff,
        DATE_DIFF(S5_date, S4_date, DAY) AS S5_S4Diff,
        DATE_DIFF(S6_date, S5_date, DAY) AS S6_S5Diff,
        DATE_DIFF(S7_date, S6_date, DAY) AS S7_S6Diff,
        DATE_DIFF(S8_date, S7_date, DAY) AS S8_S7Diff,
        DATE_DIFF(S9_date, S8_date, DAY) AS S9_S8Diff,
        DATE_DIFF(S10_date, S9_date, DAY) AS S10_S9Diff,
        DATE_DIFF(S11_date, S10_date, DAY) AS S11_S10Diff,
        DATE_DIFF(S12_date, S11_date, DAY) AS S12_S11Diff,
        DATE_DIFF(S13_date, S12_date, DAY) AS S13_S12Diff,
        DATE_DIFF(S14_date, S13_date, DAY) AS S14_S13Diff,
        DATE_DIFF(S15_date, S14_date, DAY) AS S15_S14Diff,
        DATE_DIFF(S16_date, S15_date, DAY) AS S16_S15Diff,
        DATE_DIFF(S17_date, S16_date, DAY) AS S17_S16Diff,
        DATE_DIFF(S18_date, S17_date, DAY) AS S18_S17Diff
    FROM batch_data
),

final_data AS (
    SELECT
        *,
        [
            S2_S1Diff, S3_S2Diff, S4_S3Diff, S5_S4Diff,
            S6_S5Diff, S7_S6Diff, S8_S7Diff, S9_S8Diff,
            S10_S9Diff, S11_S10Diff, S12_S11Diff, S13_S12Diff,
            S14_S13Diff, S15_S14Diff, S16_S15Diff, S17_S16Diff,
            S18_S17Diff
        ] AS session_diffs
    FROM diff_data
),

final AS (
    SELECT
        f.school_state,
        f.school_district,
        f.school_taluka,
        f.school_ward,
        f.school_area,
        f.school_partner,
        f.batch_academic_year,
        f.batch_no,
        f.batch_grade,
        f.facilitator_name,
        f.fac_start_date,
        f.fac_end_date,
        f.session_code,
        f.remaining_session,
        f.total_sessions,
        f.completed_sessions,

        f.S2_S1Diff,
        f.S3_S2Diff,
        f.S4_S3Diff,
        f.S5_S4Diff,
        f.S6_S5Diff,
        f.S7_S6Diff,
        f.S8_S7Diff,
        f.S9_S8Diff,
        f.S10_S9Diff,
        f.S11_S10Diff,
        f.S12_S11Diff,
        f.S13_S12Diff,
        f.S14_S13Diff,
        f.S15_S14Diff,
        f.S16_S15Diff,
        f.S17_S16Diff,
        f.S18_S17Diff,

        COALESCE(g.gap_count, 0) AS gap_count,
        g.max_gap_days AS Max_difference,
        g.min_gap_days AS Min_difference,

        CAST(ROUND(g.avg_gap_days, 0) AS INT64)
            AS Avg_Session_completion_days,

        COALESCE(g.same_day_sessions_count, 0)
            AS Same_day_sessions_count,

        COALESCE(g.count_sessions_in_4_days, 0)
            AS Count_sessions_in_4_days,

        COALESCE(g.count_sessions_in_14_days, 0)
            AS Count_sessions_in_14_days,

        COALESCE(g.count_sessions_in_30_days, 0)
            AS Count_sessions_in_30_days,

        COALESCE(g.count_sessions_more_than_30_days, 0)
            AS Count_sessions_more_than_30_days,

        CASE
            WHEN f.fac_start_date IS NULL THEN NULL
            WHEN EXTRACT(
                DAYOFWEEK FROM SAFE_CAST(f.fac_start_date AS DATE)
            ) = 1 THEN 0
            WHEN h.holiday_date IS NOT NULL THEN 0
            ELSE 1
        END AS school_working_status

    FROM final_data f

    LEFT JOIN session_gap_agg g
        ON f.batch_academic_year = g.batch_academic_year
        AND f.batch_no = g.batch_no
        AND f.batch_grade = g.batch_grade
        AND f.school_district = g.school_district
        AND f.school_taluka = g.school_taluka
        AND f.school_state = g.school_state
        AND f.school_partner = g.school_partner
        AND f.school_ward = g.school_ward
        AND f.school_area = g.school_area
        AND f.facilitator_name = g.facilitator_name
        AND f.fac_start_date = g.fac_start_date
        AND f.fac_end_date = g.fac_end_date

    LEFT JOIN holiday_calendar h
        ON SAFE_CAST(f.fac_start_date AS DATE) = h.holiday_date
        AND (
            (
                h.geography = 'Rajasthan'
                AND f.school_district IN ('Udaipur', 'Dungarpur')
            )
            OR (
                h.geography = 'Maharashtra'
                AND f.school_district IN (
                    'Mumbai', 'Pune', 'Thane', 'Dharashiv', 'Osmanabad'
                )
            )
            OR (
                h.geography = f.school_state
                AND f.school_state IN ('Haryana', 'Goa', 'Nagaland')
            )
        )
)

SELECT *
FROM final