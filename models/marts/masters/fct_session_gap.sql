WITH session_data AS (

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

        CASE
            WHEN session_date IS NULL
                OR total_student_present IS NULL
            THEN session_code
        END AS remaining_session,

        CASE
            WHEN session_name LIKE '01.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S1_date,

        CASE
            WHEN session_name LIKE '02.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S2_date,

        CASE
            WHEN session_name LIKE '03.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S3_date,

        CASE
            WHEN session_name LIKE '04.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S4_date,

        CASE
            WHEN session_name LIKE '05.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S5_date,

        CASE
            WHEN session_name LIKE '06.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S6_date,

        CASE
            WHEN session_name LIKE '07.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S7_date,

        CASE
            WHEN session_name LIKE '08.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S8_date,

        CASE
            WHEN session_name LIKE '09.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S9_date,

        CASE
            WHEN session_name LIKE '10.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S10_date,

        CASE
            WHEN session_name LIKE '11.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S11_date,

        CASE
            WHEN session_name LIKE '12.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S12_date,

        CASE
            WHEN session_name LIKE '13.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S13_date,

        CASE
            WHEN session_name LIKE '14.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S14_date,

        CASE
            WHEN session_name LIKE '15.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S15_date,

        CASE
            WHEN session_name LIKE '16.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S16_date,

        CASE
            WHEN session_name LIKE '17.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S17_date,

        CASE
            WHEN session_name LIKE '18.%'
                AND session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN session_date
        END AS S18_date,

        CASE
            WHEN session_date IS NOT NULL
                AND total_student_present IS NOT NULL
            THEN 1
        END AS Sessions_completed,

        session_code,

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

        MAX(S1_date) AS S1_date,
        MAX(S2_date) AS S2_date,
        MAX(S3_date) AS S3_date,
        MAX(S4_date) AS S4_date,
        MAX(S5_date) AS S5_date,
        MAX(S6_date) AS S6_date,
        MAX(S7_date) AS S7_date,
        MAX(S8_date) AS S8_date,
        MAX(S9_date) AS S9_date,
        MAX(S10_date) AS S10_date,
        MAX(S11_date) AS S11_date,
        MAX(S12_date) AS S12_date,
        MAX(S13_date) AS S13_date,
        MAX(S14_date) AS S14_date,
        MAX(S15_date) AS S15_date,
        MAX(S16_date) AS S16_date,
        MAX(S17_date) AS S17_date,
        MAX(S18_date) AS S18_date,

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
            S2_S1Diff,
            S3_S2Diff,
            S4_S3Diff,
            S5_S4Diff,
            S6_S5Diff,
            S7_S6Diff,
            S8_S7Diff,
            S9_S8Diff,
            S10_S9Diff,
            S11_S10Diff,
            S12_S11Diff,
            S13_S12Diff,
            S14_S13Diff,
            S15_S14Diff,
            S16_S15Diff,
            S17_S16Diff,
            S18_S17Diff
        ] AS session_diffs

    FROM diff_data

),

final AS (

    SELECT
        school_state,
        school_district,
        school_taluka,
        school_ward,
        school_area,
        school_partner,
        batch_academic_year,
        batch_no,
        batch_grade,
        facilitator_name,
        fac_start_date,
        fac_end_date,
        session_code,
        remaining_session,
        total_sessions,
        completed_sessions,

        S2_S1Diff,
        S3_S2Diff,
        S4_S3Diff,
        S5_S4Diff,
        S6_S5Diff,
        S7_S6Diff,
        S8_S7Diff,
        S9_S8Diff,
        S10_S9Diff,
        S11_S10Diff,
        S12_S11Diff,
        S13_S12Diff,
        S14_S13Diff,
        S15_S14Diff,
        S16_S15Diff,
        S17_S16Diff,
        S18_S17Diff,

        -- Maximum gap
        (
            SELECT MAX(diff)
            FROM UNNEST(session_diffs) AS diff
            WHERE diff IS NOT NULL
        ) AS Max_difference,

        -- Minimum gap
        (
            SELECT MIN(diff)
            FROM UNNEST(session_diffs) AS diff
            WHERE diff IS NOT NULL
        ) AS Min_difference,

        -- Average gap
        CAST(
            ROUND(
                (
                    SELECT SUM(diff)
                    FROM UNNEST(session_diffs) AS diff
                    WHERE diff IS NOT NULL
                )
                / NULLIF(completed_sessions - 1, 0),
                0
            ) AS INT64
        ) AS Avg_Session_completion_days,

        -- Same day
        (
            SELECT COUNTIF(diff = 0)
            FROM UNNEST(session_diffs) AS diff
            WHERE diff IS NOT NULL
        ) AS Same_day_sessions_count,

        -- 1-4 days
        (
            SELECT COUNTIF(diff > 0 AND diff <= 4)
            FROM UNNEST(session_diffs) AS diff
            WHERE diff IS NOT NULL
        ) AS Count_sessions_in_4_days,

        -- 5-14 days
        (
            SELECT COUNTIF(diff > 4 AND diff <= 14)
            FROM UNNEST(session_diffs) AS diff
            WHERE diff IS NOT NULL
        ) AS Count_sessions_in_14_days,

        -- 15-30 days
        (
            SELECT COUNTIF(diff > 14 AND diff <= 30)
            FROM UNNEST(session_diffs) AS diff
            WHERE diff IS NOT NULL
        ) AS Count_sessions_in_30_days,

        -- More than 30 days
        (
            SELECT COUNTIF(diff > 30)
            FROM UNNEST(session_diffs) AS diff
            WHERE diff IS NOT NULL
        ) AS Count_sessions_more_than_30_days

        
    FROM final_data

)

SELECT *
FROM final