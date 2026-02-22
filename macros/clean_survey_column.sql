{% macro clean_survey_column(column_name) %}

NULLIF(
  TRIM(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            LOWER({{ column_name }}),
            r'_+', ' '              -- underscores → space
          ),
          r'\s{2,}', ', '           -- multiple spaces → comma
        ),
        r'\s+,', ','                -- cleanup accidental commas
      ),
      r',\s*$', ''                  -- ✅ remove trailing comma
    )
  ),
''                                   -- ✅ convert empty → NULL
)

{% endmacro %}