{% macro clean_survey_column(column_name) %}

NULLIF(
  TRIM(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                LOWER({{ column_name }}),

                r'(\d+)_+(\d+)', r'\1-\2'   -- numeric ranges

              ),
              r'_+', ' '                    -- underscores → space
            ),
            r'\bdon t\b', "don't"           -- ✅ fix don't
          ),
          r'\s{2,}', ', '                   -- multi-space → comma
        ),
        r'\s+,', ','                        -- cleanup commas
      ),
      r',\s*$', ''                          -- remove trailing comma
    )
  ),
''                                          -- empty → NULL
)

{% endmacro %}