{% macro requests_draft(status_column) %}
  CASE
    WHEN TRIM({{ status_column }}) IN (
      'مسودة',
      'ملغى'
    ) THEN 1
    ELSE 0
  END
{% endmacro %}