{% macro requests_completed(status_column) %}
  CASE
    WHEN TRIM({{ status_column }}) IN (
      'منتهى',
      'تم رفض الطلب',
      'تم رفض نقل الملكية',
      'مرفوض من الجهات الخارجية',
      'مرفوض من الدفاع المدني',
      'ملغي من الدفاع المدني'
    ) THEN 1
    ELSE 0
  END
{% endmacro %}