{% macro requests_awaiting_payment(status_column) %}
  CASE
    WHEN TRIM({{ status_column }}) IN (
      'تم اصدار فاتورة سداد',
      'بانتظار اتمام عملية الدفع',
      'في انتظار سداد المديونيات'
    ) THEN 1
    ELSE 0
  END
{% endmacro %}