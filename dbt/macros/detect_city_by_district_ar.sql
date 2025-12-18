{% macro detect_city_by_district_ar(district_name_column_ar) %}
  CASE
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية الشمال',
      'بلدية الوسط',
      'أمانة منطقة حائل',
      'بلدية الجنوب',
      'الإدارة العامة للتخطيط العمراني - أمانة منطقة حائل',
      'إدارة شؤون الإسكان - أمانة منطقة حائل',
      'الوحدة المركزية لإعتماد المخططات - أمانة منطقة حائل',
      'الإدارة العامة للأراضي والمساحة - أمانة منطقة حائل',
      'إدارة الرخص الفنية',
      'بلدية الروضة',
      'بلدية جبة',
      'بلدية الخطة'
    ) THEN 'حائل'
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية محافظة الحائط',
      'بلدية انبوان',
      'بلدية الحليفة'
    ) THEN 'الحائط'
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية محافظة السليمي'
    ) THEN 'السليمي'
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية محافظة الشملي'
    ) THEN 'الشمالي'
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية محافظة الشنان',
      'بلدية الكهفة',
      'بلدية فيد'
    ) THEN 'الشنان'
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية محافظة الغزالة'
    ) THEN 'الغزالة'
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية بقعاء',
      'بلدية تربة',
      'بلدية الاجفر'
    ) THEN 'بقعاء'
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية سميراء'
    ) THEN 'سميراء'
    WHEN TRIM({{ district_name_column_ar }}) IN (
      'بلدية موقق'
    ) THEN 'موقق'
    ELSE 'Unknown'
  END
{% endmacro %}