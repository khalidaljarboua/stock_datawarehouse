{% macro detect_city_code_by_district(district_name_column) %}
  CASE
    WHEN TRIM(TRIM({{ district_name_column }})) IN (
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
    ) THEN 'HA-01'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة الحائط',
      'بلدية انبوان',
      'بلدية الحليفة'
    ) THEN 'HA-06'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة السليمي'
    ) THEN 'HA-05'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة الشملي'
    ) THEN 'HA-07'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة الشنان',
      'بلدية الكهفة',
      'بلدية فيد'
    ) THEN 'HA-03'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة الغزالة'
    ) THEN 'HA-09'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية بقعاء',
      'بلدية تربة',
      'بلدية الاجفر'
    ) THEN 'HA-02'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية سميراء'
    ) THEN 'HA-04'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية موقق'
    ) THEN 'HA-08'
    ELSE 'Unknown'
  END
{% endmacro %}