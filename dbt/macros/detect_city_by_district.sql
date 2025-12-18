{% macro detect_city_by_district(district_name_column) %}
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
    ) THEN 'Hail'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة الحائط',
      'بلدية انبوان',
      'بلدية الحليفة'
    ) THEN 'Al Hait'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة السليمي'
    ) THEN 'As Sulaymi'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة الشملي'
    ) THEN 'Ash Shamli'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة الشنان',
      'بلدية الكهفة',
      'بلدية فيد'
    ) THEN 'Ash Shinan'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية محافظة الغزالة'
    ) THEN 'Al Ghazalah'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية بقعاء',
      'بلدية تربة',
      'بلدية الاجفر'
    ) THEN 'Baqa'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية سميراء'
    ) THEN 'Simira'
    WHEN TRIM({{ district_name_column }}) IN (
      'بلدية موقق'
    ) THEN 'Mawqaq'
    ELSE 'Unknown'
  END
{% endmacro %}