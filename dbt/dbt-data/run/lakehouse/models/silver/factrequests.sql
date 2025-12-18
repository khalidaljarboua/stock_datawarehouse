
  
    

    create table "iceberg"."silver"."factrequests__dbt_tmp"
      
      
    as (
      SELECT
    s.*,
    cc.city,             
    cc.city_en,
    cc.city_code,
    cc.x,
    cc.y,
    cc.region,
    cc.region_en,
    cc.region_code,
    
  CASE
    WHEN s.REQUEST_STATUS IN (
      'رخصة استثنائية - رئيس البلدية',
      'الطلب لدى مشرف شهادات الاشغال',
      ' الطلب لدى مشرف الرخص الإنشائية',
      'رئيس البلدية',
      'مراقب الرخص التجارية',
      'استقبال الرخص التجارية',
      'رخصة قديمة - تحت المعالجة',
      ' الطلب لدى مهندس الرخص الإنشائية',
      'رخصة استثنائية - مشرف الرخص',
      'الطلب لدى المساح/المهندس',
      ' الطلب لدى مدقق الرخص الإنشائية',
      'بانتظار اعتماد البلدية',
      'الطلب لدى مشرف القرارات المساحية',
      'رخصة قديمة - بإنتظار الإعتماد',
      ' الطلب لدى استقبال القرارات المساحية',
      'مشرف الرخص'
    ) THEN TRUE
    ELSE FALSE
  END
 AS baladi_side,
    CASE 
        WHEN s.completed_request = 1 THEN DATE_DIFF('day', DATE(s.CREATE_DATE), DATE(s.UPDATE_DATE))
        ELSE DATE_DIFF('day', DATE(s.CREATE_DATE), DATE(CURRENT_TIMESTAMP))
    END AS Duration_days,
    DATE_DIFF('day', DATE(s.UPDATE_DATE), DATE(CURRENT_TIMESTAMP)) as Days_from_last_update
FROM
    "iceberg"."silver"."staging_table" AS s
LEFT JOIN
    "iceberg"."silver"."city_codes" AS cc 
    ON TRIM(s.BALADIA_NAME) = cc.baladia
    );

  