
  
    

    create table "iceberg"."silver"."staging_table__dbt_tmp"
      
      
    as (
      WITH 
trades AS (
  SELECT
    'تجاري' AS request_type,
    REQUESTS_ID as REQUEST_ID,
    LICENSES_NO as LICENSE_ID,
    CREATE_DATE,
    NAME as REQUEST_NAME,
    STATUS_NAME as REQUEST_STATUS,
    UPDATE_DATE,
    DISTRICTNAME AS DISTRICT_NAME,
    AMANANAME AS AMANA_NAME,
    BALADIANAME as BALADIA_NAME,
    TRADE_NAME,
    FULLNAME as SUBMITTER_NAME,
    MOBILENO as MOBILE_NO,
    STATUS_FAWRY,
    -- TYPE,
    -- STATUS_ID,
    -- AGENT_PERSON_ID,
    -- SUBMITTER_ID_TYPE,
    -- DISTRICT_ID,
    -- STORE_ID,
    -- LOCATION_LAND_NO,
    -- X,
    -- Y,
    -- LOCATION_MAP_NO,
    -- STREET_ID,
    -- STREET_NAME,
    -- MUNICIPALITY_ID,
    -- BUILDING_OWNER_IDNO,
    -- HOLES_COUNT,
    -- FLOOR_COUNT,
    -- STORE_AREA,
    -- LOCATION_LICENESE_ID,
    -- IDNO,
    -- BIRTHDAY,
    -- OWNER_TYPE_ID,
    -- CLIENT_TYPE_NAME,
    -- IDSOURCE,
    -- IDSTARTDATE,
    -- IDENDDATE,
    -- EMAIL,
    -- OWNER_GENDER,
    -- HEALTH_ACTIVITIE,
    -- D_ACTIVITIES_NAME,
    -- M_ACTIVITIES_ID,
    -- M_ACTIVITIES_NAME,
    -- ISIC_NUMBER,
    -- ISIC_DESC,
    -- UNITS_COUNT,
    -- ACCOMMODATION_TYPE_ID,
    -- ACCOMIDATION_DESC,
    -- FEMALE_ONLY,
    -- ACTIVITIE_TYPE_ID,
    -- ACTIVITIE_TYPE_NAME,
    -- IS_CITIZEN_PLEDGED_TOCLEAN,
    CASE WHEN STATUS_NAME IN ('ملغى', 'مسودة') THEN TRUE ELSE FALSE END AS CANCELLED_REQUESTS,
    
  CASE
    WHEN TRIM(STATUS_NAME) IN (
      'مرفوض من الجهات الخارجية',
      'رفض أولي - يرجى الإطلاع على التفاصيل في بوابة سلامـة',
      'تم رفض الطلب من المكتب الهندسي',
      'تم رفض نقل الملكية',
      'منتهى',
      'مرفوض من الدفاع المدني',
      'تم رفض الطلب'
    ) THEN 1
    ELSE 0
  END
 AS completed_request,
    
  CASE
    WHEN TRADE_NAME IN (
      'البنك السعودي البريطاني',
      'البنك السعودي الفرنسي',
      'البنك العربي الوطني',
      'العربية للعود',
      'بنك البلاد',
      'بنك الرياض',
      'شركة الاتصالات السعودية stc',
      'شركة النهدي الطبية',
      'شركة مرايا حائل المحدودة',
      'صيدلية حورة الريان الطبية',
      'صيدلية زاد الدوائية الطبية',
      'مدارس الفضيلة الأهلية',
      'مقهى الأدهم العربي لتقديم المشروبات',
      'شركة أسواق عبدالله العثيم',
      'شركة عبدالله العثيم للاستثمار',
      'شركة عبدالله العثيم للاستثمار مول حائل',
      'شركة المراعي المحدوده',
      'منتجات المراعي',
      'شركة جرير للتسويق',
      'فرع شركة درعة للتجارة',
      'شركة درعة للتجارة',
      'مؤسسة درعة للتجاره',
      'شركة الاتصالات المتنقلة السعودية زين',
      'شركة الراجحي المصرفية للإستثمار فرع الحائط',
      'الشركة الوطنية للتنمية الزراعية نادك',
      'شركة خدمات النفط',
      'شركة خدمات النفط المحدوده',
      'شركة خدمات النفط المحدودة',
      'فرع مصرف الإنماء بحي المنتزه الشرقي',
      'شركة الرياض العالميه للاغذيه ماكدونالدز',
      'ماكدونالدز جي واحد واربعون',
      'مؤسسة عبدالله المشعان للمقاولات العامة',
      'البنك السعودي للإستثمار فرع حائل',
      'محطة تميز الشرق للوقود',
      'البنك الاهلي السعودي',
      'شركة اتحاد اتصالات موبايلي',
      'موبايلي',
      'موبايلي للإتصالات',
      'مصنع سماء حائل للبلك والخرسانة الجاهزة للمنتجات الأسمنتية',
      'شركة سلمان صلاح السليمي للصناعة',
      'شركة سلمان صلاح السليمي للصيانة والنظافة'
    ) THEN TRUE
    ELSE FALSE
  END
 AS vip
  FROM "iceberg"."silver"."commercial"
),

constructions AS (
  SELECT
    'إنشائي' AS request_type,
    LICENSE_ID,
    AMANA_NAME,
    DISTRICTNAME AS DISTRICT_NAME,
    BALADI_ANAME as BALADIA_NAME,
    REQUEST_ID,
    REQUEST_STATUS_DESC as REQUEST_STATUS,
    REQUEST_TYPE_NAME as REQUEST_NAME,
    REQUEST_CREATE as CREATE_DATE,
    REQUEST_UPDATE as UPDATE_DATE,
    BUILDING_DESC as TRADE_NAME,
    SUBMITTER_NAME as SUBMITTER_NAME,
    SUBMITTER_MOBILE as MOBILE_NO,
    -- LICENSE_STATUS_DESC,
    -- ISSUE_DATE,
    -- EXPIRATION_DATE,
    -- BUILD_DESC,
    -- LICENSE_TYPE,
    -- LICENSE_TYPE_DESC,
    -- BUILDING_TYPE,
    -- MAIN_BUILDING_CLASS,
    -- MAIN_BUILDING_DESC,
    -- SUB_BUILDING_CLASS,
    -- SUB_BUILDING_DESC,
    -- OLD_LICENSE_ID,
    -- AMMANA_ID,
    -- BALADI_ID,
    -- DISTRICT_ID,
    -- MAP_NO,
    -- BLOCK_NO,
    -- X,
    -- Y,
    -- BUILDING_FLAT_AREA,
    -- AREA,
    -- OWNERSHIP_DOC_TYPE,
    -- OWNERSHIP_DOC_DESC,
    -- OWNERSHIP_DOC_NO,
    -- OWNERSHIP_DOC_DATE,
    -- ENG_OFF_DESIGNER_ID,
    -- ENG_OFF_DESIGNER_NAME,
    -- ENG_OFF_CONSULTANT_ID,
    -- ENG_OFF_CONSULTANT_NAME,
    -- CONTRACTOR_ID,
    -- CONTRACTOR_NAME,
    -- WASTE_CONTRACTOR_ID,
    -- WASTE_CONTRACTOR_NAME,
    -- SURVEY_REPORT_ID,
    -- SURVEY_REPORT_DATE,
    -- SUBMITTER_TYPE,
    -- SUBMITTER_TYPE_DESC,
    -- SUBMITTER_IDNO,
    -- INSUR_DOCUMENT_ID,
    -- INSUR_DOCUMENT_DATE,
    -- ISOL_GLASS_TYPE,
    -- ISOL_GLASS_VALUE,
    -- ISOL_ROOF_TYPE,
    -- ISOL_ROOF_VALUE,
    -- ISOL_WALL_TYPE,
    -- ISOL_WALL_VALUE,
    -- OWNERSHIP_DOC_GDATE,
    -- INSUR_DOCUMENT_GDATE,
    -- SURVEY_REPORT_GDATE,
    -- REQUEST_TYPE_ID,
    'غير فوري' AS STATUS_FAWRY,
    CASE WHEN REQUEST_STATUS_DESC IN ('ملغى', 'مسودة') THEN TRUE ELSE FALSE END AS CANCELLED_REQUESTS,
    
  CASE
    WHEN TRIM(REQUEST_STATUS_DESC) IN (
      'مرفوض من الجهات الخارجية',
      'رفض أولي - يرجى الإطلاع على التفاصيل في بوابة سلامـة',
      'تم رفض الطلب من المكتب الهندسي',
      'تم رفض نقل الملكية',
      'منتهى',
      'مرفوض من الدفاع المدني',
      'تم رفض الطلب'
    ) THEN 1
    ELSE 0
  END
 AS completed_request,
    FALSE as vip
  FROM "iceberg"."silver"."construction"
),

areas AS (
  SELECT
    'قرارات مساحية' AS request_type,
    AMANA_NAME,
    BALADIA_NAME,
    DISTRICTNAME AS DISTRICT_NAME,
    V_REQIUST_ID as REQUEST_ID,
    V_R_CREATE_DATE as CREATE_DATE,
    V_R_UPDATE_DATE as UPDATE_DATE,
    V_R_STATUS as REQUEST_STATUS,
    C_SURVEY_REPORT_ID AS LICENSE_ID,
    SUB_BUILDING_DESC as TRADE_NAME,
    ENG_OFFICE_NAME as SUBMITTER_NAME,
    ENG_OFFICE_MOBILE as MOBILE_NO,
    -- C_SURVEY_REPORT_ID,
    -- L_ISSUE_DATE,
    -- L_ISSUE_GT,
    -- L_EXPIRATION_DATE,
    -- V_PURPOSE_ID,
    -- PURPOSE_DESC,
    -- V_AREA,
    -- V_MAIN_BUILDING_CLASS,
    -- MAIN_BUILDING_DESC,
    -- V_SUB_BUILDING_CLASS,
    -- L_NOTES,
    -- AMANA_ID,
    -- BALADI_ID,
    -- R_DISTRICT_ID,
    -- R_MAP_NO,
    -- V_BLOCK_NO,
    -- V_OWNERSHIP_DOC_TYPE,
    -- OWNERSHIP_DOC_DESC,
    -- V_OWNERSHIP_DOC_NO,
    -- OWNERSHIP_DOC_DATE,
    -- OWNERSHIP_DOC_GDATE,
    -- V_ENG_OFFICE_ID,
    -- HAS_NEIGHBORS,
    -- HAS_BORDERS,
    -- HAS_INSTANT,
    -- IS_STREETS_ASPHALTED,
    -- V_ADDRESS,
    -- V_POSTAL_CODE,
    -- V_STREET_NAME,
    -- V_R_TYPE,
    -- R_ISSUING_MUNICIPALITY,
    'غير فوري' AS STATUS_FAWRY,
    CASE WHEN V_R_STATUS IN ('ملغى', 'مسودة') THEN TRUE ELSE FALSE END AS CANCELLED_REQUESTS,
    
  CASE
    WHEN TRIM(V_R_STATUS) IN (
      'مرفوض من الجهات الخارجية',
      'رفض أولي - يرجى الإطلاع على التفاصيل في بوابة سلامـة',
      'تم رفض الطلب من المكتب الهندسي',
      'تم رفض نقل الملكية',
      'منتهى',
      'مرفوض من الدفاع المدني',
      'تم رفض الطلب'
    ) THEN 1
    ELSE 0
  END
 AS completed_request,
    concat(v_r_type, ' لغرض ', 
       CASE 
        WHEN purpose_desc LIKE '%لغرض%' THEN REPLACE(purpose_desc, 'لغرض', '')
        ELSE purpose_desc 
       END) as request_name,
    FALSE as vip
  FROM "iceberg"."silver"."cadastral"
),

audits AS (
  SELECT
    'الرقابة اللاحقة' AS request_type,
    amananame as AMANA_NAME,
    baladianame as BALADIA_NAME,
    CAST(location_district AS varchar(255)) as DISTRICT_NAME,
    CAST(requests_id AS decimal(19,0)) as REQUEST_ID,
    CAST(licenses_no AS decimal(19,0)) as LICENSE_ID,
    observation_status_name as REQUEST_STATUS,
    TRADE_NAME,
    trade_name as SUBMITTER_NAME,
    mobile as MOBILE_NO,
    -- ammanaid,
    -- baladiaid,
    -- client_id,
    -- client_type,
    -- owner_type_id,
    -- requests_type,
    -- licenses_start_hdate,
    -- licenses_end_hdate,
    -- observation_status,
    -- store_area,
    -- sdad_update_date as CREATE_DATE,
    -- bill_amount,
    -- employeefullname,
    -- email,
    'غير فوري' AS STATUS_FAWRY,
    CASE WHEN observation_status_name IN ('ملغى', 'مسودة') THEN TRUE ELSE FALSE END AS CANCELLED_REQUESTS,
    
  CASE
    WHEN TRIM(observation_status_name) IN (
      'مرفوض من الجهات الخارجية',
      'رفض أولي - يرجى الإطلاع على التفاصيل في بوابة سلامـة',
      'تم رفض الطلب من المكتب الهندسي',
      'تم رفض نقل الملكية',
      'منتهى',
      'مرفوض من الدفاع المدني',
      'تم رفض الطلب'
    ) THEN 1
    ELSE 0
  END
 AS completed_request,
    FALSE as vip
  FROM "iceberg"."silver"."audit"
)



SELECT
  request_type,
  request_id,
  LICENSE_ID,
  REQUEST_STATUS,
  REQUEST_NAME,
  DISTRICT_NAME,
  BALADIA_NAME,
  AMANA_NAME,
  CANCELLED_REQUESTS,
  completed_request,
  CREATE_DATE,
  UPDATE_DATE,
  TRADE_NAME,
  SUBMITTER_NAME,
  MOBILE_NO,
  VIP,
  STATUS_FAWRY,
  CURRENT_TIMESTAMP AS dbt_loaded_at
FROM trades

UNION ALL

SELECT
  request_type,
  request_id,
  LICENSE_ID,
  REQUEST_STATUS,
  REQUEST_NAME,
  DISTRICT_NAME,
  BALADIA_NAME,
  AMANA_NAME,
  CANCELLED_REQUESTS,
  completed_request,
  CREATE_DATE,
  UPDATE_DATE,
  TRADE_NAME,
  SUBMITTER_NAME,
  MOBILE_NO,
  VIP,
  STATUS_FAWRY,
  CURRENT_TIMESTAMP AS dbt_loaded_at
FROM constructions

UNION ALL

SELECT
  request_type,
  request_id,
  LICENSE_ID,
  REQUEST_STATUS,
  REQUEST_NAME,
  DISTRICT_NAME,
  BALADIA_NAME,
  AMANA_NAME,
  CANCELLED_REQUESTS,
  completed_request,
  CREATE_DATE,
  UPDATE_DATE,
  TRADE_NAME,
  SUBMITTER_NAME,
  MOBILE_NO,
  VIP,
  STATUS_FAWRY,
  CURRENT_TIMESTAMP AS dbt_loaded_at
FROM areas
    );

  