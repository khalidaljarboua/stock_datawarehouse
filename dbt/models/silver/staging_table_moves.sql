{{
  config(
    materialized='table'
  )
}}

WITH 
commercial AS (
  SELECT
    'commercial' as source_table,
    amana,
    baladia,
    NULL as baladia_studied_request,
    licence_id,
    request_id,
    create_date,
    invoice_date,
    request_name,
    request_status_final,
    request_status_previous,
    request_status_current,
    employee_name,
    movement_start_date,
    movement_end_date,
    process_days,
    year,
    total_process_days,
    vacation_days,
    working_days,
    request_type,
    isic_activity_code,
    isic_activity_name,
    detailed_activity_code,
    detailed_activity_name,
    district,
    street,
    X,
    Y,
    shop_area,
    license_type,
    establishment_type,
    request_source,
    is_fawry,
    notes,
    CASE 
      WHEN request_status_current IN ('ملغى', 'مسودة') THEN TRUE 
      ELSE FALSE 
    END as cancelled_requests,
    {{ detect_completed_request('request_status_current') }} as completed_request
  FROM {{ ref('commercial_moves') }}
),

construction AS (
  SELECT
    'construction' as source_table,
    amana,
    baladia,
    baladia_studied_request,
    licence_id,
    request_id,
    create_date,
    invoice_date,
    request_name,
    request_status_final,
    request_status_previous,
    request_status_current,
    employee_name,
    movement_start_date,
    movement_end_date,
    process_days,
    year,
    total_process_days,
    vacation_days,
    working_days,
    request_type,
    NULL as isic_activity_code,
    NULL as isic_activity_name,
    NULL as detailed_activity_code,
    NULL as detailed_activity_name,
    NULL as district,
    NULL as street,
    NULL as X,
    NULL as Y,
    NULL as shop_area,
    NULL as license_type,
    NULL as establishment_type,
    NULL as request_source,
    NULL as is_fawry,
    notes,
    CASE 
      WHEN request_status_current IN ('ملغى', 'مسودة') THEN TRUE 
      ELSE FALSE 
    END as cancelled_requests,
    {{ detect_completed_request('request_status_current') }} as completed_request
  FROM {{ ref('construction_moves') }}
),

cadastral AS (
  SELECT
    'cadastral' as source_table,
    amana,
    baladia,
    NULL as baladia_studied_request,
    licence_id,
    request_id,
    create_date,
    invoice_date,
    request_name,
    request_status_final,
    request_status_previous,
    request_status_current,
    employee_name,
    movement_start_date,
    movement_end_date,
    process_days,
    year,
    total_process_days,
    vacation_days,
    working_days,
    request_type,
    NULL as isic_activity_code,
    NULL as isic_activity_name,
    NULL as detailed_activity_code,
    NULL as detailed_activity_name,
    NULL as district,
    NULL as street,
    NULL as X,
    NULL as Y,
    NULL as shop_area,
    NULL as license_type,
    NULL as establishment_type,
    NULL as request_source,
    is_fawry,
    notes,
    CASE 
      WHEN request_status_current IN ('ملغى', 'مسودة') THEN TRUE 
      ELSE FALSE 
    END as cancelled_requests,
    {{ detect_completed_request('request_status_current') }} as completed_request
  FROM {{ ref('cadastral_moves') }}
),

unified AS (
  SELECT * FROM commercial
  UNION ALL
  SELECT * FROM construction
  UNION ALL
  SELECT * FROM cadastral
)

SELECT 
  *,
  -- Calculate duration only when both dates are available
  CASE 
    WHEN movement_start_date IS NOT NULL AND movement_end_date IS NOT NULL 
    THEN DATE_DIFF('day', movement_start_date, movement_end_date)
    ELSE NULL 
  END as duration_days,
  -- Calculate days since last update
  CASE 
    WHEN movement_end_date IS NOT NULL 
    THEN DATE_DIFF('day', movement_end_date, CURRENT_TIMESTAMP)
    ELSE NULL 
  END as days_from_last_update,
  CURRENT_TIMESTAMP as dbt_loaded_at
FROM unified