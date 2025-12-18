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
        WHEN s.completed_request = 1 THEN DATE_DIFF('day', DATE(s.CREATE_DATE), DATE(s.UPDATE_DATE))
        ELSE DATE_DIFF('day', DATE(s.CREATE_DATE), DATE(CURRENT_TIMESTAMP))
    END AS Duration_days,
    DATE_DIFF('day', DATE(s.UPDATE_DATE), DATE(CURRENT_TIMESTAMP)) as Days_from_last_update
FROM
    "iceberg"."silver"."staging_table" AS s
LEFT JOIN
    "iceberg"."silver"."city_codes" AS cc 
    ON TRIM(s.BALADIA_NAME) = cc.baladia