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
    {{ baladi_side('s.REQUEST_STATUS') }} AS baladi_side,
    CASE WHEN s.completed_request = 1 THEN s.UPDATE_DATE END AS CLOSE_DATE
FROM
    {{ ref('staging_table_moves') }} AS s
LEFT JOIN
    {{ ref('city_codes') }} AS cc 
    ON TRIM(s.BALADIA_NAME) = cc.baladia