
    
    

select
    REQUESTS_ID as unique_field,
    count(*) as n_records

from "iceberg"."public"."BLDY_REQ_INFO_VIEW_HAIL_37"
where REQUESTS_ID is not null
group by REQUESTS_ID
having count(*) > 1


