
    
    

select
    LICENSE_ID as unique_field,
    count(*) as n_records

from "iceberg"."public"."V_BUILD_MAIN_REQ_HAIL_37"
where LICENSE_ID is not null
group by LICENSE_ID
having count(*) > 1


