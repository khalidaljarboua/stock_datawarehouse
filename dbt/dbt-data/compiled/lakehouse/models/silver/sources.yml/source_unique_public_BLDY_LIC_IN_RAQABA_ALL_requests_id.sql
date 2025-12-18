
    
    

select
    requests_id as unique_field,
    count(*) as n_records

from "iceberg"."public"."BLDY_LIC_IN_RAQABA_ALL"
where requests_id is not null
group by requests_id
having count(*) > 1


