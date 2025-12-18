
    
    

select
    licenses_no as unique_field,
    count(*) as n_records

from "iceberg"."public"."BLDY_LIC_IN_RAQABA_ALL"
where licenses_no is not null
group by licenses_no
having count(*) > 1


