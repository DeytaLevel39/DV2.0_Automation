select distinct
c.CUSTOMER_ID,
c.CUSTOMER_NUMBER,
c.FIRST_NAME,
c.LAST_NAME,
c.LASTMODIFIEDDATE,
c.CREATEDDATE,
c.APPLIEDDATE,
c.CRUD_FLAG,
p.ID
from
    {{ source('dbtvault_bigquery_demo', 'repl_US_customers') }} as c
left join
    {{ source('dbtvault_bigquery_demo', 'repl_customer_wealth_brackets') }} as p
on p.id = c.wealth_bracket