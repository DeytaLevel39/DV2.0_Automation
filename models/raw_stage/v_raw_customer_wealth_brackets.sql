select distinct
c.ID,
c.NAME,
c.DESCRIPTION,
c.LASTMODIFIEDDATE,
c.CREATEDDATE
from
    {{ source('dbtvault_bigquery_demo', 'repl_customer_wealth_brackets') }} as c