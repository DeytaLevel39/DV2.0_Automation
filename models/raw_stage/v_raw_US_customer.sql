select distinct
c.CUSTOMER_ID,
c.CUSTOMER_NUMBER,
c.FIRST_NAME,
c.LAST_NAME,
c.LASTMODIFIEDDATE,
c.CREATEDDATE,
c.APPLIEDDATE,
c.CRUD_FLAG,
c.TITLE,
c.WEALTH_BRACKET
from
    {{ source('dbtvault_bigquery_demo', 'repl_US_customers') }} as c