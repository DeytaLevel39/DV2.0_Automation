{%- set yaml_metadata -%}
source_model: 'v_raw_orders'
derived_columns:
  ORDERS_KEY: 'ORDER_NUMBER'
  UK_CUSTOMERS_KEY: 'CUSTOMER_NUMBER'
  RECORD_SOURCE: '!2'
  EFFECTIVE_FROM: 'LASTMODIFIEDDATE'
hashed_columns:
  ORDERS_HK: 'ORDERS_KEY'
  UK_CUSTOMERS_HK: 'UK_CUSTOMERS_KEY'
  ORDERS_UK_CUSTOMERS_HK:
    - 'ORDERS_KEY'
    - 'UK_CUSTOMERS_KEY'
  ORDERS_HASHDIFF:
  is_hashdiff: true
  columns: 
    - ORDER_ID
    - ORDER_NUMBER
    - ORDER_PRICE
    - CUSTOMER_ID
    - LASTMODIFIEDDATE
    - CREATEDDATE
    - CRUD_FLAG
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)

SELECT *,
       '{{ var('load_date') }}' AS LOAD_DATE
FROM staging
