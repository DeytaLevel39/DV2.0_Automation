{%- set yaml_metadata -%}
source_model: 'v_raw_order'
derived_columns:
  ORDER_KEY: 'ORDER_NUMBER'
  UK_CUSTOMER_KEY: 'CUSTOMER_NUMBER'
  RECORD_SOURCE: '!2'
  EFFECTIVE_FROM: 'LASTMODIFIEDDATE'
hashed_columns:
  ORDER_HK: 'ORDER_KEY'
  CUSTOMER_HK: 'UK_CUSTOMER_KEY'
  CUSTOMER_ORDER_HK:
    - 'ORDER_KEY'
    - 'UK_CUSTOMER_KEY'
  ORDER_HASHDIFF:
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
