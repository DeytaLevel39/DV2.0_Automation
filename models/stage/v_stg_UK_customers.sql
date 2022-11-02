{%- set yaml_metadata -%}
source_model: 'v_raw_UK_customers'
derived_columns:
  UK_CUSTOMERS_KEY: 'CUSTOMER_NUMBER'
  CUSTOMER_WEALTH_BRACKETS_KEY: 'ID'
  RECORD_SOURCE: '!1'
  EFFECTIVE_FROM: 'LASTMODIFIEDDATE'
hashed_columns:
  UK_CUSTOMERS_HK: 'UK_CUSTOMERS_KEY'
  CUSTOMER_WEALTH_BRACKETS_HK: 'CUSTOMER_WEALTH_BRACKETS_KEY'
  UK_CUSTOMERS_CUSTOMER_WEALTH_BRACKETS_HK:
    - 'UK_CUSTOMERS_KEY'
    - 'CUSTOMER_WEALTH_BRACKETS_KEY'
  UK_CUSTOMERS_HASHDIFF:
  is_hashdiff: true
  columns: 
    - CUSTOMER_ID
    - CUSTOMER_NUMBER
    - FIRST_NAME
    - LAST_NAME
    - LASTMODIFIEDDATE
    - CREATEDDATE
    - APPLIEDDATE
    - CRUD_FLAG
    - TITLE
    - WEALTH_BRACKET
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
