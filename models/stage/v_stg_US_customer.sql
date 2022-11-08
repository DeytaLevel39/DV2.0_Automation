{%- set yaml_metadata -%}
source_model: 'v_raw_US_customer'
derived_columns:
  US_CUSTOMER_KEY: 'CUSTOMER_NUMBER'
  RECORD_SOURCE: '!101'
  EFFECTIVE_FROM: 'LASTMODIFIEDDATE'
hashed_columns:
  CUSTOMER_HK: 'US_CUSTOMER_KEY'
  CUSTOMER_HASHDIFF:
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
