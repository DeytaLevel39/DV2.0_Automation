{%- set yaml_metadata -%}
source_model: 'v_raw_customer_wealth_bracket'
derived_columns:
  CUSTOMER_WEALTH_BRACKET_KEY: 'ID'
  RECORD_SOURCE: '!3'
  EFFECTIVE_FROM: 'LASTMODIFIEDDATE'
hashed_columns:
  CUSTOMER_WEALTH_BRACKET_HK: 'CUSTOMER_WEALTH_BRACKET_KEY'
  CUSTOMER_WEALTH_BRACKET_HASHDIFF:
    is_hashdiff: true
    columns: 
    - ID
    - NAME
    - DESCRIPTION
    - LASTMODIFIEDDATE
    - CREATEDDATE
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
