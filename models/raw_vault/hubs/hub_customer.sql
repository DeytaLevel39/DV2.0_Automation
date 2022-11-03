{%- set source_model = ['v_stg_UK_customer', 'v_stg_US_customer']   -%}
    {%- set src_pk = "CUSTOMER_HK" -%}
    {%- set src_nk = "CUSTOMER_NUMBER" -%}
    {%- set src_ldts = "LOAD_DATE" -%}
    {%- set src_source = "RECORD_SOURCE" -%}
    
    {{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}        
    