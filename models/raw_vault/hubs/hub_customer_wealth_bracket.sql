{%- set source_model = ['v_stg_customer_wealth_bracket']   -%}
    {%- set src_pk = "CUSTOMER_WEALTH_BRACKET_HK" -%}
    {%- set src_nk = "ID" -%}
    {%- set src_ldts = "LOAD_DATE" -%}
    {%- set src_source = "RECORD_SOURCE" -%}
    
    {{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}        
    