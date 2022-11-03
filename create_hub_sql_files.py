def create_hub_sql_files(business_keys, source_models):
    for tidy_table, sources in source_models.items():
        business_key = business_keys[sources[0]].upper()
        sql="""{%%- set source_model = %s   -%%}
    {%%- set src_pk = "%s_HK" -%%}
    {%%- set src_nk = "%s" -%%}
    {%%- set src_ldts = "LOAD_DATE" -%%}
    {%%- set src_source = "RECORD_SOURCE" -%%}
    
    {{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}        
    """%(["v_stg_"+s for s in sources], tidy_table.upper(), business_key)
        output_file = 'models\\raw_vault\\hubs\hub_%s.sql'%tidy_table
        file = open(output_file,"w")
        file.write(sql)
        file.close()