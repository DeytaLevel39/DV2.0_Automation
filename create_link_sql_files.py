def create_link_sql_files(business_keys, foreign_keys, source_models):
    #Loop through all of the source models
    for tidy_table,sources in source_models.items():
        parent = None
        #If the first source has a foreign key associated to it then find the parent
        if sources[0] in foreign_keys:
            parent = foreign_keys[sources[0]]['parent']
        #If we found the parent then as we are dealing with multiple sources find the common name e.g. UK_Customers becomes customers.
        if parent:
            for key,val in source_models.items():
                if parent in val:
                    common_parent = key
            #Construct the SQL
            sql="""{%%- set source_model = %s -%%}
        {%%- set src_pk = '%s_HK' -%%}
        {%%- set src_fk = ['%s_HK', '%s_HK'] -%%}
        {%%- set src_ldts = "LOAD_DATE" -%%}
        {%%- set src_source = "RECORD_SOURCE" -%%}
        
        {{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                         src_source=src_source, source_model=source_model) }}"""%(["v_stg_"+s for s in sources],
                                                                                  common_parent.upper()+"_"+tidy_table.upper(),
                                                                                  common_parent.upper(), tidy_table.upper())
            #And write to file
            output_file = 'models\\raw_vault\\links\link_%s_%s.sql'%(common_parent, tidy_table)
            file = open(output_file,"w")
            file.write(sql)
            file.close()