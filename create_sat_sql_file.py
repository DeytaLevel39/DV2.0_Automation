from get_metadata import get_columns

def create_sat_sql_file(tidy_table, tablename, business_keys, source_models):
    #If we have multiple sources, then we need to find the common table name so that we can join them in the hubs & links
    for key,val in source_models.items():
        if tidy_table in val:
            common_table = key
    sql="""{%%- set source_model = "v_stg_%s" -%%}
{%%- set src_pk = "%s_HK" -%%}
{%%- set src_hashdiff = "%s_HASHDIFF" -%%}
{%%- set src_payload = ["""%(tidy_table, common_table.upper(), common_table.upper())
    #Fetch all of the table columns
    cols = [dict(col) for col in get_columns('repl_' + tablename)]
    i=0
    for col in cols:
        #Exclude the business key from the satellite table columns
        if col['column_name'].upper() != business_keys[tidy_table].upper():
            sql += '"%s"'%col['column_name'].upper()
            if i != len(cols)-1:
                    sql += ","
        i+=1
    sql+="""] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
"""
    output_file = 'models\\raw_vault\\sats\sat_%s.sql'%tidy_table
    file = open(output_file,"w")
    file.write(sql)
    file.close()