from get_metadata import get_columns

def create_stage_sql_file(tidy_table, tablename, business_keys, foreign_keys, record_sources, source_models):
    #If we have multiple sources, then we need to find the common table name so that we can join them in the hubs & links
    for key,val in source_models.items():
        if tidy_table in val:
            common_table = key
    #Fetch all of the table columns
    cols = [dict(col) for col in get_columns('repl_' + tablename)]
    #Find out if there's a parent for this tablename
    parent = None
    if tidy_table in foreign_keys:
        parent = foreign_keys[tidy_table]['parent']
        # If we have multiple sources, we'll also need to know the common parent name
        for key, val in source_models.items():
            if parent in val:
                common_parent = key
    if tidy_table in record_sources:
        record_source = record_sources[tidy_table]
    else:
        print("Error! No record source found!")
    #Fetch the business key
    if tidy_table in business_keys:
        business_key = business_keys[tidy_table].upper()
    #Fetch the parent business key, if the parent exists
    if parent:
        parent_business_key = business_keys[parent].upper()
    sql = """{%%- set yaml_metadata -%%}
source_model: 'v_raw_%s'
derived_columns:
  %s_KEY: '%s'\n"""%(tidy_table,tidy_table.upper(),business_key)
    if parent:
        sql+="  %s_KEY: '%s'\n"%(parent.upper(), parent_business_key)
    sql+="""  RECORD_SOURCE: '!%i'
  EFFECTIVE_FROM: 'LASTMODIFIEDDATE'
hashed_columns:
  %s_HK: '%s_KEY'"""%(record_source, common_table.upper(), tidy_table.upper())
    if parent:
        sql+="""\n  %s_HK: '%s_KEY'
  %s_%s_HK:
    - '%s_KEY'
    - '%s_KEY'"""%(common_parent.upper(), parent.upper(),
         common_parent.upper(), common_table.upper(),
         tidy_table.upper(),
         parent.upper())
    sql+="""
  % s_HASHDIFF:
    is_hashdiff: true
    columns: 
"""%common_table.upper()
    #Add in all of the table columns to the hashdiff including the business key column
    for col in cols:
        sql+="    - %s\n"%col['column_name'].upper()
    #Add in final boilerplate
    sql+="""{%- endset -%}

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
"""
    output_file = 'models\\stage\\v_stg_%s.sql'%tidy_table
    file = open(output_file,"w")
    file.write(sql)
    file.close()