from get_metadata import get_columns

def create_raw_sql_file(tidy_table, tablename, business_keys, foreign_keys, tidy_tables):
    #Fetch all of the table columns
    cols = [dict(col) for col in get_columns('repl_' + tablename)]
    #Find out if there's a parent for this tablename
    parent = None
    if tidy_table in foreign_keys:
        parent = foreign_keys[tidy_table]['parent']
    #Fetch the parent business key, if the parent exists
    if parent:
        parent_business_key = business_keys[parent].upper()
    sql = """select distinct\n"""
    i=0
    #Add in all of the child columns
    for col in cols:
        sql+="c."+col['column_name'].upper()
        i+=1
        if not parent:
            if i!=len(cols):
                sql+=",\n"
        else:
            sql += ",\n"
    #If the parent exists then also add in the parent business key column
    if parent:
        sql+="p."+parent_business_key
    #Add in the child table
    sql+="\nfrom\n"
    sql+="    {{ source('dbtvault_bigquery_demo', 'repl_%s') }} as c"%tablename
    #If the parent exists then also left join on the parent
    if parent:
        #As the parent is in staging, it's name will be untidy, however, so we need to look up the untidy version
        untidy_parent=tidy_tables[parent]
        sql+="\nleft join\n"
        sql+="    {{ source('dbtvault_bigquery_demo', 'repl_%s') }} as p\n"%untidy_parent
        sql+="on p.%s = c.%s"%(foreign_keys[tidy_table]['parent_pk'], foreign_keys[tidy_table]['fk'])
    #Write out the file using the tidied up form of the table name
    output_file = 'models\\raw_stage\\v_raw_%s.sql'%tidy_table
    file = open(output_file,"w")
    file.write(sql)
    file.close()