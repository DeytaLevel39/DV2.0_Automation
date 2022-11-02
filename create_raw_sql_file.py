from get_metadata import get_columns

def create_raw_sql_file(tablename, business_keys, foreign_keys):
    #Fetch all of the table columns
    cols = [dict(col) for col in get_columns('repl_' + tablename)]
    #Find out if there's a parent for this tablename
    parent = None
    if tablename in foreign_keys:
        parent = foreign_keys[tablename]['parent']
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
    #If the parent exists then also add in the parent
    if parent:
        sql+="\nleft join\n"
        sql+="    {{ source('dbtvault_bigquery_demo', 'repl_%s') }} as p\n"%parent
        sql+="on p.%s = c.%s"%(foreign_keys[tablename]['parent_pk'], foreign_keys[tablename]['fk'])
    output_file = 'models\\raw_stage\\v_raw_%s.sql'%tablename
    file = open(output_file,"w")
    file.write(sql)
    file.close()