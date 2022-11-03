# import external libraries
from read_yaml_file import read_yaml
from create_raw_sql_file import create_raw_sql_file
from create_stage_sql_file import create_stage_sql_file
from create_sat_sql_file import create_sat_sql_file
from create_hub_sql_files import create_hub_sql_files
from create_link_sql_files import create_link_sql_files

# Fetch all of the business keys
business_keys = read_yaml("business_keys.yaml")
# Fetch all of the parent-child relationshipps
foreign_keys = read_yaml("foreign_keys.yaml")
# Fetch all of the record source info
record_sources = read_yaml("record_sources.yaml")
# Fetch all of the source_model info (for hubs & links)
source_models = read_yaml("source_models.yaml")

#Fetch all of the tables and their tidied up versions
tidy_tables = read_yaml("tidy_tables.yaml")

#Loop through all of our tables which are reliant on single sources
print("Generating the raw_staging, prepared_staging and satellite template files")
for tidy_table, tablename in tidy_tables.items():
    create_raw_sql_file(tidy_table, tablename, business_keys, foreign_keys, tidy_tables)
    create_stage_sql_file(tidy_table, tablename,  business_keys, foreign_keys, record_sources, source_models)
    create_sat_sql_file(tidy_table, tablename, business_keys, source_models)

#And create all of our files which may have multiple sources
print("Generating the hub template files")
#create_hub_sql_files(business_keys, source_models)
print("Generating the link template files")
create_link_sql_files(business_keys, foreign_keys, source_models)
