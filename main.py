# import external libraries
from read_yaml_file import read_yaml
from create_raw_sql_file import create_raw_sql_file
from create_stage_sql_file import create_stage_sql_file

# Fetch all of the business keys
business_keys = read_yaml("business_keys.yaml")
# Fetch all of the parent-child relationshipps
foreign_keys = read_yaml("foreign_keys.yaml")
# Fetch all of the record source info
record_sources = read_yaml("record_sources.yaml")

tables = ["customer_wealth_brackets","UK_customers","US_customers","orders"]
for tablename in tables:
    create_raw_sql_file(tablename, business_keys, foreign_keys)
    create_stage_sql_file(tablename, business_keys, foreign_keys, record_sources)
