import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dotenv import load_dotenv
load_dotenv()

# This loads the credentials from your secrets.toml
source = sql_database()
source2 = sql_table(table="website_checks_log") # We could have used sql_database(), but we need just one table.

# This attempts to list the available tables/resources from your DB
print("Attempting to connect to the database...")
print(f"Connection successful! Available tables: {list(source.resources.keys())}")
print(f"Available tables in source2: {source2.table_name}")
