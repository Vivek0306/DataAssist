import snowflake.connector
import os
import sqlite3

from dotenv import load_dotenv

load_dotenv()

PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
WAREHOUSE=os.getenv('SNOWFLAKE_WAREHOUSE')
USER = os.getenv('SNOWFLAKE_USER')
ACCOUNT  = os.getenv('SNOWFLAKE_ACCOUNT')

sql_conn = sqlite3.connect("store.db")
sql_cur = sql_conn.cursor()

conn = snowflake.connector.connect(
    account = os.getenv('SNOWFLAKE_ACCOUNT'),
    user =  os.getenv('SNOWFLAKE_USER'),
    password =  os.getenv('SNOWFLAKE_PASSWORD'),
    database =  os.getenv('SNOWFLAKE_DATABASE'),
    warehouse =  os.getenv('SNOWFLAKE_WAREHOUSE'),
    schema =  "TPCH_SF1"
)
sf_cur = conn.cursor()

print("Snowflake cursor test")
print(sf_cur.execute("SELECT COUNT(1) FROM TPCH_SF1.LINEITEM;").fetchone()[0])
for r in sf_cur.description:
    print(r[0])
# print(sf_cur.description)


