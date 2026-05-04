import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

TABLES = [
    "region", "nation", "customer", "supplier",
    "part", "partsupp", "orders", "lineitem"
]

BASE_PATH = "/home/vivek/Projects/data-assist/data"

def run():
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        schema="TPCH_STAGE"
    )
    cur = conn.cursor()
    cur.execute("USE DATABASE GITHUB_ANALYTICS")
    cur.execute("USE SCHEMA TPCH_STAGE")
    for table in TABLES:
        local_path = f"{BASE_PATH}/{table}"
        os.makedirs(local_path, exist_ok=True)
        print(f"Downloading {table}...")
        cur.execute(f"GET @tpch_stage/{table}/ file://{local_path}/")
        print(f"  → Done")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run()