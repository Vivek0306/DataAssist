import os, time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import timedelta, date
import psycopg2

load_dotenv()

JARS = "/home/vivek/jars/postgresql-42.7.3.jar"

TABLES = [
    "REGION", "NATION", "CUSTOMER", "SUPPLIER",
    "PART", "PARTSUPP", "ORDERS", "LINEITEM"
]

INCREMENTAL_TABLES = {
    "orders": {
        "watermark_column": None,
        "pk": ["O_ORDERKEY"]
    },
    "lineitem": {
        "watermark_column": None,
        "pk": ["L_ORDERKEY", "L_LINENUMBER"]
    },
    "customer": {
        "watermark_column": None,   # no date column, use PK instead
        "pk": ["C_CUSTKEY"]
    }
}
STATIC_TABLES = [
    "region", "nation", "supplier", "part", "partsupp"
]


RAW_SCHEMA = 'retail_raw'
STAGE_SCHEMA = 'retail_staging'
PG_URL = "jdbc:postgresql://localhost:5432/retail"
PG_PROPERTIES = {
    "user": os.getenv('PG_USER'),
    "password": os.getenv('PG_PASSWORD'),
    "driver": "org.postgresql.Driver"
}

def read_raw_tables(spark, table, filter = None):

    if filter:
        query = f"(SELECT * FROM {RAW_SCHEMA}.{table} WHERE {filter} ) as tmp"
    else:
        query = f"{RAW_SCHEMA}.{table}"

    return spark.read.format('jdbc')\
            .option("url", PG_URL)\
            .option("dbtable", f"{query}") \
            .option("user", PG_PROPERTIES['user']) \
            .option("password", PG_PROPERTIES['password']) \
            .option("driver", PG_PROPERTIES['driver']) \
            .load()

def get_last_timestamp(curr, table):
    timestamp_col = INCREMENTAL_TABLES[table]["watermark_column"]
    curr.execute(f"SELECT MAX({timestamp_col}) FROM {STAGE_SCHEMA}.{table};")
    return curr.fetchone()[0]      

def get_watermark(cur, table):
    cur.execute(f"""
        SELECT last_loaded_value 
        FROM {STAGE_SCHEMA}.pipeline_watermarks 
        WHERE table_name = %s
    """, (table,))
    result = cur.fetchone()
    return result[0] if result else None

def incremental_load_to_stage(spark, table, config, cur, con):
    watermark_col = config['watermark_column']
    pk_col = config["pk"]

    if watermark_col:
        watermark = get_watermark(cur, table)
        if watermark:
            print(f"    -> Loading Records after: {watermark}")
            where_clause = f"{watermark_col} >= \'{watermark}\'"
        else:
            print(f"    -> No watermark found! Attempting full load")
            where_clause = None
    else:
        watermark = get_watermark(cur, table)
        if watermark:
            pk = pk_col[0]
            print(f"    -> Loading records after: {pk} > {watermark}")
            where_clause = f"{pk} > '{watermark}'"
        else:
            where_clause = None
    
    df = read_raw_tables(spark, table, where_clause)
    count = df.count()
 
    if count == 0:
        print(f"    -> No new rows, skipping")
        return
 
    print(f"    -> {count} new rows found")
    upsert_to_stage(spark, df, table, pk_col, cur, con)
    print(f"    -> Upserted into {STAGE_SCHEMA}.{table}")
 
    if watermark_col:
        update_watermark(cur, con, table, watermark_col)
    else:
        # for customer, update watermark using max PK
        update_watermark(cur, con, table, pk_col[0])
 
def update_watermark(cur, conn, table, watermark_col):
    cur.execute(f"SELECT MAX({watermark_col}) FROM {STAGE_SCHEMA}.{table}")
    new_watermark = cur.fetchone()[0]
 
    if new_watermark:
        if isinstance(new_watermark, date):
    
            new_watermark = new_watermark - timedelta(days=1)
        cur.execute(f"""
            INSERT INTO {STAGE_SCHEMA}.pipeline_watermarks 
                (table_name, last_loaded_value, last_loaded_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (table_name) DO UPDATE 
                SET last_loaded_value = EXCLUDED.last_loaded_value,
                    last_loaded_at = NOW()
        """, (table, str(new_watermark)))
        conn.commit()
        print(f"  → Watermark updated to {new_watermark}")

def upsert_to_stage(spark, df, table, pk_col, cur, con):
    
    cur.execute(f"DROP TABLE IF EXISTS tmp_{table}")
    con.commit()
    
    df = df.toDF(*[c.lower() for c in df.columns])
    pk_col = [p.lower() for p in pk_col]
    tmp_table = f"tmp_{table}"

    df.dropDuplicates(pk_col)

    df.write.format("jdbc")\
            .option("url", PG_URL)\
            .option("dbtable", f"{tmp_table}") \
            .option("user", PG_PROPERTIES['user']) \
            .option("password", PG_PROPERTIES['password']) \
            .option("driver", PG_PROPERTIES['driver']) \
            .option("batchsize", 10000) \
            .option("numPartitions", 8) \
            .mode("overwrite")\
            .save()
    
    columns = df.columns
    col_list=  ", ".join(columns)
    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c not in pk_col])
    conflict_cols = ", ".join(pk_col)
        
    cur.execute(f"""
    INSERT INTO {STAGE_SCHEMA}.{table} ({col_list})
        SELECT DISTINCT ON ({conflict_cols}) {col_list} 
        FROM {tmp_table}
        ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}
    """)

    cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
    con.commit()
    

def full_load_to_stage(spark, table, cur, con):
    df = read_raw_tables(spark, table)
    print(f"  → Read {RAW_SCHEMA}.{table} from Postgres => {df.count()} records")

    cur.execute(f"SELECT COUNT(1) FROM {STAGE_SCHEMA}.{table}")
    db_count = cur.fetchone()[0]

    if db_count != df.count():
        # truncate preserves schema, overwrite destroys it
        cur.execute(f"TRUNCATE TABLE {STAGE_SCHEMA}.{table}")
        con.commit()

        df.write.format("jdbc") \
            .option("url", PG_URL) \
            .option("dbtable", f"{STAGE_SCHEMA}.{table}") \
            .option("user", PG_PROPERTIES['user']) \
            .option("password", PG_PROPERTIES['password']) \
            .option("driver", PG_PROPERTIES['driver']) \
            .mode("append") \
            .save()
        print(f"  → Loaded into {STAGE_SCHEMA}.{table}")
    else:
        print(f"  → Counts match, skipping")


    

def main():
    spark = SparkSession.builder.appName('Data Loader App')\
            .config("spark.jars", JARS).config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
            .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    psql_conn = psycopg2.connect(host="localhost", dbname="retail", user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"))
    curr = psql_conn.cursor()
    

    for table in STATIC_TABLES:
        print(f"Starting Load for: {table:>5}\n {"="*60}\n")
        full_load_to_stage(spark, table, curr, psql_conn)
        print(f"\nCompleted Load for: {table:>5}\n{"="*60}\n\n")

    print(f"{"="*30} STARTING INCREMENTAL LOAD {"="*30}\n")
    time.sleep(1)

    for table, config in INCREMENTAL_TABLES.items():
        print(f"Starting Load for: {table:>5}\n {"="*60}\n")
        incremental_load_to_stage(spark, table, config, curr, psql_conn)
        print(f"\nCompleted Load for: {table:>5}\n{"="*60}\n\n")
        

    curr.close()
    psql_conn.close()




if __name__ == "__main__":
    main()