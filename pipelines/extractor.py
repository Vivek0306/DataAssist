import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from dotenv import load_dotenv

load_dotenv()

JARS = "/home/vivek/jars/postgresql-42.7.3.jar"

TABLES = [
    "REGION", "NATION", "CUSTOMER", "SUPPLIER",
    "PART", "PARTSUPP", "ORDERS", "LINEITEM"
]
PG_URL = "jdbc:postgresql://localhost:5432/retail"
PG_PROPERTIES = {
    "user": os.getenv('PG_USER'),
    "password": os.getenv('PG_PASSWORD'),
    "driver": "org.postgresql.Driver"
}
SCHEMAS = {
    "REGION": StructType([
        StructField("R_REGIONKEY", LongType(), False),
        StructField("R_NAME", StringType(), False),
        StructField("R_COMMENT", StringType(), True)
    ]),
    "NATION": StructType([
        StructField("N_NATIONKEY", LongType(), False),
        StructField("N_NAME", StringType(), False),
        StructField("N_REGIONKEY", LongType(), False),
        StructField("N_COMMENT", StringType(), True)
    ]),
    "CUSTOMER": StructType([
        StructField("C_CUSTKEY", LongType(), False),
        StructField("C_NAME", StringType(), False),
        StructField("C_ADDRESS", StringType(), False),
        StructField("C_NATIONKEY", LongType(), False),
        StructField("C_PHONE", StringType(), False),
        StructField("C_ACCTBAL", DecimalType(12, 2), False),
        StructField("C_MKTSEGMENT", StringType(), True),
        StructField("C_COMMENT", StringType(), True)
    ]),
    "SUPPLIER": StructType([
        StructField("S_SUPPKEY", LongType(), False),
        StructField("S_NAME", StringType(), False),
        StructField("S_ADDRESS", StringType(), False),
        StructField("S_NATIONKEY", LongType(), False),
        StructField("S_PHONE", StringType(), False),
        StructField("S_ACCTBAL", DecimalType(12, 2), False),
        StructField("S_COMMENT", StringType(), True)
    ]),
    "PART": StructType([
        StructField("P_PARTKEY", LongType(), False),
        StructField("P_NAME", StringType(), False),
        StructField("P_MFGR", StringType(), False),
        StructField("P_BRAND", StringType(), False),
        StructField("P_TYPE", StringType(), False),
        StructField("P_SIZE", LongType(), False),
        StructField("P_CONTAINER", StringType(), False),
        StructField("P_RETAILPRICE", DecimalType(12, 2), False),
        StructField("P_COMMENT", StringType(), True)
    ]),
    "PARTSUPP": StructType([
        StructField("PS_PARTKEY", LongType(), False),
        StructField("PS_SUPPKEY", LongType(), False),
        StructField("PS_AVAILQTY", LongType(), False),
        StructField("PS_SUPPLYCOST", DecimalType(12, 2), False),
        StructField("PS_COMMENT", StringType(), True)
    ]),
    "ORDERS": StructType([
        StructField("O_ORDERKEY", LongType(), False),
        StructField("O_CUSTKEY", LongType(), True),
        StructField("O_ORDERSTATUS", StringType(), True),
        StructField("O_TOTALPRICE", DecimalType(12, 2), True),
        StructField("O_ORDERDATE", DateType(), True),
        StructField("O_ORDERPRIORITY", StringType(), True),
        StructField("O_CLERK", StringType(), True),
        StructField("O_SHIPPRIORITY", LongType(), True),
        StructField("O_COMMENT", StringType(), True)
    ]),
    "LINEITEM": StructType([
        StructField("L_ORDERKEY", LongType(), False),
        StructField("L_PARTKEY", LongType(), False),
        StructField("L_SUPPKEY", LongType(), False),
        StructField("L_LINENUMBER", LongType(), False),
        StructField("L_QUANTITY", DecimalType(12, 2), False),
        StructField("L_EXTENDEDPRICE", DecimalType(12, 2), False),
        StructField("L_DISCOUNT", DecimalType(12, 2), False),
        StructField("L_TAX", DecimalType(12, 2), False),
        StructField("L_RETURNFLAG", StringType(), False),
        StructField("L_LINESTATUS", StringType(), False),
        StructField("L_SHIPDATE", DateType(), False),
        StructField("L_COMMITDATE", DateType(), False),
        StructField("L_RECEIPTDATE", DateType(), False),
        StructField("L_SHIPINSTRUCT", StringType(), False),
        StructField("L_SHIPMODE", StringType(), False),
        # Note: DDL shows L_COMMENT as NOT NULL
        StructField("L_COMMENT", StringType(), False) 
    ])
}   

def get_spark_session():
    # SparkSession.builder with app name and postgresql jar config
    # remember .builder is a property not a method
    return SparkSession.builder.appName('Snowflake Data Extractor')\
        .config("spark.jars", JARS).getOrCreate()

def extract_table(spark, table_name):
    return spark.read \
    .option("header", "false") \
    .schema(SCHEMAS[table_name]) \
    .csv(f"/home/vivek/Projects/data-assist/data/{table_name.lower()}/")
    


def load_to_postgres(df, table_name, conn, curr):
    """
    Write Spark DataFrame to retail_raw.<table_name> in PostgreSQL via JDBC.
    Use mode overwrite — this is the full initial load.
    JDBC URL: jdbc:postgresql://localhost:5432/retail
    Use lowercase table name.
    """
    curr.execute(f"TRUNCATE TABLE retail_raw.{table_name}")
    conn.commit()

    df.write.format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/retail") \
        .option("dbtable", f"retail_raw.{table_name}") \
        .option("user", os.getenv('PG_USER')) \
        .option("password", os.getenv('PG_PASSWORD')) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def run():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    conn = psycopg2.connect(host="localhost", dbname="retail", user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"))
    curr = conn.cursor()


    for table in TABLES:
        print(f"Extracting {table}...")
        df = extract_table(spark, table)
        print(f"  → {df.count()} rows")
        load_to_postgres(df, table.lower(), conn, curr)
        print(f"  → Loaded into retail_raw.{table.lower()}")

    spark.stop()

if __name__ == "__main__":
    run()