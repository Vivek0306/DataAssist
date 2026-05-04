import psycopg2
from faker import Faker
import os
import random
from dotenv import load_dotenv
from datetime import date, timedelta

load_dotenv()
fake = Faker('en_US')

NATION_KEY_LIMIT = 24
CUSTOMER_SIZE = 50000
ORDER_SIZE = 50000
STAGE_SCHEMA = "retail_staging"
RAW_SCHEMA = "retail_raw"

def get_last_key(cur):
    cur.execute(f'SELECT MAX(C_CUSTKEY) FROM {RAW_SCHEMA}.CUSTOMER')
    custkey = cur.fetchone()[0]

    cur.execute(f'SELECT MAX(O_ORDERKEY) FROM {RAW_SCHEMA}.ORDERS')
    orderkey = cur.fetchone()[0]

    return {"O_KEY": orderkey, "C_KEY": custkey}

def get_reference_keys(cur):
    cur.execute(f'SELECT C_CUSTKEY FROM {STAGE_SCHEMA}.CUSTOMER ORDER BY RANDOM() LIMIT 1000')
    cust_keys = [row[0] for row in cur.fetchall()]

    cur.execute(f'SELECT PS_PARTKEY, PS_SUPPKEY FROM {STAGE_SCHEMA}.PARTSUPP ORDER BY RANDOM() LIMIT 1000')
    partsupp_pairs = [(row[0], row[1]) for row in cur.fetchall()]

    return {"cust_keys": cust_keys, "partsupp_pairs": partsupp_pairs}

def generate_orders_data(cur, orderkey, cust_keys):
    ORDER_PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
    orders_data = []
    new_order_keys = []

    for i in range(1, ORDER_SIZE + 1):
        key = orderkey + i
        data = [
            key,
            random.choice(cust_keys),
            random.choice(['F', 'P', 'O']),
            round(random.uniform(1000.00, 500000.00), 2),
            date.today(),
            random.choice(ORDER_PRIORITIES),
            f"Clerk#{random.randint(1, 1000):09d}",
            0,
            fake.sentence()[:79]
        ]
        orders_data.append(data)
        new_order_keys.append(key)

    cur.executemany(f"""
        INSERT INTO {RAW_SCHEMA}.orders
        (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, 
         O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, orders_data)
    return new_order_keys



def generate_customer_data(cur, custkey):
    cur.execute(f"select distinct C_MKTSEGMENT from {STAGE_SCHEMA}.CUSTOMER")
    mktsegment = [row[0] for row in cur.fetchall()]
    cust_data = []
    for i in range(1, CUSTOMER_SIZE + 1):
        key = custkey + i
        name = fake.name()[:25]
        address =  fake.address().replace('\n', ' ')[:40]
        nationkey = random.randint(1,23)
        phone = fake.phone_number()[:15]
        acctbal = round(random.uniform(5000.0, 90000.0), 2)
        comment = fake.sentence()[:110]
        data = [key, name, address, nationkey, phone, acctbal, random.choice(mktsegment), comment ]
        cust_data.append(data)

    cur.executemany(f"""
    INSERT INTO {RAW_SCHEMA}.customer 
    (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, cust_data)


def generate_lineitems_data(cur, order_keys, partsupp_pairs):
    RETURN_FLAGS = ['N', 'A', 'R']
    LINE_STATUSES = ['O', 'F']
    SHIP_INSTRUCT = ['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN']
    SHIP_MODES = ['AIR', 'MAIL', 'SHIP', 'TRUCK', 'REG AIR', 'FOB', 'RAIL']

    lineitem_data = []
    today = date.today()

    for order_key in order_keys:
        num_items = random.randint(1, 7)
        for linenumber in range(1, num_items + 1):
            partkey, suppkey = random.choice(partsupp_pairs)
            quantity = round(random.uniform(1, 50), 2)
            extendedprice = round(random.uniform(900, 100000), 2)
            discount = round(random.uniform(0.00, 0.10), 2)
            tax = round(random.uniform(0.00, 0.08), 2)

            data = [
                order_key,
                partkey,
                suppkey,
                linenumber,
                quantity,
                extendedprice,
                discount,
                tax,
                random.choice(RETURN_FLAGS),
                random.choice(LINE_STATUSES),
                today + timedelta(days=random.randint(1, 30)),
                today + timedelta(days=random.randint(5, 20)),
                today + timedelta(days=random.randint(15, 40)),
                random.choice(SHIP_INSTRUCT),
                random.choice(SHIP_MODES),
                fake.sentence()[:44]
            ]
            lineitem_data.append(data)


    cur.executemany(f"""
        INSERT INTO {RAW_SCHEMA}.lineitem
        (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,
         L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX,
         L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE,
         L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, lineitem_data)

    return len(lineitem_data)



def main():
    conn = psycopg2.connect(host="localhost", dbname="retail", user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"))
    cur = conn.cursor()

    keys = get_last_key(cur)
    ref_keys = get_reference_keys(cur)

    print(f"Generating customer data...")
    generate_customer_data(cur, keys['C_KEY'])
    print(f"{"Loaded customer data"} {CUSTOMER_SIZE:>30} records✅")

    print(f"\nGenerating orders data...")
    new_orders = generate_orders_data(cur, keys['O_KEY'], ref_keys['cust_keys'])
    print(f"{"Loaded orders data"} {ORDER_SIZE:>30} records✅")


    print(f"\nGenerating lineitem data...")
    new_lineitems = generate_lineitems_data(cur, new_orders, ref_keys['partsupp_pairs'])
    print(f"{"Loaded lineitem data"} {new_lineitems:>30} records✅")



    conn.commit()
    cur.close()
    conn.close()



if __name__ == "__main__":
    main()