import json
import os
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from decimal import Decimal
from datetime import datetime
load_dotenv()

SCHEMA = "retail_staging"
OUTPUT_PATH = "/home/vivek/Projects/data-assist/agent/metadata.json"
DB_CONFIG = {
            "host" : os.getenv("PG_HOST"),
            "dbname" : os.getenv("PG_DB"),
            "user" : os.getenv("PG_USER"),
            "password" : os.getenv("PG_PASSWORD"),
            "port": 5432
}


class MetadataGenerator:
    def __init__(self, db_config: dict, schema: str, output_path: str):
        self.db_config = db_config
        self.schema = schema
        self.output_path = output_path
        self.conn = None
        self.cursor = None
    
    def connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


    def get_tables(self) -> list[str]:
        self.cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (self.schema, )
        )
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_columns(self, table:str) -> list[dict]:
        self.cursor.execute(
            """
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            where table_name = %s and table_schema = %s
            ORDER BY ordinal_position
            """,
            (table, self.schema, )
        )
        return [{
            "column_name": row[0],
            "data_type": row[1],
            "is_null": row[2]
        }  for row in self.cursor.fetchall()]
    


    def get_primary_keys(self, table: str) -> set[str]:
        self.cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            """,
            (self.schema, table)
        )
        return {row[0] for row in self.cursor.fetchall()}

    
    def get_foreign_keys(self, table: str) -> list[dict]:
        self.cursor.execute(
            """
            SELECT
                kcu.constraint_name,
                kcu.column_name,
                ccu.table_name  AS references_table,
                ccu.column_name AS references_column
            FROM information_schema.referential_constraints rc
            JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = rc.constraint_name
            AND kcu.table_schema = rc.constraint_schema
            JOIN information_schema.key_column_usage ccu
            ON ccu.constraint_name = rc.unique_constraint_name
            AND ccu.table_schema = rc.unique_constraint_schema
            AND ccu.ordinal_position = kcu.ordinal_position
            WHERE kcu.table_schema = %s
            AND kcu.table_name = %s
            ORDER BY kcu.constraint_name, kcu.ordinal_position
            """,
            (self.schema, table)
        )
        rows = self.cursor.fetchall()

        constraints = {}
        for constraint_name, col_name, ref_table, ref_col in rows:
            if constraint_name not in constraints:
                constraints[constraint_name] = {
                    "columns": [],
                    "references_table": ref_table,
                    "references_columns": [],
                }
            constraints[constraint_name]["columns"].append(col_name)
            constraints[constraint_name]["references_columns"].append(ref_col)

        result = []
        for fk in constraints.values():
            if len(fk["columns"]) == 1:
                result.append({
                    "column": fk["columns"][0],
                    "references_table": fk["references_table"],
                    "references_column": fk["references_columns"][0],
                })
            else:
                result.append({
                    "columns": fk["columns"],
                    "references_table": fk["references_table"],
                    "references_columns": fk["references_columns"],
                })
        return result

    def get_row_count(self, table: str) -> int:
        query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(self.schema),
            sql.Identifier(table)
        )

        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_sample_value(self, table: str, column: str) -> list:
        query = sql.SQL("""
            SELECT DISTINCT {col} FROM {schema}.{table}
            WHERE {col} IS NOT NULL LIMIT 3
        """).format(
            col = sql.Identifier(column),
            schema = sql.Identifier(self.schema),
            table = sql.Identifier(table),
        )
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        samples = []
        for row in rows:
            val = row[0]
            if isinstance(val, Decimal):
                val = float(val)
            samples.append(val)
        return samples

    def build_table_metadata(self, table: str):
        columns = self.get_columns(table)
        primary_keys = self.get_primary_keys(table)
        foreign_keys = self.get_foreign_keys(table)
        row_count = self.get_row_count(table)
        fkeys = {}

        if foreign_keys:
            for fk in foreign_keys:
                if 'column' in fk:
                    fkeys[fk["column"]] = {
                    "references_table": fk["references_table"],
                    "references_column": fk["references_column"],
                    }
                else:
                    for col, ref_col in zip(fk["columns"], fk["references_columns"]):
                        fkeys[col] = {
                            "references_table": fk["references_table"],
                            "references_column": ref_col,
                            "composite_key": True,
                            "paired_with": [c for c in fk["columns"] if c != col],
                        }



        columns_data = {}
        for col in columns:
            colname = col['column_name']
            datatype = col['data_type']
            isnull = col['is_null']
            isfk = colname in fkeys

            col_entry = {
                "data_type": datatype,
                "nullable": isnull,
                "is_primary_key": colname in primary_keys,
                "is_foreign_key": isfk
            }

            if isfk:
                col_entry["references"] = {
                    "table": fkeys[colname]["references_table"],
                    "column": fkeys[colname]["references_column"],
                }
            col_entry['sample_values'] = self.get_sample_value(table, colname)

            columns_data[colname] = col_entry
        return {
            "row_count": row_count,
            "columns": columns_data,
            "foreign_key": foreign_keys
        }




    def generate_data(self):
        self.connect()
        try:
            tables = self.get_tables()
            metadata = {
                "schema": self.schema,
                "extracted_at": datetime.now(),
                "tables": {}
            }
            for table in tables:
                print(f"Processing... {table}")
                metadata['tables'][table] = self.build_table_metadata(table)
            
            with open(self.output_path, "w") as file:
                json.dump(metadata, file, indent = 2, default=str)

            print(f"\n Metadata written to: {self.output_path}")
        finally:
            self.disconnect()




if __name__ == "__main__":
    generator = MetadataGenerator(
        db_config=DB_CONFIG,
        schema=SCHEMA,
        output_path=OUTPUT_PATH,
    )
    print(generator.generate_data())