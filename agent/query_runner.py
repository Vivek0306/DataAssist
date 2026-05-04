import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("PG_DBNAME", "retail"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
}

MAX_ROWS = 100

BLOCKED_KEYWORDS = [
    "insert", "update", "delete", "drop",
    "truncate", "alter", "create", "replace"
]


class QueryRunner:

    def __init__(self, db_config: dict = DB_CONFIG, max_rows: int = MAX_ROWS):
        self.db_config = db_config
        self.max_rows = max_rows

    def _is_safe(self, query: str) -> tuple[bool, str]:
        normalized = query.strip().lower()

        for keyword in BLOCKED_KEYWORDS:
            if keyword in normalized:
                return False, f"Blocked keyword detected: '{keyword}'"

        if not normalized.startswith("select"):
            return False, "Only SELECT queries are allowed"

        return True, ""

    def _inject_limit(self, query: str) -> str:
        normalized = query.strip().rstrip(";").lower()
        if "limit" not in normalized:
            return f"{query.strip().rstrip(';')} LIMIT {self.max_rows};"
        return query

    def run(self, query: str) -> dict:
        is_safe, reason = self._is_safe(query)
        if not is_safe:
            return {
                "success": False,
                "error": reason,
                "columns": [],
                "rows": [],
            }

        query = self._inject_limit(query)

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute(query)

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            cursor.close()
            conn.close()

            return {
                "success": True,
                "error": None,
                "columns": columns,
                "rows": [list(row) for row in rows],
                "row_count": len(rows),
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "columns": [],
                "rows": [],
            }
        
if __name__ == "__main__":
    runner = QueryRunner()
    result = runner.run("""
        SELECT o_orderstatus, COUNT(*) as order_count
        FROM retail_staging.orders
        GROUP BY o_orderstatus
    """)

    if result["success"]:
        print("Columns:", result["columns"])
        for row in result["rows"]:
            print(row)
    else:
        print("Error:", result["error"])