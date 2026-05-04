import json
import os

METADATA_PATH = os.path.join(os.path.dirname(__file__), "metadata.json")
BUSINESS_CONTEXT_PATH = os.path.join(os.path.dirname(__file__), "business_context.json")


class PromptBuilder:

    def __init__(self, metadata_path: str = METADATA_PATH, business_context_path: str = BUSINESS_CONTEXT_PATH):
        self.metadata = self._load_json(metadata_path)
        self.business_context = self._load_json(business_context_path)

    def _load_json(self, path: str) -> dict:
        with open(path, "r") as f:
            return json.load(f)

    def _render_schema(self) -> str:
        lines = []
        tables = self.metadata["tables"]

        for table_name, table_data in tables.items():
            lines.append(f"Table: {table_name} ({table_data['row_count']} rows)")

            for col_name, col in table_data["columns"].items():
                parts = [f"  {col_name:<30} {col['data_type']:<20}"]

                tags = []
                if col["is_primary_key"]:
                    tags.append("PK")
                if col["is_foreign_key"]:
                    ref = col.get("references", {})
                    tags.append(f"FK -> {ref.get('table')}.{ref.get('column')}")
                if col.get("sample_values"):
                    samples = ", ".join(str(v) for v in col["sample_values"])
                    tags.append(f"samples: {samples}")

                parts.append(" | ".join(tags))
                lines.append("".join(parts))

            lines.append("")

        return "\n".join(lines)

    def _render_business_context(self) -> str:
        bc = self.business_context
        lines = [f"Domain: {bc['domain']}"]

        lines.append("\nEncoded values:")
        for field, mapping in bc.get("encoded_values", {}).items():
            pairs = ", ".join(f"{k}={v}" for k, v in mapping.items())
            lines.append(f"  {field}: {pairs}")

        lines.append("\nDerived metrics:")
        for metric, formula in bc.get("derived_metrics", {}).items():
            lines.append(f"  {metric}: {formula}")

        lines.append("\nGotchas:")
        for gotcha in bc.get("gotchas", []):
            lines.append(f"  - {gotcha}")

        return "\n".join(lines)

    def build_system_prompt(self) -> str:
        schema = self._render_schema()
        business_context = self._render_business_context()

        return f"""You are a database intelligence agent for a retail supply chain database.
You have full knowledge of the schema and business domain below.

=== BUSINESS CONTEXT ===
{business_context}

=== DATABASE SCHEMA ===
{schema}

=== RULES ===
- Strictly answer questions adhering to the database context, dont answer unrelated topics.
- For schema or metadata questions — answer directly from the context above, do not query the database
- For data questions — generate a single valid PostgreSQL SQL query wrapped in ```sql``` blocks
- Always use fully qualified table names with schema prefix: retail_staging.<table>
- For revenue calculations always use net_revenue formula from derived metrics above
- Never use SELECT * — always name columns explicitly
- If the question is ambiguous — state your assumption before answering
- Keep answers concise and business-friendly, not raw data dumps
- Provide the query used to obtain the result only if the user explicitly asked for the query.
"""

    def build_user_message(self, question: str) -> str:
        return question.strip()
    
if __name__ == "__main__":
    builder = PromptBuilder()
    print(builder.build_system_prompt())