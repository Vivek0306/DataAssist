import re
import json
from llm_client import LLMClient
from prompt_builder import PromptBuilder
from query_runner import QueryRunner

METADATA_KEYWORDS = [
    "schema", "table", "column", "what tables", "what columns",
    "data type", "primary key", "foreign key", "how many tables",
    "describe", "structure", "relationship", "what is the"
]


class Agent:

    def __init__(self):
        self.llm = LLMClient()
        self.prompt_builder = PromptBuilder()
        self.query_runner = QueryRunner()
        self.system_prompt = self.prompt_builder.build_system_prompt()

    def _is_metadata_question(self, question: str) -> bool:
        normalized = question.lower()
        return any(keyword in normalized for keyword in METADATA_KEYWORDS)

    def _extract_sql(self, response: str) -> str | None:
        match = re.search(r"```sql\s*(.*?)\s*```", response, re.DOTALL)
        if match:
            return match.group(1).strip()
        return None

    def _format_results(self, columns: list, rows: list) -> str:
        if not rows:
            return "The query returned no results."

        lines = [", ".join(columns)]
        for row in rows:
            lines.append(", ".join(str(v) for v in row))

        return "\n".join(lines)

    def _answer_with_data(self, question: str, results_text: str) -> str:
        user_message = f"""
The user asked: {question}

The query returned these results:
{results_text}

Summarize the results in a concise, business-friendly natural language answer.
Do not repeat the raw data row by row — highlight the key insight.
- Provide the query used to obtain the result only if the user question explicitly mentions they need the query.
"""
        return self.llm.chat(
            system_prompt=self.system_prompt,
            user_message=user_message,
        )

    def ask(self, question: str) -> str:
        if self._is_metadata_question(question):
            return self.llm.chat(
                system_prompt=self.system_prompt,
                user_message=question,
            )

        first_response = self.llm.chat(
            system_prompt=self.system_prompt,
            user_message=question,
        )

        sql = self._extract_sql(first_response)

        if not sql:
            return first_response

        result = self.query_runner.run(sql)

        if not result["success"]:
            return f"I generated this SQL but it failed to execute:\n\n```sql\n{sql}\n```\n\nError: {result['error']}"

        results_text = self._format_results(result["columns"], result["rows"])
        return self._answer_with_data(question, results_text)


if __name__ == "__main__":
    agent = Agent()
    print("Database Intelligence Agent ready. Type 'exit' to quit.\n")

    while True:
        question = input("You: ").strip()
        if question.lower() in ("exit", "quit"):
            break
        if not question:
            continue

        answer = agent.ask(question)
        print(f"\nAgent: {answer}\n")