import os
import requests
from dotenv import load_dotenv

load_dotenv()


class LLMClient:

    def __init__(self):
        self.api_key = os.getenv("NIM_API_KEY")
        self.base_url = os.getenv("NIM_BASE_URL")
        self.model = os.getenv("NIM_MODEL")

        if not self.api_key:
            raise ValueError("NIM_API_KEY not found in environment")

    def _build_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _build_payload(self, system_prompt: str, user_message: str) -> dict:
        return {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.3,
            "max_tokens": 1024,
        }

    def chat(self, system_prompt: str, user_message: str) -> str:
        payload = self._build_payload(system_prompt, user_message)
        response = requests.post(
            self.base_url,
            headers=self._build_headers(),
            json=payload,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"].strip()
    

if __name__ == "__main__":
    client = LLMClient()
    response = client.chat(
        system_prompt="You are a helpful data engineering assistant.",
        user_message="What is a foreign key? Answer in one sentence."
    )
    print(response)