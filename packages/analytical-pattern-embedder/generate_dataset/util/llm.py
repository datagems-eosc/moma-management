"""The LLM that writes the dataset, behind a disk cache."""

import hashlib
import os
from pathlib import Path

import requests
from pydantic import BaseModel


class LLM:
    """The API that writes the dataset. Answers are cached on disk, one file per question."""
    ALLOWED_WORKERS = 4

    def __init__(self, cache: Path) -> None:
        self.cache = cache
        self.calls = 0
        cache.mkdir(parents=True, exist_ok=True)

    def ask[T: BaseModel](self, instructions: str, question: str, answer_type: type[T]) -> T:
        """The LLM's answer, as an `answer_type`. Tried three times, since timeouts and
        malformed JSON happen; after that the script stops, rather than build a dataset
        with holes in it."""
        key = hashlib.sha256(
            f"{instructions}\n{question}".encode()).hexdigest()
        file = self.cache / f"{key}.json"
        if file.exists():
            return answer_type.model_validate_json(file.read_text())

        api_base, model, api_key = (
            os.getenv(f"LLM_API_{name}") for name in ("BASE", "MODEL", "KEY"))
        if not api_base or not model:
            raise SystemExit(
                "Set LLM_API_BASE and LLM_API_MODEL (and LLM_API_KEY if the API needs one).")
        self.calls += 1
        if self.calls % 25 == 0:
            print(f"  {self.calls} LLM calls so far...", flush=True)
        for _ in range(3):
            try:
                response = requests.post(
                    f"{api_base.rstrip('/')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}"} if api_key else {},
                    json={
                        # AP Management names its model the LiteLLM way, "openai/<model>".
                        "model": model.split("/", 1)[-1],
                        "messages": [{"role": "system", "content": instructions},
                                     {"role": "user", "content": question}],
                        "response_format": {"type": "json_schema", "json_schema": {
                            "name": answer_type.__name__,
                            "schema": answer_type.model_json_schema(),
                            "strict": True,
                        }},
                    },
                    timeout=300,
                    verify=os.getenv("LLM_SSL_VERIFY",
                                     "true").lower() == "true",
                )
                response.raise_for_status()
                answer = answer_type.model_validate_json(
                    response.json()["choices"][0]["message"]["content"])
            except Exception as e:
                error = e
                continue
            # Written then renamed: an interrupted run must not leave a file that breaks the next one.
            partial = file.with_suffix(".tmp")
            partial.write_text(answer.model_dump_json())
            partial.replace(file)
            return answer
        print(f"  LLM call failed three times: {str(error)[:200]}", flush=True)
        raise SystemExit(
            "The LLM left a question unanswered. Run again: the answers already given are kept.")
