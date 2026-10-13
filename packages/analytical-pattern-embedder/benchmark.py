"""
Benchmark the analytical pattern embedder against the base model and against today's search.

    uv run benchmark.py
    uv run benchmark.py --serve

Every system ranks the APs of dataset/generated/evaluation/aps.jsonl for each task of
tasks.jsonl in the same folder, by cosine similarity. The systems differ in the model
and in the text an AP is embedded from (see SYSTEMS). The report is printed and saved
to .benchmarks/analytical_pattern_embedder_<timestamp>.md.

With --serve, nothing is benchmarked: the script answers
`POST /search {"system", "query", "top_k"}` instead, which is how AP Management's
tests/benchmark/test_matching_e2e.py puts each system behind its Matchmaker.

"""

# Unsloth patches transformers on import and must come first.
from unsloth import FastSentenceTransformer  # isort: skip

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import numpy as np
from sentence_transformers import SentenceTransformer

HERE = Path(__file__).parent
DATA = HERE / "dataset" / "generated" / "evaluation"
ADAPTER = HERE / "models" / "analytical_pattern_embedder"
REPORTS = HERE / ".benchmarks"

BASE_MODEL = "unsloth/embeddinggemma-2"
# The commit of BASE_MODEL that is trained on, benchmarked and merged into. An adapter
# only means something on the exact weights it was trained on, and `main` can move.
BASE_REVISION = "c8f89cc7ecb4ba5122cd15fdd487422b2b487daf"
# Tokens kept of a text. A step of an AP takes about 60, so this fits about 15 steps; the
# longest description of the dataset is 316. Beyond it a description loses its last steps.
MAX_SEQ_LENGTH = 1024
# moma-management's default `top_k`: what the Matchmaker gets to see.
TOP_K = 10
# Where --serve listens; AP Management's end-to-end test looks there by default.
PORT = 8765


@dataclass(frozen=True)
class System:
    name: str
    model: str
    # The field of aps.jsonl an AP is embedded from.
    ap_text: str


SYSTEMS = [
    # Today's search, as AnalyticalPatternService._embed_ap does it: MiniLM, no prompt,
    # on the root description. Neo4j scores (1 + cosine) / 2, which ranks the same.
    System("current", "MiniLM", "root_description"),
    # The same on the root description AP Management's Composer leaves on a composed
    # AP, which is the one of its first AP.
    System("current, as composed", "MiniLM", "root_description_as_composed"),
    # What the description forged from the operators brings, without changing the model.
    System("MiniLM + forged description", "MiniLM", "description"),
    System("EmbeddingGemma 2", "EmbeddingGemma 2", "description"),
    System("EmbeddingGemma 2 fine-tuned", "fine-tuned", "description"),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--serve", action="store_true", help="answer searches instead of benchmarking")
    args = parser.parse_args()
    check_score()

    aps = read_jsonl(DATA / "aps.jsonl")
    tasks = read_jsonl(DATA / "tasks.jsonl")
    systems = [s for s in SYSTEMS if s.model != "fine-tuned" or ADAPTER.exists()]
    if len(systems) < len(SYSTEMS):
        print(f"No adapter in {ADAPTER}: run train.py to benchmark the fine-tuned model.")

    models = {name: load_model(name) for name in dict.fromkeys(s.model for s in systems)}
    ap_vectors = {s.name: embed(models[s.model], [ap[s.ap_text] for ap in aps], "document") for s in systems}
    if args.serve:
        return serve(systems, models, ap_vectors, [ap["id"] for ap in aps])

    task_vectors = {name: embed(model, [task["task"] for task in tasks], "query") for name, model in models.items()}
    similarity = {s.name: task_vectors[s.model] @ ap_vectors[s.name].T for s in systems}

    every_ap = np.ones(len(aps), dtype=bool)
    real_aps = np.array([ap["real"] for ap in aps])
    # title, which tasks, among which APs
    views = [
        ("APs seen in training, tasks worded differently", lambda t: t["set"] == "seen", every_ap),
        ("APs made of operators never seen in training", lambda t: t["set"] == "unseen", every_ap),
        ("Never-seen operators, user-level tasks only",
         lambda t: t["set"] == "unseen" and t["style"] == "user", every_ap),
        ("Never-seen operators, technical tasks only",
         lambda t: t["set"] == "unseen" and t["style"] == "technical", every_ap),
        ("Real questions, among the real APs", lambda t: t["set"] == "real", real_aps),
        ("Real questions, among all APs (their labels only know the real APs, so a miss "
         "is not always an error)", lambda t: t["set"] == "real", every_ap),
    ]

    report = ["# Analytical pattern embedder benchmark", "", "| System | Model | AP text |", "|---|---|---|"]
    report += [f"| {s.name} | {s.model} | {s.ap_text} |" for s in systems]
    for title, wanted, among in views:
        rows = [i for i, task in enumerate(tasks) if wanted(task)]
        if not rows:
            continue
        report += ["", f"## {title}", "", f"{len(rows)} tasks, {among.sum()} APs.", "",
                   "| System | recall@1 | recall@10 | nDCG@10 |", "|---|---|---|---|"]
        for s in systems:
            result = score(similarity[s.name][rows][:, among], [tasks[i] for i in rows],
                           [ap for ap, kept in zip(aps, among) if kept])
            report.append(f"| {s.name} | " + " | ".join(f"{value:.3f}" for value in result.values()) + " |")

    REPORTS.mkdir(exist_ok=True)
    file = REPORTS / f"analytical_pattern_embedder_{datetime.now():%Y%m%d_%H%M%S}.md"
    file.write_text("\n".join(report) + "\n")
    print("\n".join(report))
    print(f"\nReport saved to {file}")


def load_model(name: str):
    if name == "MiniLM":
        return SentenceTransformer("all-MiniLM-L6-v2")
    # The adapter folder names its own base model and revision.
    source = {"model_name": str(ADAPTER)} if name == "fine-tuned" else {
        "model_name": BASE_MODEL, "revision": BASE_REVISION}
    return FastSentenceTransformer.from_pretrained(
        **source,
        max_seq_length=MAX_SEQ_LENGTH,
        # EmbeddingGemma 2 is multimodal; only its text tower is needed here.
        config_kwargs={"vision_config": None, "audio_config": None},
    )


def embed(model, texts: list[str], role: str) -> np.ndarray:
    """Unit vectors for `texts`, which are tasks (role "query") or AP texts (role
    "document"). EmbeddingGemma prefixes each role with its own prompt; MiniLM has none."""
    return model.encode(
        texts, prompt_name=role if role in model.prompts else None, batch_size=32,
        normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=False,
    ).astype(np.float32)


def score(similarity: np.ndarray, tasks: list[dict], aps: list[dict]) -> dict[str, float]:
    """
    How good the rankings in `similarity` are, which has one row per task and one column
    per AP. Averaged over the tasks:

    - recall@1, recall@10: the AP written for the task is first, is in the top 10.
    - ndcg@10: the same idea with partial credit. The task's own AP is worth 2 and each
      of its building blocks 1, the higher in the top 10 the better.

    APs that do what the task asks and then more (`supersets`) are left out of the
    ranking: finding one is neither a hit nor a mistake.
    """
    column = {ap["id"]: j for j, ap in enumerate(aps)}
    discount = 1 / np.log2(np.arange(TOP_K) + 2)
    results = []
    for scores, task in zip(similarity, tasks):
        gain = np.zeros(len(aps))
        gain[[column[id_] for id_ in task["building_blocks"] if id_ in column]] = 1
        gain[column[task["ap"]]] = 2
        scores = scores.copy()
        scores[[column[id_] for id_ in task["supersets"] if id_ in column]] = -np.inf

        top = gain[np.argsort(-scores, kind="stable")[:TOP_K]]
        best_possible = np.sort(gain)[::-1][:TOP_K]
        results.append({
            "recall@1": top[0] == 2,
            "recall@10": 2 in top,
            "ndcg@10": ((2 ** top - 1) * discount[:len(top)]).sum()
                       / ((2 ** best_possible - 1) * discount[:len(top)]).sum(),
        })
    return {metric: float(np.mean([result[metric] for result in results])) for metric in results[0]}


def check_score() -> None:
    """`score` on a ranking small enough to work out by hand."""
    task = {"ap": "a", "building_blocks": ["b"], "supersets": ["c"]}
    result = score(np.array([[0.5, 0.2, 0.9, 0.7]]), [task], [{"id": id_} for id_ in "abcd"])
    # c is left out, so the ranking is d, a, b: worth 0, 2, 1 where the best order is 2, 1, 0.
    ndcg = (3 / np.log2(3) + 1 / np.log2(4)) / (3 + 1 / np.log2(3))
    assert (result["recall@1"], result["recall@10"]) == (0, 1) and abs(result["ndcg@10"] - ndcg) < 1e-9


def read_jsonl(file: Path) -> list[dict]:
    return [json.loads(line) for line in file.read_text().splitlines()]


def serve(systems: list[System], models: dict, ap_vectors: dict[str, np.ndarray], ap_ids: list[str]) -> None:
    by_name = {s.name: s for s in systems}

    class Handler(BaseHTTPRequestHandler):
        def reply(self, payload: dict) -> None:
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            self.reply({"systems": list(by_name)})

        def do_POST(self) -> None:
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            system = by_name[request["system"]]
            scores = ap_vectors[system.name] @ embed(models[system.model], [request["query"]], "query")[0]
            best = np.argsort(-scores)[:int(request.get("top_k", TOP_K))]
            self.reply({"ap_ids": [ap_ids[i] for i in best], "scores": [float(scores[i]) for i in best]})

        def log_message(self, *_) -> None:
            pass

    print(f"Serving {', '.join(by_name)} on port {PORT}. Ctrl+C to stop.")
    # One request at a time: the models share a single GPU.
    # shortcut: no authentication, on every interface so that AP Management's dev container
    # can reach it. Fine for a test server on a laptop, not for anything left running.
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
