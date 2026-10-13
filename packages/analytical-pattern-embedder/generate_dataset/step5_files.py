"""
Step 5, files: dataset/generated/training for train.py, dataset/generated/evaluation for
benchmark.py, and the APs as PG-JSON for AP Management.
"""

import json
import random
from collections import Counter
from pathlib import Path

from generate_dataset.schemas.ap import AP, HARD_NEGATIVES, Task
from generate_dataset.schemas.dataset import Dataset


def write_files(
    out: Path,
    training: Dataset,
    evaluation: Dataset,
    real_tasks: list[dict],
    rng: random.Random,
) -> None:
    aps = evaluation.aps
    by_chain = {ap.chain: ap for ap in aps}
    train_aps = [ap for ap in training.aps if ap.split == "train"]

    def task_line(task: Task, gold: AP, candidates: list[AP]) -> dict:
        related = {ap.id: ap.relation_to(gold) for ap in candidates}
        return {
            "task": task.text,
            "style": task.style,
            # seen: new wording for an AP trained on. unseen: an AP with an operator never trained on.
            "set": "seen" if gold.split == "train" else "unseen",
            "ap": gold.id,
            "building_blocks": [id_ for id_, r in related.items() if r == "building_block"],
            "supersets": [id_ for id_, r in related.items() if r == "superset"],
        }

    train_tasks, dev_tasks, test_tasks = [], [], []
    for ap in sorted(training.aps, key=lambda ap: ap.id):
        for task in training.tasks[ap.id]:
            if task.use == "dev":
                # Ranked among the train and dev APs.
                dev_tasks.append(task_line(task, ap, training.aps))
            else:
                # Training only ever involves training APs, as positives and as negatives.
                negatives = ap.hard_negatives(train_aps, rng)
                if len(negatives) == HARD_NEGATIVES:
                    train_tasks.append({**task_line(task, ap, train_aps),
                                        "negatives": [negative.id for negative in negatives]})
    for ap in sorted(aps, key=lambda ap: ap.id):
        for task in evaluation.tasks[ap.id]:
            test_tasks.append(task_line(task, ap, aps))
    for task in real_tasks:
        if not task["gold_ops"]:
            continue  # labelled as something no real AP answers
        gold = by_chain.get(tuple(task["gold_ops"]))
        if gold is None:
            raise SystemExit(f"No AP in the catalogue for the real task chain {task['gold_ops']}")
        test_tasks.append({**task_line(Task(text=task["text"], style="user"), gold, aps), "set": "real"})

    def ap_line(ap: AP) -> dict:
        return {
            "id": ap.id,
            "name": ap.name,
            "split": ap.split,
            # Made of real operators only: the catalogue as it stands today.
            "real": all(op.real for op in ap.operators),
            "operators": list(ap.chain),
            "description": ap.description,
            "root_description": ap.root_description,
            # AP Management's Composer hands a composed AP the root of its first AP.
            "root_description_as_composed": by_chain.get(ap.chain[:1], ap).root_description,
        }

    def write(file: Path, lines: list[dict]) -> None:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("".join(json.dumps(line) + "\n" for line in lines))

    lines = [ap_line(ap) for ap in aps]
    training_fields = ("id", "name", "split", "operators", "description")
    write(out / "training" / "aps.jsonl",
          [{key: line[key] for key in training_fields} for line in lines if line["split"] != "test"])
    write(out / "training" / "train_tasks.jsonl", train_tasks)
    write(out / "training" / "dev_tasks.jsonl", dev_tasks)
    write(out / "evaluation" / "aps.jsonl", lines)
    write(out / "evaluation" / "tasks.jsonl", test_tasks)

    print(f"APs: {dict(Counter(ap.split for ap in aps))}")
    print(f"Tasks: {len(train_tasks)} train, {len(dev_tasks)} dev, "
          f"{dict(Counter(task['set'] for task in test_tasks))} test")


def write_graphs(out: Path, aps: list[AP]) -> None:
    """The APs themselves as PG-JSON, for AP Management's end-to-end test."""
    graphs = out / "evaluation" / "aps"
    graphs.mkdir(parents=True, exist_ok=True)
    for stale in graphs.glob("*.json"):
        stale.unlink()
    for ap in aps:
        graph = ap.graph.model_dump(by_alias=True, mode="json", exclude_none=True)
        (graphs / f"{ap.id}.json").write_text(json.dumps(graph, indent=1))
