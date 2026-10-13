import argparse
import json
import logging
import random
from pathlib import Path

from generate_dataset.config import DOMAINS, HERE, REAL_APS_DIR, REAL_TASKS_FILE
from generate_dataset.schemas.operator import OperatorChain
from generate_dataset.step1_operators import generate_operators
from generate_dataset.step2_aps import generate_aps
from generate_dataset.step3_tasks import generate_questions, generate_tasks
from generate_dataset.step4_splits import split_dataset
from generate_dataset.step5_files import write_files, write_graphs
from generate_dataset.util.llm import LLM
from generate_dataset.util.string import normalize

logging.basicConfig(level=logging.INFO, format="%(message)s")


SEED = 13
COMPOSED_APS = 420


def main() -> None:
    """
    Generate the dataset the analytical pattern embedder is trained and benchmarked on.

    BEWARE THAT THIS USES LLMs AND CONSUMES A LOT OF TOKENS

    This generation is a five-step process, one file per step (see README.md):
    1. Extract Operators                Extract the operators from dataset/inputs/real_aps, and generate new ones for 22 domains
    2. Generate APs                     One per operator, and chains of 2 to 4 operators that fit together
    3. Generate Tasks                   For every AP, generate some task that would fit
    4. Split Training and Evaluation    Split the operators into training and evaluation sets
    5. Persistence                      Write the dataset files for training and evaluation
    """
    # CLI
    docstring = main.__doc__.strip().splitlines()[0]
    parser = argparse.ArgumentParser(description=docstring)
    parser.add_argument("--out", type=Path, default=HERE /
                        "dataset" / "generated")
    parser.add_argument(
        "--domains", type=int, default=len(DOMAINS),
        help="generate operators for the first N domains only"
    )
    parser.add_argument(
        "--aps_count", type=int, default=COMPOSED_APS, help="number of composed APs"
    )
    args = parser.parse_args()

    # Dependencies
    llm = LLM(cache=args.out / "llm_cache")

    # Golden tasks. These contains real questions from the actual uses cases and the reference operators chain to solve them
    real_tasks = [
        json.loads(line) for line in REAL_TASKS_FILE.read_text().splitlines() if line.strip()
    ]
    real_ops_chains: list[OperatorChain] = list(dict.fromkeys(
        tuple(t["gold_ops"]) for t in real_tasks if t["gold_ops"] and len(t["gold_ops"]) > 1))

    # Step 1 : Build a set of operators, both real and generated, for the specified domains.
    operators = generate_operators(
        llm, REAL_APS_DIR, list(DOMAINS)[:args.domains]
    )
    unknown = {label for chain in real_ops_chains for label in chain} - {op.label for op in operators}
    if unknown:
        raise SystemExit(
            f"{REAL_TASKS_FILE.name} names operators that do not exist: {', '.join(sorted(unknown))}")

    # Step 2: Use the generated operators to build APs
    aps = generate_aps(
        operators, args.aps_count, real_ops_chains, random.Random(SEED)
    )

    # Step 3: Generate tasks for each AP, by AP id. An AP without any is left out.
    tasks_by_ap_id = generate_tasks(llm, aps)
    aps = [ap for ap in aps if ap.id in tasks_by_ap_id]

    # Step 4: Split the operators, and the APs and tasks with them.
    training_ds, evaluation_ds = split_dataset(
        operators, aps, tasks_by_ap_id, SEED + 1)

    # More questions for the training APs that start from one. Asked once the split is
    # known, and kept apart from the other tasks, so that nothing else moves.
    taken = {normalize(task.text) for tasks in tasks_by_ap_id.values() for task in tasks}
    taken |= {normalize(task["text"]) for task in real_tasks}
    questions = generate_questions(
        llm, [ap for ap in training_ds.aps if ap.split == "train"], taken)
    print(f"{sum(map(len, questions.values()))} more questions for {len(questions)} APs")

    # Step 5: Write the files.
    write_files(args.out, training_ds, evaluation_ds,
                real_tasks, random.Random(SEED + 3), questions)
    write_graphs(args.out, evaluation_ds.aps)
    print(f"Dataset written to {args.out}")


if __name__ == "__main__":
    main()
