"""
Step 4, splits: which operators the model trains on and which it is only tested on.

APs and tasks follow their operators.
"""

import random

from generate_dataset.schemas.ap import AP, Task
from generate_dataset.schemas.dataset import Dataset
from generate_dataset.schemas.operator import Operator

# Besides two whole domains each, dev and test get this share of the other operators.
HELD_OUT_OPERATOR_SHARE = 0.06


def split_dataset(
    operators: list[Operator], aps: list[AP], tasks: dict[str, list[Task]], seed: int
) -> tuple[Dataset, Dataset]:
    """
    The training dataset and the evaluation dataset.

    - Training: the train and dev APs, with their train and dev tasks (`task.use`).
    - Evaluation: every AP, as candidates, with the test tasks. Test APs appear nowhere
      else, so nothing about them is seen before the benchmark.
    """
    operator_split = _split_operators(operators, random.Random(seed))
    return _split_aps_and_tasks(aps, tasks, operator_split, random.Random(seed + 1))


def _split_operators(operators: list[Operator], rng: random.Random) -> dict[str, str]:
    """
    train, dev or test for every operator, by label. Splitting by operator rather than by task is
    what lets the benchmark say how the model does on APs it has never seen any part of.

    Two whole domains go to dev and two to test, plus a few operators of the other
    domains. Real operators and their domains stay in training: they are the catalogue
    the model will serve.
    """
    real_domains = {op.domain for op in operators if op.real}
    domains = sorted({op.domain for op in operators} - real_domains - {""})
    rng.shuffle(domains)
    held_out = min(2, len(domains) // 6)
    domain_split = {domain: "dev" for domain in domains[:held_out]}
    domain_split |= {domain: "test" for domain in domains[held_out:2 * held_out]}

    split = {op.label: domain_split.get(op.domain, "train") for op in operators}
    others = sorted(op.label for op in operators if op.domain not in domain_split and not op.real)
    rng.shuffle(others)
    share = max(1, round(len(others) * HELD_OUT_OPERATOR_SHARE))
    split |= {label: "dev" for label in others[:share]}
    split |= {label: "test" for label in others[share:2 * share]}
    return split


def _split_aps_and_tasks(
    aps: list[AP],
    tasks: dict[str, list[Task]],
    operator_split: dict[str, str],
    rng: random.Random,
) -> tuple[Dataset, Dataset]:
    """
    Splits every AP, then its tasks: they are used as their AP is. Two tasks of every
    training AP are held back as well, one for dev and one for test, to measure how new
    wording for a known AP is handled. In id order so that the shuffles are reproducible.
    """
    for ap in sorted(aps, key=lambda ap: ap.id):
        ap.assign_split(operator_split)
        ap_tasks = tasks[ap.id]
        rng.shuffle(ap_tasks)
        for i, task in enumerate(ap_tasks):
            held_back = ap.split == "train" and i < 2 and len(ap_tasks) >= 4
            task.use = ("dev", "test")[i] if held_back else ap.split

    assert all(
        operator_split[label] == "train" for ap in aps if ap.split == "train" for label in ap.chain
    ), "a held-out operator leaked into a training AP"
    training_aps = [ap for ap in aps if ap.split != "test"]
    training = Dataset(
        training_aps,
        {ap.id: [task for task in tasks[ap.id] if task.use != "test"] for ap in training_aps},
    )
    evaluation = Dataset(
        aps, {ap.id: [task for task in tasks[ap.id] if task.use == "test"] for ap in aps}
    )
    return training, evaluation
