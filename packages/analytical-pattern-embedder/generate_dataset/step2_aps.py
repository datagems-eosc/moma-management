"""
Step 2, APs: one AP per operator, and chains of operators that fit together.

How an AP is built, and how two APs relate, is in schemas/ap.py.
"""

import logging
import random

from generate_dataset.schemas.ap import AP
from generate_dataset.schemas.operator import Operator, OperatorChain

logger = logging.getLogger(__name__)

MAX_CHAIN_LENGTH = 4


def generate_aps(
    operators: list[Operator], count: int, required: list[OperatorChain], rng: random.Random
) -> list[AP]:
    """One AP per operator, the `required` chains and `count` composed chains drawn at
    random. An AP that comes out with the same description as an earlier one is dropped:
    the embedder could not tell them apart."""
    aps: list[AP] = []
    descriptions = set[str]()
    by_label = {op.label: op for op in operators}
    for chain in generate_operator_chains(operators, count, required, rng):
        ap = AP.build([by_label[label] for label in chain])
        if ap.description not in descriptions:
            descriptions.add(ap.description)
            aps.append(ap)
    logger.info(
        f"{len(operators)} operators, {len(aps)} APs ({len(aps) - len(operators)} composed)")
    return aps


def generate_operator_chains(
    operators: list[Operator], count: int, required: list[OperatorChain], rng: random.Random
) -> list[OperatorChain]:
    """
    The one-operator chains, then up to `count` distinct chains of 2 to MAX_CHAIN_LENGTH
    operators drawn by random walks over the "can feed" graph. The `required` chains the
    walks did not draw are put in front of those: requiring a chain the catalogue already
    has changes nothing.

    Half of the time a chain's shorter sub-chains are added too. A catalogue where
    `A > B > C` sits next to `A > B` and `B > C` is what makes the matching hard, and
    what gives a task building blocks besides its exact AP.
    """
    one_operator_chains = [(op.label,) for op in operators]

    # The operators each operator can be followed by.
    successors = {
        a.label: [b.label for b in operators if b is not a and a.feeds(b)]
        for a in operators
    }
    # Only an operator with a successor can start a chain.
    starts = sorted(label for label, following in successors.items() if following)
    chains: dict[OperatorChain, None] = {}

    # Lengths 2, 3, 4 are drawn 4:2:1: the longer a chain, the less likely.
    lengths = list(range(2, MAX_CHAIN_LENGTH + 1))
    weights = [2 ** (MAX_CHAIN_LENGTH - length) for length in lengths]
    attempts = 0
    while starts and len(chains) < count and attempts < count * 50:
        attempts += 1
        target = rng.choices(lengths, weights)[0]
        chain = [rng.choice(starts)]
        while len(chain) < target:
            following = [label for label in successors[chain[-1]] if label not in chain]
            if not following:
                break
            chain.append(rng.choice(following))
        if len(chain) < 2:
            continue
        chains.setdefault(tuple(chain))
        if rng.random() < 0.5:
            for length in range(2, len(chain)):
                for start in range(len(chain) - length + 1):
                    chains.setdefault(tuple(chain[start:start + length]))
    drawn = list(chains)[:count]
    return one_operator_chains + [chain for chain in dict.fromkeys(required) if chain not in drawn] + drawn
