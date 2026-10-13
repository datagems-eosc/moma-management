"""
Step 1, operators: the building blocks the APs are made of.

The real ones are read from the AP files of dataset/inputs/real_aps and tagged by the
LLM with a domain and a `kind` per port. The LLM then writes new ones for each domain, so that
they chain with what exists.
"""

import json
import logging
from pathlib import Path

from pydantic import BaseModel

from generate_dataset.config import DOMAINS
from generate_dataset.schemas.operator import Operator, Port
from generate_dataset.util.llm import LLM
from generate_dataset.util.string import snake
from moma_management.domain.analytical_pattern import AnalyticalPattern

logger = logging.getLogger(__name__)

OPERATORS_PER_DOMAIN = 8
PORT_TYPES = ("string", "number", "boolean", "array", "object")


class OperatorBatch(BaseModel):
    operators: list[Operator]


class PortKind(BaseModel):
    label: str
    port: str
    kind: str


class OperatorDomain(BaseModel):
    label: str
    domain: str


class RealOperatorTags(BaseModel):
    kinds: list[PortKind]
    domains: list[OperatorDomain]


def generate_operators(llm: LLM, real_aps_dir: Path, domains: list[str]) -> list[Operator]:
    """
    Return a mix of real and generated operators about the specified domains.

    Args:
        llm: the LLM to ask for new operators and for port kinds and operator domains
        real_aps_dir: the directory where to read the real APs, which provide the real operators
        domains: the domains for which to generate new operators, each one of DOMAINS

    Returns:
        The operators, real ones first. Each has a domain and each of its ports a kind.
    """

    # Retrieve all operator from the real APs.
    ops = _list_operator_in_aps(real_aps_dir)
    # Assign them to a domain and give each port a kind, so that the LLM can write new operators that chain with them.
    ops = _annotate_operators_batch(llm, ops)
    logger.info(
        f"{len(ops)} real operators: {', '.join(op.name for op in ops)}"
    )

    # Now we have some real operators. We need more to actually generate APs, so we generate new ones for each domain, so that they chain with the real ones.
    for domain in domains:
        logger.info(f"Generating new operators for domain: {domain}")
        ops += _generate_new_operators(llm, domain, ops)

    return ops


def _list_operator_in_aps(directory: Path) -> list[Operator]:
    """
    Read all APs in the given directory and return a list of all typed operators found, with
    their label, name, description, inputs and outputs. 

    /!\\ Operators with no typed ports or Magic_* operators are skipped.
    """
    found: dict[str, Operator] = {}
    # Read all APs...
    for path in sorted(directory.glob("*.json")):

        try:
            ap = AnalyticalPattern.model_validate_json(path.read_text())
        except ValueError as error:
            raise SystemExit(
                f"{path.name} is not a valid AP: {str(error)[:400]}")

        # Add every operator to the valid pool for generation
        # There are three cases where an operator is not added:
        # - it has no typed ports
        # - or it is a Magic_* one.
        # - it's a duplicate
        for node in ap.operators:
            properties = node.properties
            inputs = properties.get("inputs")
            outputs = properties.get("outputs")

            # Verify that the op is typed
            if not isinstance(inputs, list) or not isinstance(outputs, list):
                logger.warning(
                    f"{path.name} operator {node.labels} has no typed ports, skipping"
                )
                continue

            # Add op to hashmap if it is not a Magic_* one
            label = next(
                label for label in node.labels if label.lower() != "operator")
            if label.startswith("Magic_"):
                logger.warning(
                    f"{path.name} operator {label} is a Magic_* operator, skipping"
                )
                continue

            if label in found:
                continue

            found[label] = Operator(
                label=label,
                name=properties.get("name", label),
                description=properties.get("description", ""),
                inputs=[Port.from_property(p) for p in inputs],
                outputs=[Port.from_property(p) for p in outputs],
                real=True,
            )

    if not found:
        raise SystemExit(f"No AP file with typed operators in {directory}")

    return list(found.values())


def _annotate_operators_batch(llm: LLM, operators: list[Operator]) -> list[Operator]:
    """
    Ask the LLM to give every port a `kind` and every operator a `domain`. The kinds are
    used to chain operators together, and the domains are used to generate new operators
    for each domain.

    For example, the Text_To_SQL operator has an output port `query` of type `string`. 
    The LLM tags it with the kind `sql_query`, so that it can be chained to the SQL_Query operator

    This is kind of a hack to generate many APs that chains together without having a lot of real APs.

    NOTE: This should be eventually replaced by a more robust approach once we have a lot of real APS.
    """
    tags = llm.ask(
        KIND_PROMPT,
        json.dumps([op.to_dict(with_kinds=False)
                   for op in operators], indent=1),
        RealOperatorTags,
    )

    kinds = {(k.label, k.port): snake(k.kind) for k in tags.kinds}
    domains = {d.label: d.domain for d in tags.domains if d.domain in DOMAINS}

    for op in operators:
        op.domain = domains.get(op.label, "")
        for port in op.ports:
            port.kind = kinds.get((op.label, port.name)) or snake(port.name)

    return operators


def _generate_new_operators(llm: LLM, domain: str, pool: list[Operator]) -> list[Operator]:
    """New operators for one domain, written by the LLM so that they chain with `pool`:
    the real operators and the ones already written for the domains before this one."""

    # Up to three operators of the domain as examples, or any three when it has none yet.
    MAX_EXAMPLES = 3
    ops_of_domain = [op for op in pool if op.domain == domain]
    examples_ops = (ops_of_domain or pool)[:MAX_EXAMPLES]

    kinds = "\n".join(
        f"- {kind}: {type_}" for kind, type_ in sorted(Operator.type_by_kind(pool).items())
    )
    existing = "\n".join(
        f"- {op.name}: {op.description}" for op in pool[-60:]
    )
    shown = json.dumps(
        [op.to_dict(with_kinds=True) for op in examples_ops], indent=1)

    # Every character of the question is part of the cache key: rewording it asks the LLM
    # again for this domain and for everything built on its operators.
    question = "\n".join([
        f"Domain: {domain} -- {DOMAINS[domain]}",
        "",
        f"Write {OPERATORS_PER_DOMAIN} operators for this domain.",
        "",
        "Kind vocabulary so far (kind: type):",
        kinds,
        "",
        "Operators that already exist (do not duplicate them):",
        existing,
        "",
        "Examples of the expected form:",
        shown,
    ])
    batch = llm.ask(OPERATOR_PROMPT, question, OperatorBatch)

    # Skip the operators that duplicate one of the pool or of this batch, and the malformed ones
    taken = (
        {op.label.lower() for op in pool}
        | {op.name.lower() for op in pool}
    )
    generated_operators = []
    for new_op in batch.operators:
        label = new_op.pascal_label()

        # Do not add if duplicate
        if label.lower() in taken or new_op.name.lower() in taken:
            continue
        # Do not add if malformed
        if not new_op.inputs or not new_op.outputs or any(port.type not in PORT_TYPES for port in new_op.ports):
            continue

        taken |= {label.lower(), new_op.name.lower()}
        generated_operators.append(Operator(
            label=label,
            name=new_op.name.strip(),
            description=new_op.description.strip(),
            inputs=Port.unique(new_op.inputs),
            outputs=Port.unique(new_op.outputs),
            domain=domain,
        ))

    logger.info(f"  {domain}: +{len(generated_operators)} operators")

    return generated_operators


DOMAIN_LIST = "\n".join(
    f"- {name}: {covers}" for name, covers in DOMAINS.items())

KIND_PROMPT = f"""
You are given data-processing operators as JSON. Each has typed input and output ports.

Give every port a `kind`: a short snake_case tag for what the value IS, independent of
the port's name. Two ports of different operators must get the SAME kind exactly when
one could be plugged into the other -- for instance an output `query` holding an SQL
statement and an input `sql` expecting one are both `sql_query`.

Return in `kinds` one entry per port: the operator `label`, the `port` name, and its
`kind`. Return in `domains` one entry per operator: its `label` and the name of the
domain below it belongs to.

Domains:
{DOMAIN_LIST}"""

OPERATOR_PROMPT = f"""
You design operators for a catalogue of Analytical Patterns. An operator is one reusable
data-processing step with typed inputs and outputs; operators are chained into pipelines.

Rules:
- An operator does ONE thing. Its description is one or two plain sentences saying what
  it does, in the style of the examples.
- Operators must be distinct, but make several of them close siblings: same kind of
  data, different purpose. Telling them apart should require understanding the task.
- Ports: 1 to 3 inputs, 1 or 2 outputs. `name` is a short snake_case identifier.
  `type` is one of: {", ".join(PORT_TYPES)}.
- `kind` is a snake_case tag for what the value IS (sql_query, table_rows, plain_text).
  Two ports can be wired together only when their kind AND type are equal, so:
  - reuse a kind from the vocabulary whenever one fits, with the type it has there;
  - make the batch chainable: at least half of the operators must take as input a kind
    that another operator -- in this batch or already in the vocabulary -- produces.
- `label` is Pascal_Case_With_Underscores, ends in _Operator, and is unique.
"""
