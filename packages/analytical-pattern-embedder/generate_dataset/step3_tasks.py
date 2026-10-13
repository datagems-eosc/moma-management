"""
Step 3, tasks: what the people who need an AP would type, written by the LLM.
"""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from pydantic import BaseModel

from generate_dataset.schemas.ap import AP, Task
from generate_dataset.util.llm import LLM
from generate_dataset.util.string import normalize

# Tasks asked for per AP. User-level ones dominate: they are what today's search misses.
TASKS_PER_AP = {"user": 5, "technical": 2, "terse": 1}


class CatalogueEntry(BaseModel):
    name: str
    summary: str
    tasks: list[Task]


def generate_tasks(llm: LLM, aps: list[AP]) -> dict[str, list[Task]]:
    """
    One LLM call per AP, for its name, its root description and its tasks. The name and
    description are registered on the AP; the tasks are returned by AP id. An AP whose
    answer had no summary is left out of the result, and so out of the dataset.
    """

    # Generate a few task for each AP
    with ThreadPoolExecutor(LLM.ALLOWED_WORKERS) as threads:
        entries = list(
            threads.map(
                lambda ap: llm.ask(
                    TASK_PROMPT, ap.description, CatalogueEntry), aps
            )
        )

    tasks_by_ap: dict[str, list[Task]] = {}
    for ap, entry in zip(aps, entries):
        ap.register(entry.name.strip(), entry.summary.strip())
        operator_names = [
            normalize(op.name) for op in ap.operators if " " in op.name.strip()
        ]

        tasks: dict[str, Task] = {}
        for task in entry.tasks:
            text = " ".join(task.text.split())

            # Remove any user task that mention an op by name, that's cheating
            names_an_operator = any(
                f" {name} " in f" {normalize(text)} " for name in operator_names)
            if text and not (task.style == "user" and names_an_operator):
                tasks.setdefault(
                    normalize(text),
                    Task(text=text, style=task.style)
                )

        tasks_by_ap[ap.id] = list(tasks.values())

    # Dedup the tasks across APs:
    # if the same sentence is written for two APs, it cannot be labelled with either.
    written = Counter(
        normalize(task.text) for tasks in tasks_by_ap.values() for task in tasks)
    return {
        ap.id: [task for task in tasks_by_ap[ap.id] if written[normalize(task.text)] == 1]
        for ap in aps if ap.root_description
    }


# The real requests at the end are not in real_tasks.jsonl, which the model is evaluated on.
TASK_PROMPT = f"""
You help build a search index for a data platform. The platform holds Analytical
Patterns (APs): pipelines of one or more operators. You are given one AP, as the list
of its steps, and you write what people who need it would type.

Return:
- `name`: a short title for the AP, 3 to 6 words.
- `summary`: one sentence saying what the AP does from start to end, as its catalogue
  entry would.
- `tasks`: {sum(TASKS_PER_AP.values())} requests this AP is the right tool for:
  - {TASKS_PER_AP["user"]} with style "user": what an analyst, teacher, researcher,
    journalist or student would actually type. They talk about THEIR subject and what
    they want to know or get, with concrete invented details (places, dates, names,
    thresholds). They do not know how the platform works: no operator names, no
    techniques, no "pipeline", no "step", no "then" -- unless the thing they want is
    itself technical (someone who wants an SQL query says so). Mix questions, requests
    and plain statements of a problem.
  - {TASKS_PER_AP["technical"]} with style "technical": a developer describing the
    processing they want, in their own words rather than the wording of the steps.
  - {TASKS_PER_AP["terse"]} with style "terse": a keyword-style request, under 8 words.

Rules:
- The person has exactly what the FIRST step takes as input, nothing more processed: if
  the AP starts from an SQL query they already hold that query, if it starts from a
  question in plain language they only have the question.
- Every task must need the FINAL result of the LAST step. For an AP of several steps, a
  task that an earlier step would already satisfy is wrong.
- A task must not ask for something the AP does not produce.
- Tasks must differ from each other in subject and in wording.

Real requests from this platform, for tone only (they belong to other APs):
- What topics did I perform worst in last week?
- Who are the students at risk of failing based on last month's activities?
- I'm stuck on eigenvalues. Is there a short video that explains this?
- Compare the performance of the students of university A and B, per topic
- What is the average temperature in Celsius in City of Zurich in 2016?
- Show the highest wind speed in City of Zurich in July 2018.
- What was the average snow depth in City of Zurich during week 5 of 2010 at elevations below 1000 meters?
- List the top 10 lowest temperature readings (in celsius) in City of Zurich in July 2015 with their coordinates.
"""
