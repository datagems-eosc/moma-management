# generate_dataset

Builds the dataset the analytical pattern embedder is trained and benchmarked on: a catalogue of Analytical Patterns (APs) and, for each AP, the tasks people would type to find it.

Only a few real APs exist, so an LLM invents a catalogue around them. **This uses a lot of LLM tokens:** about 660 calls for a full run.

## Run

From `packages/analytical-pattern-embedder`:

```sh
cp .env.example .env          # once, then fill in the LLM settings
uv run --env-file .env -m generate_dataset.main
```

Every answer is kept in `dataset/generated/llm_cache`. A run that stops resumes where it stopped, and running again with nothing changed calls nothing.

Quick trial: `uv run --env-file .env -m generate_dataset.main --domains 2 --aps_count 30 --out /tmp/small`

## What goes in, what comes out

```
dataset/
  inputs/                 what you provide
    real_aps/*.json       real AP files; their operators seed the catalogue
    real_tasks.jsonl      real questions, labelled by hand; evaluated on, never trained on
  generated/              what the script writes; never edit by hand
    training/             read by train.py
    evaluation/           read by benchmark.py
    llm_cache/            the LLM's answers; not versioned
```

## The five steps

| File | What it does |
|---|---|
| `step1_operators.py` | Reads the operators of the real APs, then has the LLM write 8 more for each of 22 domains. |
| `step2_aps.py` | Builds one AP per operator, plus chains of 2 to 4 operators that fit together. |
| `step3_tasks.py` | Has the LLM write 8 tasks per AP, returned by AP id. For the training APs that start from a question in plain language, it writes 16 more questions on varied subjects, which are trained on only. |
| `step4_splits.py` | Decides which operators the model trains on and which it is only tested on, and returns the training and the evaluation dataset. |
| `step5_files.py` | Writes the files. |

`schemas/` holds the types the steps share: the operator and its ports, the AP, the task and the dataset (APs and their tasks). `main.py` runs the steps in order. `config.py` holds the input paths and the domains. `util/llm.py` is the LLM client and its cache, `util/string.py` the text helpers.

## Add a real AP and retrain

The steps are in [the package README](../README.md#retrain-with-new-aps-or-tasks). What to know about the dataset side:

- Only operators with typed `inputs` and `outputs` are taken from the file. Each one gets its own AP in the catalogue.
- An AP that chains several operators is only sure to be in the catalogue as a chain if a line of `real_tasks.jsonl` names that chain.
- Files are read in name order. An operator defined in two files keeps its first definition.
- A new operator changes what the LLM is asked from step 1 on, so nearly everything is asked again.

## Settings

They are constants at the top of the files: `SEED` and `COMPOSED_APS` in `main.py`, `DOMAINS` in `config.py`, `OPERATORS_PER_DOMAIN` in step 1, `MAX_CHAIN_LENGTH` in step 2, `HARD_NEGATIVES` in `schemas/ap.py`, `TASKS_PER_AP` and `QUESTIONS_PER_AP` in step 3, `HELD_OUT_OPERATOR_SHARE` in step 4.
