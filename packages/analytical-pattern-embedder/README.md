# analytical-pattern-embedder

EmbeddingGemma 2 with a LoRA adapter, fine-tuned to match a user's task to the Analytical Pattern (AP) that solves it. Scripts, not a library: nothing here is installed by the service.

Its own environment (`pyproject.toml`, own lock), and not a member of the root uv workspace: training needs CUDA torch, Unsloth and `datasets`, which the service image does not (it pins CPU-only torch). Run everything from this folder; `uv run` builds the environment from `pyproject.toml`.

Unsloth 2026.10.3 declares `transformers<=5.17.0`, while EmbeddingGemma 2 first shipped in 5.19.0, so `pyproject.toml` overrides that bound.

```
generate_dataset/     LLM-written catalogue of APs and tasks -> dataset/generated   (see its README)
train.py              LoRA fine-tuning on dataset/generated/training -> models/analytical_pattern_embedder   (CUDA GPU)
merge.py              adapter merged into the base model -> models/analytical_pattern_embedder_merged, what is published
benchmark.py          analytical pattern embedder vs base model vs today's search -> .benchmarks/
dataset/inputs/       real AP files and real questions: drop an AP in real_aps/, regenerate, retrain
dataset/generated/    training/, evaluation/, and llm_cache/ (not versioned)
tests/                pytest, no GPU needed
```

```sh
uv run --env-file .env -m generate_dataset.main   # LLM settings: see .env.example
uv run train.py
uv run benchmark.py
uv run --group dev pytest
```

The model is trained on the text `AnalyticalPattern.describe()` (moma-domain) forges from an AP's operators. Embed that text, not the root description, and retrain if its wording changes.

## Retrain with new APs or tasks

The adapter (the LoRA layer) is the only thing training changes; the base model stays as published.

When to do it:

- **A real AP with new operators joins the platform.** Not strictly needed: the model still ranks APs made of operators it never saw (nDCG@10 0.73, against 0.85 for operators it trained on). Retraining closes that gap.
- **New real questions were collected.** They are only ever evaluated on, never trained on, so they tell you how good the model is; they do not change it. The exception is a question answered by a chain of operators the catalogue does not have yet: that chain becomes an AP, and the model trains on it.
- **The wording of `AnalyticalPattern.describe()` changed.** Then retraining is required.

Steps, from this folder:

1. **Add the inputs.**
   - An AP: drop its PG-JSON file in `dataset/inputs/real_aps/`. Only operators with typed `inputs` and `outputs` are read.
   - Questions: add one line each to `dataset/inputs/real_tasks.jsonl`. `gold_ops` is the chain of operator labels that answers the question, or `null` when no AP does:
     `{"text": "...", "source": "mathe", "gold_ops": ["Text_To_SQL_Operator", "SQL_Query_Operator"], "note": ""}`
2. **Regenerate the dataset.** This needs the LLM settings (`cp .env.example .env`, once):
   `uv run --env-file .env -m generate_dataset.main`
   | What you added | LLM calls |
   |---|---|
   | A question on a chain the catalogue already has | none |
   | A question on a new chain of existing operators | 1 |
   | An AP with a new operator | nearly all of them again, about 630 |
3. **Check it.** `uv run --group dev pytest`, then `git diff --stat dataset/generated` to see what changed.
4. **Train.** `uv run train.py` needs a CUDA GPU with 8 GB of memory and takes about 17 minutes on a laptop RTX. It prints the dev nDCG@10 before and after (0.55 to 0.78 on the current dataset) and leaves the best adapter in `models/analytical_pattern_embedder`. `uv run train.py --smoke` checks the setup in a minute without replacing the adapter.
5. **Benchmark.** `uv run benchmark.py` writes `.benchmarks/analytical_pattern_embedder_<date>.md`. Compare it with the previous report: the fine-tuned rows move by about 0.01 between two trainings on the same data, so only a larger change means something.
6. **Commit** `dataset/inputs` and `dataset/generated/training` and `evaluation`. The adapter is not versioned: release the model (below).

After a retrain, the APs already stored in MoMa still carry the vectors of the previous model and have to be embedded again.

More on the dataset: [generate_dataset/README.md](generate_dataset/README.md).

## Release the model

What is published on Hugging Face is the merged model: the adapter folded into the base model's weights. It loads with plain sentence-transformers and no longer depends on the base model's repository staying as it is.

Training needs a GPU, which GitHub's runners do not have, so the adapter is trained on your machine and handed to the `publish-analytical-pattern-embedder.yml` workflow through a GitHub release. Merging needs no GPU: the workflow does it.

From the root of the repository, once the adapter is trained, benchmarked and your commits are pushed:

```sh
make release-embedder VERSION=1 NOTES="Why it was retrained."
```

This packs the adapter and creates the GitHub release `analytical-pattern-embedder-v1` from the current commit, with the adapter and the latest benchmark report attached. It needs the `gh` command, logged in.

Publishing a release whose tag starts with `analytical-pattern-embedder-v` makes the workflow:

1. merge the adapter into the base model, in the service's own environment (`merge.py`, which stops if the merged model does not give what the adapter gave);
2. upload the merged model (about 550 MB) to the Hugging Face repository named by `HF_MODEL_REPO`;
3. tag it there: `v1` for `analytical-pattern-embedder-v1`.

The workflow needs two settings of the GitHub repository: `HF_MODEL_REPO` (`<organisation>/<name>`, as a variable or a secret) and the `HF_TOKEN` secret, a token with write access to that repository. If the repository does not exist it is created public.

The service then runs with `EMBEDDER_MODEL=<HF_MODEL_REPO>:v1` and no `EMBEDDER_LORA_PATH`. Name the tag: without one the service takes the latest release at every restart, whose vectors do not match those already stored.

Two things keep a release reproducible:

- **The base model is pinned.** `BASE_REVISION` in `benchmark.py` is the commit of `unsloth/embeddinggemma-2` that is trained on. `train.py` writes it in the adapter, and `merge.py` merges into that commit and no other. To move to newer base weights, change it and retrain.
- **A release is never rewritten.** A Hugging Face tag keeps pointing at the model it was created for, while `main` moves with every release.

To try the merge locally: `uv run merge.py` writes `models/analytical_pattern_embedder_merged`.
