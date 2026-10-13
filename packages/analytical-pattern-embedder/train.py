"""
Train the analytical pattern embedder: EmbeddingGemma 2 with a LoRA adapter, fine-tuned to match
tasks to Analytical Patterns.

    uv run train.py            # needs a CUDA GPU
    uv run train.py --smoke    # 30 steps on 500 tasks

Follows Unsloth's EmbeddingGemma 2 notebook, with the batch size and sequence length
brought down to fit 8 GB of VRAM:
https://colab.research.google.com/github/unslothai/notebooks/blob/main/nb/EmbeddingGemma2_(300M).ipynb

Reads dataset/generated/training, written by generate_dataset/main.py. Each training task pulls the
description of its AP closer and pushes away three look-alike APs, plus the APs of the
other tasks in the batch. The model is scored on the dev tasks every EVAL_STEPS steps,
and the adapter that scores best is the one left in models/analytical_pattern_embedder.

Run it from this folder: `uv run` reads the dependencies from pyproject.toml.
"""

# Unsloth patches transformers on import and must come first.
from unsloth import FastSentenceTransformer, is_bfloat16_supported  # isort: skip

import argparse
import json
import random
from pathlib import Path
from collections.abc import Iterator

from datasets import Dataset
from sentence_transformers import SentenceTransformerTrainer, SentenceTransformerTrainingArguments
from sentence_transformers.base.evaluation.evaluator import BaseEvaluator
from sentence_transformers.base.sampler import DefaultBatchSampler
from sentence_transformers.sentence_transformer import losses
from transformers import TrainerCallback

# The model is selected on the very metric it is then reported on.
from benchmark import ADAPTER, BASE_MODEL, BASE_REVISION, MAX_SEQ_LENGTH, embed, read_jsonl, score

DATA = Path(__file__).parent / "dataset" / "generated" / "training"
EPOCHS = 4
LEARNING_RATE = 1e-4
BATCH_SIZE = 32
EVAL_STEPS = 100
SEED = 3407


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--smoke", action="store_true", help="30 steps on 500 tasks, to check the setup")
    args = parser.parse_args()

    aps = {ap["id"]: ap for ap in read_jsonl(DATA / "aps.jsonl")}
    train_tasks = read_jsonl(DATA / "train_tasks.jsonl")
    dev_tasks = read_jsonl(DATA / "dev_tasks.jsonl")
    # A smoke run must not replace the real adapter.
    out = ADAPTER.with_name(ADAPTER.name + "_smoke") if args.smoke else ADAPTER
    if args.smoke:
        train_tasks = random.Random(SEED).sample(train_tasks, min(500, len(train_tasks)))
    print(f"{len(train_tasks)} training tasks, {len(dev_tasks)} dev tasks, {len(aps)} APs", flush=True)

    model = FastSentenceTransformer.from_pretrained(
        model_name=BASE_MODEL,
        revision=BASE_REVISION,
        max_seq_length=MAX_SEQ_LENGTH,
        full_finetuning=False,
        # EmbeddingGemma 2 is multimodal; only its text tower is needed here.
        config_kwargs={"vision_config": None, "audio_config": None},
    )
    model = FastSentenceTransformer.get_peft_model(
        model,
        r=32,
        lora_alpha=64,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0,
        bias="none",
        use_gradient_checkpointing="unsloth",
        random_state=SEED,
        use_rslora=False,
        loftq_config=None,
        task_type="FEATURE_EXTRACTION",
    )

    # One row per task: the task, the description of its AP, those of three look-alikes.
    dataset = Dataset.from_list([{
        "anchor": task["task"],
        "positive": aps[task["ap"]]["description"],
        **{f"negative_{i}": aps[id_]["description"] for i, id_ in enumerate(task["negatives"], start=1)},
    } for task in train_tasks])

    evaluator = DevEvaluator(dev_tasks, list(aps.values()))
    before = evaluator(model)["ndcg@10"]
    print(f"Before training: dev nDCG@10 {before:.4f}", flush=True)
    keep_best = KeepBestAdapter(model, out, before)

    bf16 = is_bfloat16_supported()
    trainer = SentenceTransformerTrainer(
        model=model,
        train_dataset=dataset,
        loss=losses.MultipleNegativesRankingLoss(model),
        evaluator=evaluator,
        callbacks=[keep_best],
        args=SentenceTransformerTrainingArguments(
            output_dir=str(out / "trainer"),
            num_train_epochs=EPOCHS,
            max_steps=30 if args.smoke else -1,
            per_device_train_batch_size=BATCH_SIZE,
            learning_rate=LEARNING_RATE,
            warmup_steps=0.05,  # a float is a share of the total steps
            lr_scheduler_type="linear",
            fp16=not bf16,
            bf16=bf16,
            logging_steps=10,
            eval_strategy="steps",
            eval_steps=15 if args.smoke else EVAL_STEPS,
            save_strategy="no",
            report_to="none",
            seed=SEED,
            prompts={column: model.prompts["query" if column == "anchor" else "document"]
                     for column in dataset.column_names},
            batch_sampler=lambda dataset, **kwargs: UnrelatedBatchSampler(dataset, tasks=train_tasks, **kwargs),
        ),
    )
    trainer.train()
    trainer.evaluate()  # the last step counts too

    if keep_best.step is None:
        raise SystemExit("Fine-tuning never beat the base model on the dev tasks: no adapter written.")
    print(f"Dev nDCG@10: {before:.4f} -> {keep_best.best:.4f} at step {keep_best.step}. Adapter in {out}")


class UnrelatedBatchSampler(DefaultBatchSampler):
    """
    Builds batches in which no AP is related to another row's task.

    The loss treats every AP of a batch but a row's own as a negative for that row.
    Random batches would therefore regularly push a task away from one of its building
    blocks, or from an AP that does what it asks and more. Here a row only joins a batch
    if the APs it brings are unrelated to the tasks already in it, and the other way round.
    """

    def __init__(self, dataset: Dataset, *, tasks: list[dict], **kwargs) -> None:
        super().__init__(dataset, **kwargs)
        self.brought = [{task["ap"], *task["negatives"]} for task in tasks]
        self.related = [{task["ap"], *task["building_blocks"], *task["supersets"]} for task in tasks]
        self.batches: dict[int, list[list[int]]] = {}

    def epoch_batches(self) -> list[list[int]]:
        epoch = getattr(self, "epoch", 0)
        if epoch not in self.batches:
            rows = list(range(len(self.related)))
            random.Random(self.seed + epoch).shuffle(rows)
            batches = []
            while rows:
                batch, brought, related, left_over = [], set(), set(), []
                for row in rows:
                    fits = not (self.related[row] & brought) and not (self.brought[row] & related)
                    if fits and len(batch) < self.batch_size:
                        batch.append(row)
                        brought |= self.brought[row]
                        related |= self.related[row]
                    else:
                        left_over.append(row)
                rows = left_over
                if len(batch) == self.batch_size or not self.drop_last:
                    batches.append(batch)
            # The rows that fit nowhere gather at the end as small batches; spread them out.
            random.Random(self.seed + epoch).shuffle(batches)
            self.batches = {epoch: batches}
        return self.batches[epoch]

    def __iter__(self) -> Iterator[list[int]]:
        yield from self.epoch_batches()

    def __len__(self) -> int:
        return len(self.epoch_batches())


class DevEvaluator(BaseEvaluator):
    """Scores the model on the dev tasks exactly as the benchmark scores the test tasks."""

    def __init__(self, tasks: list[dict], aps: list[dict]) -> None:
        super().__init__()
        self.tasks, self.aps = tasks, aps
        self.primary_metric = "ndcg@10"

    def __call__(self, model, output_path=None, epoch: int = -1, steps: int = -1) -> dict[str, float]:
        similarity = (embed(model, [task["task"] for task in self.tasks], "query")
                      @ embed(model, [ap["description"] for ap in self.aps], "document").T)
        return score(similarity, self.tasks, self.aps)


class KeepBestAdapter(TrainerCallback):
    """Writes the adapter each time the dev score improves, so the one left at the end
    is the best seen. The trainer's own checkpoints would also store the optimizer."""

    def __init__(self, model, out: Path, best: float) -> None:
        self.model, self.out, self.best, self.step = model, out, best, None

    def on_evaluate(self, args, state, control, metrics=None, **kwargs) -> None:
        if metrics["eval_ndcg@10"] > self.best:
            self.best, self.step = metrics["eval_ndcg@10"], state.global_step
            self.model.save_pretrained(str(self.out))
            self.model.tokenizer.save_pretrained(str(self.out))
            # The adapter carries the revision of the base it was trained on: merge.py reads it.
            config = self.out / "adapter_config.json"
            config.write_text(json.dumps(json.loads(config.read_text()) | {"revision": BASE_REVISION}, indent=2))
            print(f"  step {state.global_step}: dev nDCG@10 {self.best:.4f}, adapter saved", flush=True)


if __name__ == "__main__":
    main()
