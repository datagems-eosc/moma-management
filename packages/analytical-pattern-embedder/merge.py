"""
Merge the LoRA adapter into the base model, so that the analytical pattern embedder loads with plain
sentence-transformers (no peft, no Unsloth) and no longer depends on the base model's
repository. The merged folder is what is published.

    uv run merge.py

Reads models/analytical_pattern_embedder, written by train.py, and writes models/analytical_pattern_embedder_merged.
Needs no GPU. The publish workflow runs it in the service's environment.
"""

import json
from pathlib import Path

import numpy as np
from peft import PeftModel
from sentence_transformers import SentenceTransformer

MODELS = Path(__file__).parent / "models"
# EmbeddingGemma 2 is multimodal; only its text tower is needed here.
TEXT_ONLY = {"vision_config": None, "audio_config": None}


def main() -> None:
    adapter, out = MODELS / "analytical_pattern_embedder", MODELS / "analytical_pattern_embedder_merged"
    config = json.loads((adapter / "adapter_config.json").read_text())
    base, revision = config["base_model_name_or_path"], config.get("revision")
    if not revision:
        raise SystemExit(
            f"{adapter.name}/adapter_config.json names no revision of {base}: the adapter cannot "
            "be merged into weights it may not have been trained on. Train it again with train.py.")

    model = SentenceTransformer(base, revision=revision, device="cpu", config_kwargs=TEXT_ONLY)
    # The length the adapter was trained and benchmarked with.
    model.max_seq_length = json.loads((adapter / "sentence_bert_config.json").read_text())["max_seq_length"]
    with_adapter = PeftModel.from_pretrained(model[0].auto_model, str(adapter))
    model[0].auto_model = with_adapter
    expected = model.encode_query(CHECK)
    model[0].auto_model = with_adapter.merge_and_unload()
    model.save(str(out))
    (out / "README.md").write_text(MODEL_CARD.format(base=base, revision=revision))

    # What is published must give what the adapter gave, loaded the way the service loads it.
    merged = SentenceTransformer(str(out), device="cpu", config_kwargs=TEXT_ONLY)
    got = merged.encode_query(CHECK)
    similarity = float(np.dot(expected, got) / (np.linalg.norm(expected) * np.linalg.norm(got)))
    # The base model without the adapter scores about 0.87 here.
    if similarity < 0.99:
        raise SystemExit(f"The merged model does not match the adapter (cosine {similarity:.4f}).")
    print(f"Merged model in {out} (cosine with the adapter: {similarity:.4f})")


CHECK = "Which students are at risk of failing based on last month's activities?"

MODEL_CARD = """---
license: apache-2.0
library_name: sentence-transformers
pipeline_tag: sentence-similarity
base_model: {base}
tags:
- sentence-transformers
- feature-extraction
---

# MoMa analytical pattern embedder

EmbeddingGemma 2 fine-tuned to match a user's task to the Analytical Pattern (AP) that
answers it, for the search of the DataGEMS MoMa Management service. Text only, 768
dimensions.

It is a LoRA adapter merged into `{base}` at revision `{revision}`.

```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("<this repository>", config_kwargs={{"vision_config": None, "audio_config": None}})
task = model.encode_query("Which students are at risk of failing?")
aps = model.encode_document(["Translates a natural-language string into a safe SQL query, ..."])
```

Queries and AP texts are given different prefixes, so use `encode_query` and
`encode_document` rather than `encode`. The AP text the model was trained on is the one
`AnalyticalPattern.describe()` builds from an AP's operators, not the AP's own description.

Dataset, training and benchmark:
https://github.com/datagems-eosc/moma-management/tree/main/packages/analytical-pattern-embedder
"""

if __name__ == "__main__":
    main()
