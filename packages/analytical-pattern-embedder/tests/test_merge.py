import json

import pytest


def test_merge_refuses_an_adapter_that_names_no_base_revision(tmp_path, monkeypatch):
    import merge  # imports torch: kept out of the collection of the other tests

    adapter = tmp_path / "analytical_pattern_embedder"
    adapter.mkdir()
    (adapter / "adapter_config.json").write_text(
        json.dumps({"base_model_name_or_path": "unsloth/embeddinggemma-2", "revision": None}))
    monkeypatch.setattr(merge, "MODELS", tmp_path)

    with pytest.raises(SystemExit, match="names no revision"):
        merge.main()
    assert not (tmp_path / "analytical_pattern_embedder_merged").exists()
