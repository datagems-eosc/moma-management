
from pathlib import Path

from sentence_transformers import SentenceTransformer

from .embedder import Embedder


class LocalEmbedder(Embedder):
    """Embedding provider backed by a local Sentence Transformer model."""

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        lora_path_or_hf_id: str | None = None,
        revision: str | None = None,
    ) -> None:
        """        
        Args:
            model_name: The name of the Sentence Transformer model to use.
            revision: The tag, branch or commit of the model on Hugging Face; the latest if None.
            lora_path_or_hf_id: The path to the LoRA adapter or the Hugging Face model ID
        """
        self._model = SentenceTransformer(
            model_name,
            revision=revision,
            device="cpu",
            config_kwargs={
                "vision_config": None,
                "audio_config": None
            },
        )

        if lora_path_or_hf_id is not None:
            self._model.load_adapter(lora_path_or_hf_id)

        self._dimensions = self._model.get_embedding_dimension()

    @property
    def dimensions(self) -> int:
        return self._dimensions

    # Some models (EmbeddingGemma) are trained with a different prefix on queries and on
    # documents, which `encode_query` and `encode_document` add. For a model without
    # such prefixes (MiniLM) both are plain `encode`.
    def embed(self, text: str) -> list[float]:
        return self._model.encode_document(
            text,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).tolist()

    def embed_query(self, text: str) -> list[float]:
        return self._model.encode_query(
            text,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).tolist()
