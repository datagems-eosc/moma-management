from typing import List

from sentence_transformers import SentenceTransformer


class LocalEmbedder:
    """Embedding provider backed by a local ``sentence-transformers`` model."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self._model = SentenceTransformer(model_name)
        self._dimensions: int = self._model.get_sentence_embedding_dimension()

    @property
    def dimensions(self) -> int:
        return self._dimensions

    def embed(self, text: str) -> List[float]:
        return self._model.encode(text, convert_to_numpy=True).tolist()
