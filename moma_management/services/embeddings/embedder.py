from typing import List, Protocol, runtime_checkable


@runtime_checkable
class Embedder(Protocol):
    """Protocol for text embedding providers."""

    @property
    def dimensions(self) -> int:
        """Dimensionality of the embedding vectors."""
        ...

    def embed(self, text: str) -> List[float]:
        """Return the embedding vector for *text*."""
        ...
