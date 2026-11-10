from uuid import UUID

from pydantic import BaseModel, ConfigDict, PrivateAttr


class NodeProperties(BaseModel):
    """Base of the model of every node type: the node's properties as fields, plus the id
    and labels of the node they were read from. Those two are not fields, so validating
    bare properties with these models is unchanged."""

    # PG-JSON properties are open: the JSON Schemas allow any property they do not
    # declare, so a typed read keeps them too, in model_extra
    model_config = ConfigDict(extra="allow")

    _id: UUID | None = PrivateAttr(default=None)
    _labels: list[str] = PrivateAttr(default_factory=list[str])

    @property
    def id(self) -> UUID:
        if self._id is None:
            raise AttributeError(f"{type(self).__name__} was read without its node: it has no id")
        return self._id

    @property
    def labels(self) -> list[str]:
        return self._labels
