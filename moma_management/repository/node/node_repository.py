from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.repository import Repository


class NodeRepository(Repository[Node, dict]):
    pass
