from fastapi import Depends, HTTPException

from moma_management.di import get_node_service
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.node import NodeService


async def get_node(
    id: str,
    svc: NodeService = Depends(get_node_service),
) -> Node:
    """
    Retrieve a single node by its ID.
    """
    try:
        result = svc.get(id)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}")

    if result is None:
        raise HTTPException(
            status_code=404, detail=f"Node '{id}' not found.")

    return result
