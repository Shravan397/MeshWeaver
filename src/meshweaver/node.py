import socket
import uuid
from dataclasses import asdict, dataclass, field


@dataclass
class Node:
    """Represents a single MeshWeaver network node."""

    node_id: str = field(
        default_factory=lambda: f"node-{uuid.uuid4().hex[:8]}"
    )
    hostname: str = field(default_factory=socket.gethostname)
    status: str = "active"

    def get_info(self) -> dict:
        """Return information about this node."""
        return asdict(self)