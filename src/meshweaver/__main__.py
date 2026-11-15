import json

from .node import Node


def main() -> None:
    """Start the MeshWeaver node."""
    node = Node()

    print("MeshWeaver Node Started!")
    print("-" * 30)
    print(json.dumps(node.get_info(), indent=4))


if __name__ == "__main__":
    main()