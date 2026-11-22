from meshweaver.node import Node


def test_node_generates_unique_ids():
    first = Node()
    second = Node()

    assert first.node_id.startswith("node-")
    assert first.node_id != second.node_id


def test_node_info_contains_expected_metadata():
    node = Node()
    info = node.get_info()

    assert info["node_id"] == node.node_id
    assert info["hostname"] == node.hostname
    assert info["status"] == "active"
