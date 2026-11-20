# MeshWeaver Architecture

## Scope

The current repository contains the first working layers of a peer-to-peer networking prototype. It should not yet be described as a complete distributed task broker.

## Runtime flow

1. The command-line entry point in `meshweaver.__main__` parses the listen address and optional peer address.
2. A `Node` instance supplies a unique node ID and basic host metadata.
3. `start_udp_listener` creates an asyncio UDP endpoint.
4. Incoming datagrams are decoded by the protocol module.
5. The CLI message handler responds to PING messages with PONG messages.

## Modules

| Module | Responsibility |
|---|---|
| `node.py` | Node identity and metadata |
| `network/protocol.py` | Message types, serialization, and validation |
| `network/udp.py` | Async UDP receive loop and callbacks |
| `network/peer_registry.py` | In-memory, thread-safe peer records |
| `tasks/scheduler.py` | Selects an active peer with the lowest valid CPU value |
| `__main__.py` | CLI arguments, startup, and PING/PONG handling |

## Current limitations and next engineering steps

- Peer registry and scheduler are currently independent of the UDP message flow.
- The scheduler selects from supplied CPU values; it does not collect system metrics.
- UDP is connectionless and does not guarantee delivery, ordering, or duplicate suppression.
- The current CLI has no persistent configuration or service lifecycle manager.
- There is no authentication or message encryption.
- Task submission, execution, acknowledgement, retry, and result delivery are not implemented yet.
- Automated integration tests for two live UDP nodes should be added.

## Engineering priorities

1. Keep the message protocol stable and expand tests for malformed input.
2. Add focused UDP transport tests, including shutdown and handler-error behavior.
3. Define peer discovery and heartbeat messages before coupling the registry to networking.
4. Define a task lifecycle and acknowledgement/retry contract before implementing remote execution.
5. Add security controls before exposing nodes outside a trusted local network.
