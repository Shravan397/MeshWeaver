
# MeshWeaver Architecture

## 1. Project Scope

MeshWeaver is a peer-to-peer distributed computing prototype.

The current implementation provides foundational networking, peer discovery,
heartbeat monitoring, peer status management, resource monitoring, and task
scheduling components.

The project is still under development. It should not yet be described as a
complete distributed task execution platform.

## 2. Current Runtime Architecture

The runtime starts an individual MeshWeaver node that communicates with
other nodes through UDP.

### Runtime startup flow

1. The command-line entry point in `meshweaver.__main__` parses the node's
   listening host, port, and optional bootstrap peer arguments.
2. `NodeConfig` validates the listening configuration before the UDP listener
   starts.
3. A `Node` instance generates a unique node ID and provides basic metadata.
4. `PeerRegistry` is created to maintain known peer records.
5. `PeerDiscovery` is connected to the shared peer registry.
6. `start_udp_listener` creates an asyncio UDP endpoint.
7. Incoming datagrams are decoded into protocol `Message` objects.
8. The runtime message handler processes PING, PONG, and ERROR messages.
9. Incoming PING/PONG messages are used to discover or update peer records.
10. `HeartbeatManager` starts a background thread to periodically monitor
    active peers.
11. A thread-safe transport adapter schedules heartbeat UDP sends on the
    asyncio event loop.
12. The peer status monitor reports changes between ACTIVE and INACTIVE.
13. On shutdown, the heartbeat manager and status monitor are stopped before
    the UDP listener is closed.

## 3. Networking and Peer Discovery

### UDP communication

Module: `network/udp.py`

Responsibilities:

- Start an asyncio UDP listener.
- Receive incoming datagrams.
- Decode messages using the protocol decoder.
- Pass decoded messages to the configured message handler.
- Close the UDP transport during shutdown.

### Message protocol

Module: `network/protocol.py`

The current protocol supports:

| Message type | Purpose |
|---|---|
| PING | Request a peer heartbeat response |
| PONG | Respond to a PING request |
| ERROR | Communicate an error message |

Messages contain a sender ID, message ID, timestamp, version, and payload.

Messages are serialized and decoded using JSON.

### Peer discovery

Module: `network/discovery.py`

Responsibilities:

- Discover peers from incoming PING and PONG messages.
- Register previously unknown peers.
- Update the address and last-seen information of known peers.
- Handle duplicate discovery messages.
- Preserve existing peer metadata when updating a discovered peer.

Messages originating from the local node are ignored by the runtime handler.

### Peer registry

Module: `network/peer_registry.py`

The registry provides thread-safe in-memory storage of peer records.

Each peer record contains:

- Node ID
- Host address
- UDP port
- Peer status
- CPU usage metadata
- Last-seen timestamp

Supported peer states:

- ACTIVE
- INACTIVE

The registry supports registration, lookup, updates, removal, and inactive
peer cleanup.

## 4. Heartbeat Monitoring

Module: `network/heartbeat.py`

`HeartbeatManager` monitors registered active peers using periodic PING/PONG
messages.

Responsibilities:

- Send heartbeat PING messages to active peers.
- Track pending heartbeat requests.
- Match PONG responses with pending requests.
- Calculate round-trip time (RTT).
- Update peer last-seen timestamps.
- Mark peers INACTIVE when heartbeat requests expire.
- Stop its background thread gracefully.

The runtime uses the configured heartbeat interval and timeout from
`NodeConfig`.

Default settings:

| Setting | Default |
|---|---:|
| Heartbeat interval | 5 seconds |
| Heartbeat timeout | 15 seconds |

The runtime uses `ThreadSafeUDPTransport` to safely send packets from the
heartbeat background thread through the asyncio UDP transport.

A peer status monitor checks the registry periodically and logs status
transitions.

## 5. Configuration

Module: `config.py`

`NodeConfig` validates node startup configuration, including:

- Listening host
- Listening port
- Heartbeat interval
- Heartbeat timeout
- Bootstrap peer configuration

Invalid configuration is rejected before the UDP listener starts.

## 6. Current Module Responsibilities

| Module | Responsibility |
|---|---|
| `node.py` | Node identity and basic metadata |
| `config.py` | Validated node configuration |
| `network/protocol.py` | Message types, serialization, and validation |
| `network/udp.py` | Async UDP listener and message callbacks |
| `network/discovery.py` | Peer discovery and peer updates |
| `network/peer_registry.py` | Thread-safe peer records and status |
| `network/heartbeat.py` | Heartbeat requests, RTT, and timeout detection |
| `__main__.py` | CLI startup, runtime integration, and shutdown |
| `tasks/task.py` | Task data model |
| `tasks/task_queue.py` | Task queue operations |
| `tasks/scheduler.py` | Selects an eligible peer using available CPU metadata |
| `resource_monitor.py` | Local resource monitoring |

## 7. Running MeshWeaver

Run commands from the repository root with the virtual environment activated.

### Start a node

```bash
python -m meshweaver --host 127.0.0.1 --port 9000
```

### Start a second node and send an initial PING

Open another terminal:

```bash
python -m meshweaver --host 127.0.0.1 --port 9001 --peer-host 127.0.0.1 --peer-port 9000 --ping
```

### Stop a node

Press `CTRL+C` in its terminal.

The runtime stops the heartbeat manager, stops the peer status monitor,
closes the UDP listener, and exits.

## 8. Testing

Run all tests:

```bash
python -m pytest -q
```

Run code quality checks:

```bash
ruff check src tests
```

Run the UDP integration tests:

```bash
python -m pytest -q tests/test_udp_integration.py
```

Run the peer runtime integration test:

```bash
python -m pytest -q tests/test_peer_runtime_integration.py -v
```

The peer runtime integration test verifies:

- Two live UDP endpoints.
- Peer discovery and registry updates.
- Bidirectional heartbeat communication.
- Heartbeat RTT recording.
- Peer failure detection.
- ACTIVE-to-INACTIVE status transition.
- UDP transport cleanup.

## 9. Current Limitations

The following capabilities still require further development:

- UDP does not guarantee delivery, ordering, or duplicate suppression.
- There is no authentication or message encryption.
- Automatic recovery and re-probing of INACTIVE peers are not implemented.
- Peer information is stored in memory and is not persistent.
- The protocol currently supports PING, PONG, and ERROR messages only.
- Remote task assignment and execution are not yet integrated.
- Task acknowledgement, retry, and result delivery are not yet implemented.
- Resource monitoring is not yet fully integrated into network resource updates.
- The runtime does not yet provide a complete production service lifecycle.
- Multi-node deployment and network failure scenarios require additional testing.

## 10. Engineering Priorities

1. Complete automated integration testing for networking and heartbeat behavior.
2. Improve peer recovery and lifecycle management.
3. Integrate resource monitoring with peer resource updates.
4. Define and implement task assignment and result message contracts.
5. Integrate task scheduling with discovered peer information.
6. Implement remote task execution and acknowledgement handling.
7. Add authentication and secure communication.
8. Perform broader multi-node testing and deployment validation.
