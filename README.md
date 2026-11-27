# MeshWeaver

MeshWeaver is a lightweight Python project for experimenting with peer-to-peer (P2P) node communication and distributed task selection.

## Current capabilities

- Unique node identity and basic node metadata.
- UDP listener built with Python's `asyncio` datagram API.
- JSON message protocol with versioning and input validation.
- Peer registry with status tracking, validation, and stale-peer cleanup.
- Basic peer selection based on reported CPU utilization.
- Automated tests for the protocol, peer registry, and scheduler.

> **Project status:** Early development. The current implementation is a foundation, not yet a production-ready distributed task broker. Authentication, encryption, reliable delivery, task execution, and deployment hardening remain future work.

## Requirements

- Python 3.10 or newer
- pip

## Setup

### Windows (PowerShell)

```powershell
cd "C:\D Drive\MeshWeaver"
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

### Windows (Command Prompt)

```bat
cd /d "C:\D Drive\MeshWeaver"
py -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

## Run a node

Start a listener:

```bash
python -m meshweaver --host 127.0.0.1 --port 9000
```

In a second terminal, start another node and send it a PING:

```bash
python -m meshweaver --host 127.0.0.1 --port 9001 --peer-host 127.0.0.1 --peer-port 9000 --ping
```

Stop a node with `Ctrl+C`.

## Run tests

```bash
python -m pytest
```

## Project layout

```text
MeshWeaver/
├── docs/                         # Project documentation
├── src/
│   └── meshweaver/
│       ├── __init__.py
│       ├── __main__.py           # CLI and node startup
│       ├── node.py               # Node identity and metadata
│       ├── network/
│       │   ├── __init__.py
│       │   ├── peer_registry.py  # Peer state and registry
│       │   ├── protocol.py       # Message model and validation
│       │   └── udp.py            # Async UDP transport
│       └── tasks/
│           ├── __init__.py
│           └── scheduler.py      # Basic peer selection
├── tests/
│   ├── test_network_protocol.py
│   ├── test_peer_registry.py
│   ├── test_scheduler.py
│   └── test_node.py
├── .gitignore
└── pyproject.toml
```

## Development workflow

1. Create a focused feature branch from `main`.
2. Make a small, reviewable change.
3. Run the test suite and review the diff.
4. Commit with a descriptive message.
5. Open a pull request into `main`; merge only after review.


## Mid-project review demonstrations

The repository includes a controlled local demonstration for the two review
requirements: ten independently running node processes with runtime peer
discovery, and remote execution of a serialized linear-regression function.

Install the optional demo dependency:

```powershell
python -m pip install -e ".[dev,mid-review]"
```

Run both demonstrations:

```powershell
python -m scripts.mid_review_demo --nodes 10 --timeout 25
```

Run the test suite:

```powershell
python -m pytest -q
python -m ruff check .
```

See [docs/MID_REVIEW.md](docs/MID_REVIEW.md) for setup, expected acceptance
criteria, evidence collection, and the security limitations of Cloudpickle.
The review targets are not claimed as passed until the commands are run and
their actual output is saved.

## Documentation

See [docs/architecture.md](docs/architecture.md) for the current module responsibilities and known limitations.
