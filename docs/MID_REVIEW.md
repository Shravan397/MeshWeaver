# MeshWeaver Mid-Project Review

This guide covers the two requested demonstrations: a 10-process local network
audit and remote execution of a serialized ML/math callable.

## What the demo proves

1. The launcher starts ten independent Python processes.
2. Each process advertises its node ID and listening port through UDP multicast.
3. Nodes learn peer addresses from announcements rather than a list of peer IPs.
4. Every node attempts a TCP PING/PONG handshake with every discovered peer.
5. Node 01 sends a cloudpickle-serialized linear-regression computation to Node 02.
6. Node 02 executes the callable and returns a result; the sender checks it against
   a local calculation.

The local demo uses loopback TCP for peer traffic and multicast for discovery.
It is intended for a single-computer controlled review, not multi-host deployment.

## Setup (Windows PowerShell)

From the repository root:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,mid-review]"
```

If the virtual environment already exists, activate it and run only the install
command.

## Run the review demo

```powershell
python -m scripts.mid_review_demo --nodes 10 --timeout 25
```

A successful run should report:

- 10/10 node processes ready
- 90/90 directed peer discoveries
- 90/90 successful directed TCP handshakes
- NETWORK AUDIT: PASS
- SERIALIZATION CHECK: PASS

The exact timings and output depend on the computer and local network policy.
These are acceptance targets, not claimed test results.

## Run tests and lint

```powershell
python -m pytest -q
python -m ruff check .
```

## Evidence to save for the review

Save the unedited terminal output from a successful demo run and test run under
`reports/mid_review_evidence/`. Include the date, Python version, operating system,
and exact command used. Do not create a PASS report from an unsuccessful run.

## Important security limitation

Cloudpickle deserialization can execute arbitrary code. This demo intentionally
uses it only between trusted processes on localhost. Never accept serialized
callables from unknown or untrusted peers. A production design should prefer a
restricted task registry, signed artifacts, authentication, authorization,
resource limits, and process/container isolation.
