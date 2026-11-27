"""Run the MeshWeaver mid-review demonstrations on one computer.

The audit starts independent OS processes. Nodes discover peers through UDP
multicast announcements; peer IPs are learned at runtime, not listed in config.
A discovered peer is contacted over TCP for a connectivity check. The same TCP
channel carries a cloudpickle-serialized ML/math callable for remote execution.

This is a controlled localhost demonstration, not a production security model.
Never deserialize cloudpickle payloads from untrusted peers.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import socket
import socketserver
import struct
import threading
import time
from typing import Any

try:
    import cloudpickle
except ImportError:  # pragma: no cover - exercised by CLI setup
    cloudpickle = None

MULTICAST_GROUP = "239.255.42.99"
DISCOVERY_PORT = 37020
BASE_TCP_PORT = 41000
DISCOVERY_INTERVAL = 0.35
MAX_FRAME_BYTES = 8 * 1024 * 1024


def linear_regression_step(
    xs: list[float],
    ys: list[float],
    weight: float,
    bias: float,
    learning_rate: float = 0.01,
) -> dict[str, float]:
    """Calculate predictions, MSE, gradients and one gradient-descent step."""
    if not xs or len(xs) != len(ys):
        raise ValueError("xs and ys must be non-empty lists of equal length")
    if not all(isinstance(v, (int, float)) for v in xs + ys):
        raise ValueError("dataset values must be numeric")
    count = len(xs)
    predictions = [weight * x + bias for x in xs]
    errors = [prediction - target for prediction, target in zip(predictions, ys)]
    mse = sum(error * error for error in errors) / count
    grad_weight = 2.0 * sum(error * x for error, x in zip(errors, xs)) / count
    grad_bias = 2.0 * sum(errors) / count
    return {
        "mse": mse,
        "gradient_weight": grad_weight,
        "gradient_bias": grad_bias,
        "updated_weight": weight - learning_rate * grad_weight,
        "updated_bias": bias - learning_rate * grad_bias,
    }


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("peer closed connection before frame completed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _recv_frame(sock: socket.socket) -> bytes:
    size = struct.unpack("!Q", _recv_exact(sock, 8))[0]
    if size > MAX_FRAME_BYTES:
        raise ValueError(f"frame too large: {size} bytes")
    return _recv_exact(sock, size)


def _send_frame(sock: socket.socket, payload: bytes) -> None:
    if len(payload) > MAX_FRAME_BYTES:
        raise ValueError("payload exceeds maximum frame size")
    sock.sendall(struct.pack("!Q", len(payload)) + payload)


class _PeerRequestHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        try:
            request = cloudpickle.loads(_recv_frame(self.request))
            if request.get("kind") == "ping":
                response = {"kind": "pong", "node_id": self.server.node_id}
            elif request.get("kind") == "execute":
                function = request["function"]
                args = request.get("args", [])
                kwargs = request.get("kwargs", {})
                result = function(*args, **kwargs)
                response = {
                    "kind": "result",
                    "node_id": self.server.node_id,
                    "result": result,
                }
            else:
                response = {"kind": "error", "error": "unsupported request"}
            _send_frame(self.request, cloudpickle.dumps(response))
        except Exception as exc:  # return an error to the caller where possible
            try:
                _send_frame(
                    self.request,
                    cloudpickle.dumps({"kind": "error", "error": str(exc)}),
                )
            except OSError:
                pass


class _NodeTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _node_process(
    node_id: str,
    tcp_port: int,
    stop_event: Any,
    event_queue: Any,
) -> None:
    server = _NodeTCPServer(("127.0.0.1", tcp_port), _PeerRequestHandler)
    server.node_id = node_id
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    discovery = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    discovery.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        discovery.bind(("", DISCOVERY_PORT))
        membership = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton("0.0.0.0")
        discovery.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        discovery.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        discovery.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        discovery.settimeout(0.15)
        event_queue.put(("ready", node_id, tcp_port))
        peers: dict[str, tuple[str, int]] = {}
        connected_peers: set[str] = set()
        last_probe: dict[str, float] = {}
        last_announce = 0.0

        while not stop_event.is_set():
            now = time.monotonic()
            if now - last_announce >= DISCOVERY_INTERVAL:
                announcement = json.dumps({
                    "kind": "announce",
                    "node_id": node_id,
                    "host": "127.0.0.1",
                    "tcp_port": tcp_port,
                }).encode("utf-8")
                discovery.sendto(announcement, (MULTICAST_GROUP, DISCOVERY_PORT))
                last_announce = now

            try:
                data, _source = discovery.recvfrom(65535)
                message = json.loads(data.decode("utf-8"))
                if message.get("kind") != "announce":
                    continue
                peer_id = message.get("node_id")
                host = message.get("host")
                peer_port = message.get("tcp_port")
                if (
                    not isinstance(peer_id, str)
                    or peer_id == node_id
                    or not isinstance(host, str)
                    or not isinstance(peer_port, int)
                ):
                    continue
                if peer_id not in peers:
                    peers[peer_id] = (host, peer_port)
                    event_queue.put(("discovered", node_id, peer_id, host, peer_port))
                # Retry handshakes if a peer announced before its TCP server was ready.
                should_probe = (
                    peer_id not in connected_peers
                    and now - last_probe.get(peer_id, 0.0) >= 1.0
                )
                if should_probe:
                    last_probe[peer_id] = now
                    try:
                        with socket.create_connection(
                            (host, peer_port), timeout=1.5
                        ) as peer:
                            _send_frame(peer, cloudpickle.dumps({"kind": "ping"}))
                            pong = cloudpickle.loads(_recv_frame(peer))
                            if pong.get("kind") == "pong":
                                connected_peers.add(peer_id)
                                event_queue.put(("connected", node_id, peer_id))
                    except OSError as exc:
                        event_queue.put(
                            ("connection_error", node_id, peer_id, str(exc))
                        )
            except socket.timeout:
                continue
            except (OSError, ValueError, TypeError):
                if not stop_event.is_set():
                    event_queue.put(
                        ("discovery_error", node_id, "invalid announcement")
                    )
    except OSError as exc:
        event_queue.put(("startup_error", node_id, str(exc)))
    finally:
        discovery.close()
        server.shutdown()
        server.server_close()


def _remote_execute(
    host: str,
    port: int,
    function: Any,
    args: list[Any],
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request = {
        "kind": "execute",
        "function": function,
        "args": args,
        "kwargs": kwargs or {},
    }
    with socket.create_connection((host, port), timeout=5.0) as peer:
        peer.settimeout(10.0)
        _send_frame(peer, cloudpickle.dumps(request))
        return cloudpickle.loads(_recv_frame(peer))


def run_audit(node_count: int = 10, timeout_seconds: float = 20.0) -> int:
    if cloudpickle is None:
        raise RuntimeError(
            'Install the demo extra with: pip install -e ".[mid-review]"'
        )
    if node_count < 2 or node_count > 32:
        raise ValueError("node_count must be between 2 and 32")
    context = mp.get_context("spawn")
    stop_event = context.Event()
    event_queue = context.Queue()
    processes: list[mp.Process] = []
    discovered: dict[str, dict[str, tuple[str, int]]] = {}
    connected: set[tuple[str, str]] = set()
    expected_pairs = node_count * (node_count - 1)

    try:
        for index in range(node_count):
            node_id = f"node-{index + 1:02d}"
            discovered[node_id] = {}
            process = context.Process(
                target=_node_process,
                args=(node_id, BASE_TCP_PORT + index, stop_event, event_queue),
                name=node_id,
            )
            process.start()
            processes.append(process)
            time.sleep(0.08)

        deadline = time.monotonic() + timeout_seconds
        ready: set[str] = set()
        while time.monotonic() < deadline:
            try:
                event = event_queue.get(timeout=0.25)
            except Exception:
                if all(not process.is_alive() for process in processes):
                    break
                continue
            kind = event[0]
            if kind == "ready":
                ready.add(event[1])
            elif kind == "discovered":
                _, node_id, peer_id, host, port = event
                discovered.setdefault(node_id, {})[peer_id] = (host, port)
            elif kind == "connected":
                _, node_id, peer_id = event
                connected.add((node_id, peer_id))
            elif kind in {"startup_error", "connection_error", "discovery_error"}:
                print(f"[{kind}] {event[1:]}")

            pair_count = sum(len(peers) for peers in discovered.values())
            all_peers_discovered = pair_count >= expected_pairs
            all_peers_connected = len(connected) >= expected_pairs
            if (
                len(ready) == node_count
                and all_peers_discovered
                and all_peers_connected
            ):
                break

        print("\n=== MESHWEAVER NETWORK AUDIT ===")
        running_count = sum(process.is_alive() for process in processes)
        print(f"Independent processes still running: {running_count}/{node_count}")
        print(f"Nodes ready: {len(ready)}/{node_count}")
        pair_count = sum(len(peers) for peers in discovered.values())
        print(f"Directed peer discoveries: {pair_count}/{expected_pairs}")
        print(f"Successful directed TCP handshakes: {len(connected)}/{expected_pairs}")
        for node_id in sorted(discovered):
            print(f"{node_id}: {len(discovered[node_id])} peers discovered")

        if (
            len(ready) != node_count
            or pair_count < expected_pairs
            or len(connected) < expected_pairs
        ):
            print(
                "NETWORK AUDIT: FAIL "
                "(not all nodes discovered and contacted every peer)"
            )
            return 1

        target_id = "node-02"
        target = discovered["node-01"].get(target_id)
        if target is None:
            print("SERIALIZATION CHECK: FAIL (target peer not discovered)")
            return 1

        args = ([1.0, 2.0, 3.0, 4.0], [3.0, 5.0, 7.0, 9.0], 0.5, 0.0, 0.01)
        remote = _remote_execute(
            target[0], target[1], linear_regression_step, list(args)
        )
        expected = linear_regression_step(*args)
        result_ok = (
            remote.get("kind") == "result"
            and remote.get("node_id") == target_id
            and all(
                abs(remote["result"][key] - value) < 1e-12
                for key, value in expected.items()
            )
        )
        print("\n=== SERIALIZATION CHECK ===")
        print(f"Sender: node-01 | Receiver: {target_id}")
        print("Callable: linear_regression_step")
        print(f"Remote result: {remote.get('result')}")
        print(f"Result matches local calculation: {'YES' if result_ok else 'NO'}")
        print(f"SERIALIZATION CHECK: {'PASS' if result_ok else 'FAIL'}")
        print(f"NETWORK AUDIT: PASS")
        return 0 if result_ok else 1
    finally:
        stop_event.set()
        for process in processes:
            process.join(timeout=2.0)
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=1.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver mid-review demo")
    parser.add_argument(
        "--nodes", type=int, default=10,
        help="number of local node processes (default: 10)",
    )
    parser.add_argument(
        "--timeout", type=float, default=20.0,
        help="discovery timeout in seconds",
    )
    args = parser.parse_args()
    raise SystemExit(run_audit(args.nodes, args.timeout))


if __name__ == "__main__":
    mp.freeze_support()
    main()
