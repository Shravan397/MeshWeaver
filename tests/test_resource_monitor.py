
"""Tests for MeshWeaver resource monitoring."""

import asyncio
import ctypes
from unittest.mock import mock_open, patch

import pytest

from meshweaver.monitoring import (
    ResourceMonitor,
    ResourceMonitorError,
    ResourceSnapshot,
)


def test_monitor_rejects_zero_interval() -> None:
    with pytest.raises(ValueError):
        ResourceMonitor(interval=0)


def test_monitor_rejects_negative_interval() -> None:
    with pytest.raises(ValueError):
        ResourceMonitor(interval=-1)


def test_monitor_rejects_boolean_interval() -> None:
    with pytest.raises(ValueError):
        ResourceMonitor(interval=True)


@pytest.mark.parametrize(
    "interval",
    [float("nan"), float("inf"), float("-inf")],
)
def test_monitor_rejects_non_finite_interval(interval: float) -> None:
    with pytest.raises(ValueError, match="positive finite number"):
        ResourceMonitor(interval=interval)


def test_cpu_percentage_calculation() -> None:
    result = ResourceMonitor._calculate_cpu_percent(
        100, 200, 125, 300
    )

    assert result == 75.0


def test_cpu_percentage_can_be_zero() -> None:
    result = ResourceMonitor._calculate_cpu_percent(
        100, 200, 200, 300
    )

    assert result == 0.0


def test_cpu_percentage_can_be_one_hundred() -> None:
    result = ResourceMonitor._calculate_cpu_percent(
        100, 200, 100, 300
    )

    assert result == 100.0


def test_cpu_calculation_rejects_unchanged_counters() -> None:
    with pytest.raises(ResourceMonitorError):
        ResourceMonitor._calculate_cpu_percent(
            100, 200, 100, 200
        )


def test_cpu_calculation_rejects_invalid_idle_delta() -> None:
    with pytest.raises(ResourceMonitorError):
        ResourceMonitor._calculate_cpu_percent(
            100, 200, 250, 300
        )


def test_snapshot_contains_expected_fields() -> None:
    snapshot = ResourceSnapshot(
        cpu_percent=20.0,
        ram_percent=40.0,
        ram_total_mb=8192.0,
        ram_available_mb=4915.2,
        timestamp="2026-11-21T12:00:00+00:00",
    )

    assert snapshot.cpu_percent == 20.0
    assert snapshot.ram_percent == 40.0
    assert snapshot.ram_total_mb == 8192.0
    assert snapshot.ram_available_mb == 4915.2
    assert snapshot.timestamp.endswith("+00:00")


def test_collect_sync_returns_snapshot() -> None:
    monitor = ResourceMonitor(interval=0.01)

    with (
        patch.object(
            monitor,
            "_read_cpu_times",
            side_effect=[(100, 200), (125, 300)],
        ),
        patch.object(
            monitor,
            "_read_memory",
            return_value=(8 * 1024**3, 4 * 1024**3),
        ),
        patch("meshweaver.monitoring.resource_monitor.time.sleep"),
    ):
        snapshot = monitor._collect_sync()

    assert isinstance(snapshot, ResourceSnapshot)
    assert snapshot.cpu_percent == 75.0
    assert snapshot.ram_percent == 50.0
    assert snapshot.ram_total_mb == 8192.0
    assert snapshot.ram_available_mb == 4096.0
    assert snapshot.timestamp


def test_collect_rejects_invalid_total_memory() -> None:
    monitor = ResourceMonitor()

    with (
        patch.object(
            monitor,
            "_read_cpu_times",
            side_effect=[(100, 200), (125, 300)],
        ),
        patch.object(
            monitor,
            "_read_memory",
            return_value=(0, 0),
        ),
        patch("meshweaver.monitoring.resource_monitor.time.sleep"),
    ):
        with pytest.raises(ResourceMonitorError):
            monitor._collect_sync()


def test_collect_rejects_available_memory_above_total() -> None:
    monitor = ResourceMonitor()

    with (
        patch.object(
            monitor,
            "_read_cpu_times",
            side_effect=[(100, 200), (125, 300)],
        ),
        patch.object(
            monitor,
            "_read_memory",
            return_value=(1000, 1200),
        ),
        patch("meshweaver.monitoring.resource_monitor.time.sleep"),
    ):
        with pytest.raises(ResourceMonitorError):
            monitor._collect_sync()


def test_collect_is_async() -> None:
    monitor = ResourceMonitor()

    expected = ResourceSnapshot(
        cpu_percent=10.0,
        ram_percent=20.0,
        ram_total_mb=4096.0,
        ram_available_mb=3276.8,
        timestamp="2026-11-21T12:00:00+00:00",
    )

    with patch.object(
        monitor,
        "_collect_sync",
        return_value=expected,
    ):
        result = asyncio.run(monitor.collect())

    assert result == expected


def test_linux_cpu_parser_excludes_guest_counters() -> None:
    cpu_data = "cpu 100 20 30 40 10 5 3 2 50 6\n"

    with patch(
        "builtins.open",
        mock_open(read_data=cpu_data),
    ):
        idle, total = ResourceMonitor._read_linux_cpu_times()

    assert idle == 50
    assert total == 210


def test_linux_memory_parser_returns_bytes() -> None:
    memory_data = (
        "MemTotal:       16384 kB\n"
        "MemFree:         4096 kB\n"
        "MemAvailable:    8192 kB\n"
    )

    with patch(
        "builtins.open",
        mock_open(read_data=memory_data),
    ):
        total, available = ResourceMonitor._read_linux_memory()

    assert total == 16384 * 1024
    assert available == 8192 * 1024


def test_windows_cpu_parser_returns_idle_and_total_times() -> None:
    class FakeKernel32:
        @staticmethod
        def GetSystemTimes(idle, kernel, user):
            idle._obj.dwLowDateTime = 100
            idle._obj.dwHighDateTime = 0

            kernel._obj.dwLowDateTime = 300
            kernel._obj.dwHighDateTime = 0

            user._obj.dwLowDateTime = 200
            user._obj.dwHighDateTime = 0

            return 1

    with patch.object(
        ctypes,
        "WinDLL",
        return_value=FakeKernel32(),
        create=True,
    ):
        idle, total = ResourceMonitor._read_windows_cpu_times()

    assert idle == 100
    assert total == 500


def test_windows_memory_parser_returns_physical_memory() -> None:
    class FakeKernel32:
        @staticmethod
        def GlobalMemoryStatusEx(status_pointer):
            status = status_pointer._obj
            status.ullTotalPhys = 16 * 1024**3
            status.ullAvailPhys = 8 * 1024**3
            return 1

    with patch.object(
        ctypes,
        "WinDLL",
        return_value=FakeKernel32(),
        create=True,
    ):
        total, available = ResourceMonitor._read_windows_memory()

    assert total == 16 * 1024**3
    assert available == 8 * 1024**3
