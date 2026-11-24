"""Edge-case tests for MeshWeaver resource monitoring."""

import asyncio
import ctypes
from unittest.mock import mock_open, patch

import pytest

from meshweaver.monitoring import ResourceMonitor, ResourceMonitorError


def test_cpu_monitor_rejects_unsupported_operating_system() -> None:
    with patch(
        "meshweaver.monitoring.resource_monitor.platform.system",
        return_value="UnknownOS",
    ):
        with pytest.raises(
            ResourceMonitorError,
            match="CPU monitoring is not supported",
        ):
            ResourceMonitor._read_cpu_times()


def test_ram_monitor_rejects_unsupported_operating_system() -> None:
    with patch(
        "meshweaver.monitoring.resource_monitor.platform.system",
        return_value="UnknownOS",
    ):
        with pytest.raises(
            ResourceMonitorError,
            match="RAM monitoring is not supported",
        ):
            ResourceMonitor._read_memory()


def test_linux_cpu_parser_rejects_incomplete_data() -> None:
    cpu_data = "cpu 100 20\n"
    with (
        patch(
            "meshweaver.monitoring.resource_monitor.platform.system",
            return_value="Linux",
        ),
        patch("builtins.open", mock_open(read_data=cpu_data)),
    ):
        with pytest.raises(ResourceMonitorError, match="incomplete"):
            ResourceMonitor._read_linux_cpu_times()


def test_linux_cpu_parser_rejects_non_numeric_data() -> None:
    cpu_data = "cpu 100 20 invalid 40\n"
    with (
        patch(
            "meshweaver.monitoring.resource_monitor.platform.system",
            return_value="Linux",
        ),
        patch("builtins.open", mock_open(read_data=cpu_data)),
    ):
        with pytest.raises(
            ResourceMonitorError,
            match="Failed to read Linux CPU counters",
        ):
            ResourceMonitor._read_linux_cpu_times()


def test_linux_memory_parser_rejects_missing_available_memory() -> None:
    memory_data = "MemTotal: 16384 kB\nMemFree: 4096 kB\n"
    with patch("builtins.open", mock_open(read_data=memory_data)):
        with pytest.raises(
            ResourceMonitorError,
            match="Failed to read Linux RAM information",
        ):
            ResourceMonitor._read_linux_memory()


def test_linux_memory_parser_rejects_invalid_numeric_data() -> None:
    memory_data = "MemTotal: invalid kB\nMemAvailable: 8192 kB\n"
    with patch("builtins.open", mock_open(read_data=memory_data)):
        with pytest.raises(
            ResourceMonitorError,
            match="Failed to read Linux RAM information",
        ):
            ResourceMonitor._read_linux_memory()


def test_cpu_calculation_rejects_negative_idle_delta() -> None:
    with pytest.raises(
        ResourceMonitorError,
        match="Invalid CPU counter values",
    ):
        ResourceMonitor._calculate_cpu_percent(200, 300, 100, 400)


def test_cpu_calculation_rejects_decreasing_total_counter() -> None:
    with pytest.raises(
        ResourceMonitorError,
        match="CPU counters did not advance",
    ):
        ResourceMonitor._calculate_cpu_percent(100, 300, 110, 250)


def test_collect_rejects_negative_available_memory() -> None:
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
            return_value=(1000, -1),
        ),
        patch("meshweaver.monitoring.resource_monitor.time.sleep"),
    ):
        with pytest.raises(
            ResourceMonitorError,
            match="Invalid available RAM value",
        ):
            monitor._collect_sync()


def test_windows_cpu_api_failure_is_reported() -> None:
    class FakeKernel32:
        @staticmethod
        def GetSystemTimes(idle, kernel, user):
            return 0

    with patch.object(
        ctypes,
        "WinDLL",
        return_value=FakeKernel32(),
        create=True,
    ):
        with pytest.raises(
            ResourceMonitorError,
            match="Windows GetSystemTimes call failed",
        ):
            ResourceMonitor._read_windows_cpu_times()


def test_windows_ram_api_failure_is_reported() -> None:
    class FakeKernel32:
        @staticmethod
        def GlobalMemoryStatusEx(status_pointer):
            return 0

    with patch.object(
        ctypes,
        "WinDLL",
        return_value=FakeKernel32(),
        create=True,
    ):
        with pytest.raises(
            ResourceMonitorError,
            match="Windows GlobalMemoryStatusEx call failed",
        ):
            ResourceMonitor._read_windows_memory()


def test_async_collect_propagates_monitor_error() -> None:
    monitor = ResourceMonitor()
    with patch.object(
        monitor,
        "_collect_sync",
        side_effect=ResourceMonitorError("Test collection failure"),
    ):
        with pytest.raises(
            ResourceMonitorError,
            match="Test collection failure",
        ):
            asyncio.run(monitor.collect())
