
"""Standard-library CPU and RAM monitoring for MeshWeaver."""

from __future__ import annotations

import asyncio
import ctypes
import platform
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final


class ResourceMonitorError(RuntimeError):
    """Raised when system resource information cannot be collected."""


@dataclass(frozen=True)
class ResourceSnapshot:
    """A snapshot of the current node's resource usage."""

    cpu_percent: float
    ram_percent: float
    ram_total_mb: float
    ram_available_mb: float
    timestamp: str


class ResourceMonitor:
    """Collect CPU and RAM information using the Python standard library."""

    _BYTES_PER_MB: Final[float] = 1024 * 1024

    def __init__(self, interval: float = 0.2) -> None:
        if (
            isinstance(interval, bool)
            or not isinstance(interval, (int, float))
            or interval <= 0
        ):
            raise ValueError("interval must be a positive number")

        self.interval = float(interval)

    async def collect(self) -> ResourceSnapshot:
        """Collect a snapshot without blocking the asyncio event loop."""
        return await asyncio.to_thread(self._collect_sync)

    def _collect_sync(self) -> ResourceSnapshot:
        """Perform the blocking system measurements in a worker thread."""
        first_idle, first_total = self._read_cpu_times()
        time.sleep(self.interval)
        second_idle, second_total = self._read_cpu_times()

        cpu_percent = self._calculate_cpu_percent(
            first_idle,
            first_total,
            second_idle,
            second_total,
        )

        total_bytes, available_bytes = self._read_memory()

        if total_bytes <= 0:
            raise ResourceMonitorError("Total system RAM must be positive")

        if not 0 <= available_bytes <= total_bytes:
            raise ResourceMonitorError("Invalid available RAM value")

        ram_percent = (
            (total_bytes - available_bytes) / total_bytes
        ) * 100

        return ResourceSnapshot(
            cpu_percent=round(cpu_percent, 2),
            ram_percent=round(ram_percent, 2),
            ram_total_mb=round(total_bytes / self._BYTES_PER_MB, 2),
            ram_available_mb=round(
                available_bytes / self._BYTES_PER_MB, 2
            ),
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    @staticmethod
    def _calculate_cpu_percent(
        first_idle: int,
        first_total: int,
        second_idle: int,
        second_total: int,
    ) -> float:
        """Calculate CPU usage between two cumulative CPU readings."""
        total_delta = second_total - first_total
        idle_delta = second_idle - first_idle

        if total_delta <= 0:
            raise ResourceMonitorError(
                "CPU counters did not advance during the sampling interval"
            )

        if idle_delta < 0 or idle_delta > total_delta:
            raise ResourceMonitorError("Invalid CPU counter values")

        usage = ((total_delta - idle_delta) / total_delta) * 100
        return max(0.0, min(100.0, usage))

    @staticmethod
    def _read_cpu_times() -> tuple[int, int]:
        """Return cumulative idle and total CPU times."""
        system = platform.system()

        if system == "Linux":
            return ResourceMonitor._read_linux_cpu_times()

        if system == "Windows":
            return ResourceMonitor._read_windows_cpu_times()

        raise ResourceMonitorError(
            f"CPU monitoring is not supported on {system}"
        )

    @staticmethod
    def _read_linux_cpu_times() -> tuple[int, int]:
        """Read CPU counters from Linux /proc/stat."""
        try:
            with open("/proc/stat", encoding="utf-8") as cpu_file:
                first_line = cpu_file.readline().split()

            if not first_line or first_line[0] != "cpu":
                raise ResourceMonitorError(
                    "Could not read Linux CPU counters"
                )

            values = [int(value) for value in first_line[1:]]

            if len(values) < 4:
                raise ResourceMonitorError(
                    "Linux CPU counters are incomplete"
                )

            idle = values[3]
            if len(values) > 4:
                idle += values[4]

            total = sum(values)
            return idle, total

        except (OSError, ValueError) as exc:
            raise ResourceMonitorError(
                "Failed to read Linux CPU counters"
            ) from exc

    @staticmethod
    def _read_windows_cpu_times() -> tuple[int, int]:
        """Read cumulative CPU counters using the Windows API."""

        class FileTime(ctypes.Structure):
            _fields_ = [
                ("dwLowDateTime", ctypes.c_ulong),
                ("dwHighDateTime", ctypes.c_ulong),
            ]

        idle = FileTime()
        kernel = FileTime()
        user = FileTime()

        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            success = kernel32.GetSystemTimes(
                ctypes.byref(idle),
                ctypes.byref(kernel),
                ctypes.byref(user),
            )

            if not success:
                raise ResourceMonitorError(
                    "Windows GetSystemTimes call failed"
                )

        except (AttributeError, OSError) as exc:
            raise ResourceMonitorError(
                "Could not access Windows CPU counters"
            ) from exc

        def to_integer(value: FileTime) -> int:
            return (
                (value.dwHighDateTime << 32)
                | value.dwLowDateTime
            )

        idle_value = to_integer(idle)
        total_value = to_integer(kernel) + to_integer(user)

        return idle_value, total_value

    @staticmethod
    def _read_memory() -> tuple[int, int]:
        """Return total and available physical RAM in bytes."""
        system = platform.system()

        if system == "Linux":
            return ResourceMonitor._read_linux_memory()

        if system == "Windows":
            return ResourceMonitor._read_windows_memory()

        raise ResourceMonitorError(
            f"RAM monitoring is not supported on {system}"
        )

    @staticmethod
    def _read_linux_memory() -> tuple[int, int]:
        """Read physical RAM information from Linux /proc/meminfo."""
        values: dict[str, int] = {}

        try:
            with open("/proc/meminfo", encoding="utf-8") as memory_file:
                for line in memory_file:
                    key, value = line.split(":", maxsplit=1)
                    parts = value.split()

                    if parts:
                        values[key] = int(parts[0]) * 1024

            total = values["MemTotal"]
            available = values["MemAvailable"]

            return total, available

        except (OSError, KeyError, ValueError) as exc:
            raise ResourceMonitorError(
                "Failed to read Linux RAM information"
            ) from exc

    @staticmethod
    def _read_windows_memory() -> tuple[int, int]:
        """Read physical RAM information using the Windows API."""

        class MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatusEx()
        status.dwLength = ctypes.sizeof(MemoryStatusEx)

        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            success = kernel32.GlobalMemoryStatusEx(ctypes.byref(status))

            if not success:
                raise ResourceMonitorError(
                    "Windows GlobalMemoryStatusEx call failed"
                )

        except (AttributeError, OSError) as exc:
            raise ResourceMonitorError(
                "Could not access Windows RAM information"
            ) from exc

        return int(status.ullTotalPhys), int(status.ullAvailPhys)
