"""
bacnet_collector.py — Edge BACnet Collector for SensorGuard

Runs on-site (Raspberry Pi, VM, or container), discovers BACnet devices,
subscribes to COV or polls points, and pushes data to SensorGuard cloud API.

Architecture:
  ┌─────────────────────────────────────────────────────────────────────┐
  │  Building Network (isolated)                                         │
  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                              │
  │  │ AHU-1   │  │ AHU-2   │  │ Chiller │  ← BACnet/IP devices         │
  │  └────┬────┘  └────┬────┘  └────┬────┘                              │
  │       │            │            │                                    │
  │       └────────────┼────────────┘                                    │
  │                    │                                                 │
  │              ┌─────▼─────┐                                           │
  │              │ Collector │  ← THIS SERVICE (BAC0 + asyncio)         │
  │              └─────┬─────┘                                           │
  └────────────────────┼────────────────────────────────────────────────┘
                       │ HTTPS (outbound only)
                       ▼
              ┌────────────────┐
              │  SensorGuard   │  ← Cloud API
              │  Cloud API     │
              └────────────────┘

Usage:
  # Install dependencies
  pip install BAC0 httpx python-dotenv

  # Configure
  export SENSORGUARD_API=https://api.sensorguard.io
  export SENSORGUARD_TOKEN=your-jwt-token
  export BUILDING_ID=1
  export BACNET_IP=192.168.1.100/24  # Your interface IP/mask

  # Run
  python bacnet_collector.py

  # Or with config file
  python bacnet_collector.py --config collector.yaml
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from collections import deque
from pathlib import Path

# Optional: load from .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import httpx

# BAC0 import with fallback for development/testing
try:
    import BAC0
    HAS_BAC0 = True
except ImportError:
    HAS_BAC0 = False
    logging.warning("BAC0 not installed. Running in simulation mode.")

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class PointConfig:
    """Single BACnet point to monitor."""
    name: str                          # Human-readable name (e.g., "AHU1_SAT")
    device_id: int                     # BACnet device instance
    object_type: str                   # e.g., "analogInput", "analogValue", "binaryInput"
    object_instance: int               # Object instance number
    pair_role: str = "a"               # "a" or "b" - which side of the sensor pair
    pair_name: str = ""                # Which pair this belongs to
    cov_increment: Optional[float] = None  # COV threshold (None = use device default)

    @property
    def object_id(self) -> tuple:
        """BAC0-compatible object identifier."""
        return (self.object_type, self.object_instance)


@dataclass
class CollectorConfig:
    """Full collector configuration."""
    # SensorGuard cloud connection
    api_url: str = "http://localhost:8000"
    api_token: str = ""
    building_id: int = 1

    # BACnet network
    bacnet_ip: str = ""                # e.g., "192.168.1.100/24"
    bacnet_port: int = 47808           # Standard BACnet/IP port

    # Behavior
    poll_interval: float = 30.0        # Seconds between polls (if not using COV)
    cov_lifetime: int = 300            # COV subscription lifetime in seconds
    use_cov: bool = True               # Prefer COV over polling
    push_interval: float = 10.0        # How often to push batched data to cloud
    max_batch_size: int = 100          # Max readings per API call
    reconnect_delay: float = 30.0      # Seconds to wait before reconnecting

    # Development / test
    simulate: bool = False            # Only run fake data if explicitly enabled

    # Discovery
    auto_discover: bool = True         # Auto-discover devices on startup
    discovery_timeout: float = 10.0    # Seconds to wait for device responses

    # Points to monitor (populated from cloud config or local file)
    points: List[PointConfig] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "CollectorConfig":
        """Load configuration from environment variables."""
        return cls(
            api_url=os.getenv("SENSORGUARD_API", "http://localhost:8000"),
            api_token=os.getenv("SENSORGUARD_TOKEN", ""),
            building_id=int(os.getenv("BUILDING_ID", "1")),
            bacnet_ip=os.getenv("BACNET_IP", ""),
            bacnet_port=int(os.getenv("BACNET_PORT", "47808")),
            poll_interval=float(os.getenv("POLL_INTERVAL", "30")),
            use_cov=os.getenv("USE_COV", "true").lower() == "true",
            simulate=os.getenv("SENSORGUARD_SIMULATE", os.getenv("SIMULATE", "false"))
                .lower() in ("1", "true", "yes"),
        )

    @classmethod
    def from_yaml(cls, path: str) -> "CollectorConfig":
        """Load configuration from YAML file."""
        try:
            import yaml
            with open(path) as f:
                data = yaml.safe_load(f)
            points = [PointConfig(**p) for p in data.pop("points", [])]
            return cls(**data, points=points)
        except ImportError:
            raise RuntimeError("PyYAML required for YAML config: pip install pyyaml")


# ══════════════════════════════════════════════════════════════════════════════
# Data Models
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class PointReading:
    """A single point reading to be sent to the cloud."""
    point_name: str
    value: float
    timestamp: float
    quality: str = "good"              # "good", "uncertain", "bad"
    device_id: int = 0

    def to_dict(self) -> dict:
        return {
            "point_name": self.point_name,
            "value": self.value,
            "timestamp": self.timestamp,
            "quality": self.quality,
            "device_id": self.device_id,
        }


@dataclass
class DiscoveredDevice:
    """A discovered BACnet device."""
    device_id: int
    address: str
    name: str = ""
    vendor: str = ""
    model: str = ""
    objects: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ══════════════════════════════════════════════════════════════════════════════
# BACnet Collector Core
# ══════════════════════════════════════════════════════════════════════════════

class BACnetCollector:
    """
    Edge collector that bridges BACnet to SensorGuard cloud.
    
    Supports:
    - Device discovery (Who-Is)
    - COV subscriptions (preferred, reduces traffic)
    - Polling fallback (for devices without COV support)
    - Batched push to cloud API
    - Automatic reconnection
    """

    def __init__(self, config: CollectorConfig):
        self.config = config
        self.logger = logging.getLogger("bacnet_collector")
        
        # BACnet state
        self.bacnet = None
        self.devices: Dict[int, Any] = {}  # device_id -> BAC0 device object
        self.discovered: Dict[int, DiscoveredDevice] = {}
        
        # Data buffer
        self.reading_buffer: deque[PointReading] = deque(maxlen=10000)
        self.last_values: Dict[str, float] = {}  # point_name -> last value
        
        # COV subscriptions
        self.cov_subscriptions: Dict[str, asyncio.Task] = {}
        
        # Control
        self._running = False
        self._tasks: List[asyncio.Task] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self):
        """Start the collector."""
        self.logger.info("Starting BACnet collector...")
        self._running = True

        # Initialize BACnet
        await self._init_bacnet()

        # Fetch point config from cloud
        await self._fetch_cloud_config()

        # Discover devices if enabled
        if self.config.auto_discover:
            await self.discover_devices()

        # Connect to devices and set up monitoring
        await self._setup_monitoring()

        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._push_loop()),
            asyncio.create_task(self._poll_loop()),
            asyncio.create_task(self._cov_renewal_loop()),
            asyncio.create_task(self._heartbeat_loop()),
        ]

        self.logger.info("Collector started successfully")

    async def stop(self):
        """Stop the collector gracefully."""
        self.logger.info("Stopping collector...")
        self._running = False

        # Cancel tasks
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Disconnect BACnet
        if self.bacnet:
            try:
                await self.bacnet._disconnect()
            except Exception as e:
                self.logger.warning(f"Error disconnecting BACnet: {e}")

        # Push any remaining data
        await self._push_readings()

        self.logger.info("Collector stopped")

    async def run_forever(self):
        """Run until interrupted."""
        await self.start()
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    # ── BACnet Initialization ─────────────────────────────────────────────────

    async def _init_bacnet(self):
        """Initialize BAC0 connection.

        Important: we NEVER enter simulation mode implicitly.
        Set SENSORGUARD_SIMULATE=1 (or SIMULATE=1) to run without BACnet.
        """
        if not HAS_BAC0:
            if self.config.simulate:
                self.logger.warning("BAC0 not available; running in explicit simulation mode (SENSORGUARD_SIMULATE=1).")
                self.bacnet = None
                return
            raise RuntimeError(
                "BAC0 is not installed, and simulation is disabled. "
                "Install BAC0 (pip install BAC0) or enable simulation via SENSORGUARD_SIMULATE=1."
            )

        try:
            ip = self.config.bacnet_ip or None
            self.logger.info(f"Initializing BACnet on {ip or 'auto-detect'}...")
            
            # BAC0.start() returns an async context manager
            self.bacnet = await BAC0.start(ip=ip)
            
            self.logger.info(f"BACnet initialized: {self.bacnet}")
        except Exception as e:
            self.logger.error(f"Failed to initialize BACnet: {e}")
            if self.config.simulate:
                self.logger.warning("Falling back to explicit simulation mode (SENSORGUARD_SIMULATE=1).")
                self.bacnet = None
                return
            raise

    # ── Device Discovery ──────────────────────────────────────────────────────

    async def discover_devices(self, timeout: float = None) -> List[DiscoveredDevice]:
        """
        Discover BACnet devices on the network.
        
        Returns list of discovered devices with their basic properties.
        """
        timeout = timeout or self.config.discovery_timeout
        self.logger.info(f"Discovering BACnet devices (timeout={timeout}s)...")

        if not self.bacnet:
            # Simulation mode - return fake devices
            return self._simulate_discovery()

        try:
            # Global broadcast Who-Is
            await self.bacnet._discover(global_broadcast=True)
            await asyncio.sleep(timeout)

            # Process discovered devices
            devices = []
            for device in self.bacnet.devices:
                dev_id = device[1]  # (address, device_instance)
                address = str(device[0])
                
                discovered = DiscoveredDevice(
                    device_id=dev_id,
                    address=address,
                )
                
                # Try to read device properties
                try:
                    dev_obj = await BAC0.device(address, dev_id, self.bacnet, poll=0)
                    discovered.name = await self._read_property(dev_obj, "device", dev_id, "objectName") or ""
                    discovered.vendor = await self._read_property(dev_obj, "device", dev_id, "vendorName") or ""
                    discovered.model = await self._read_property(dev_obj, "device", dev_id, "modelName") or ""
                    
                    # Get object list
                    obj_list = await self._read_property(dev_obj, "device", dev_id, "objectList")
                    if obj_list:
                        discovered.objects = [
                            {"type": str(o[0]), "instance": o[1]}
                            for o in obj_list[:100]  # Limit for performance
                        ]
                except Exception as e:
                    self.logger.warning(f"Could not read properties from device {dev_id}: {e}")

                devices.append(discovered)
                self.discovered[dev_id] = discovered
                self.logger.info(f"Discovered: {discovered.name or f'Device {dev_id}'} at {address}")

            return devices

        except Exception as e:
            self.logger.error(f"Discovery failed: {e}")
            return []

    def _simulate_discovery(self) -> List[DiscoveredDevice]:
        """Simulate device discovery for testing."""
        devices = [
            DiscoveredDevice(
                device_id=1001, address="192.168.1.101", name="AHU-1",
                vendor="Johnson Controls", model="NAE55",
                objects=[
                    {"type": "analogInput", "instance": 1},   # SAT
                    {"type": "analogInput", "instance": 2},   # RAT
                    {"type": "analogValue", "instance": 1},   # SAT_SP
                    {"type": "analogOutput", "instance": 1},  # Cooling Valve CMD
                    {"type": "analogInput", "instance": 3},   # Cooling Valve POS
                ]
            ),
            DiscoveredDevice(
                device_id=1002, address="192.168.1.102", name="Chiller-1",
                vendor="Trane", model="CG-PRC-030",
                objects=[
                    {"type": "analogInput", "instance": 1},   # CHWS Temp
                    {"type": "analogInput", "instance": 2},   # CHWR Temp
                    {"type": "analogValue", "instance": 1},   # CHWS Setpoint
                ]
            ),
        ]
        for d in devices:
            self.discovered[d.device_id] = d
        return devices

    # ── Point Monitoring Setup ────────────────────────────────────────────────

    async def _fetch_cloud_config(self):
        """Fetch point configuration from SensorGuard cloud."""
        if not self.config.api_token:
            self.logger.warning("No API token configured, skipping cloud config fetch")
            return

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.config.api_url}/api/buildings/{self.config.building_id}/bacnet-config",
                    headers={"Authorization": f"Bearer {self.config.api_token}"},
                    timeout=30,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    self.config.points = [PointConfig(**p) for p in data.get("points", [])]
                    self.logger.info(f"Loaded {len(self.config.points)} points from cloud")
                elif resp.status_code == 404:
                    self.logger.info("No BACnet config found in cloud, using local config")
                else:
                    self.logger.warning(f"Failed to fetch cloud config: {resp.status_code}")
        except Exception as e:
            self.logger.warning(f"Could not fetch cloud config: {e}")

    async def _setup_monitoring(self):
        """Set up COV subscriptions or polling for configured points."""
        if not self.config.points:
            self.logger.warning("No points configured for monitoring")
            return

        # Group points by device
        by_device: Dict[int, List[PointConfig]] = {}
        for point in self.config.points:
            by_device.setdefault(point.device_id, []).append(point)

        # Connect to each device and set up monitoring
        for device_id, points in by_device.items():
            try:
                await self._setup_device_monitoring(device_id, points)
            except Exception as e:
                self.logger.error(f"Failed to set up monitoring for device {device_id}: {e}")

    async def _setup_device_monitoring(self, device_id: int, points: List[PointConfig]):
        """Set up monitoring for a single device."""
        if not self.bacnet:
            self.logger.info(f"Simulation mode: would monitor {len(points)} points on device {device_id}")
            return

        # Get device connection info
        dev_info = self.discovered.get(device_id)
        if not dev_info:
            self.logger.warning(f"Device {device_id} not discovered, skipping")
            return

        # Connect to device
        try:
            # poll=0 means don't auto-poll, we'll handle it
            device = await BAC0.device(dev_info.address, device_id, self.bacnet, poll=0)
            self.devices[device_id] = device
            self.logger.info(f"Connected to device {device_id} ({dev_info.name})")
        except Exception as e:
            self.logger.error(f"Could not connect to device {device_id}: {e}")
            return

        # Set up COV or polling for each point
        for point in points:
            if self.config.use_cov:
                try:
                    await self._subscribe_cov(device, point)
                except Exception as e:
                    self.logger.warning(f"COV subscription failed for {point.name}, will poll: {e}")

    async def _subscribe_cov(self, device, point: PointConfig):
        """Subscribe to COV notifications for a point."""
        try:
            # Create callback for this point
            def cov_callback(elements):
                self._handle_cov_notification(point, elements)

            # Subscribe
            obj_ref = device[point.name] if point.name in device else None
            if obj_ref:
                await obj_ref.subscribe_cov(
                    lifetime=self.config.cov_lifetime,
                    callback=cov_callback,
                )
                self.logger.info(f"COV subscription active for {point.name}")
            else:
                # Direct subscription by object ID
                self.bacnet.cov(
                    device.properties.address,
                    point.object_id,
                    confirmed=True,
                    lifetime=self.config.cov_lifetime,
                )
                self.logger.info(f"COV subscription (direct) for {point.name}")

        except Exception as e:
            raise RuntimeError(f"COV subscription failed: {e}")

    def _handle_cov_notification(self, point: PointConfig, elements: dict):
        """Handle incoming COV notification."""
        try:
            props = elements.get("properties", {})
            value = props.get("presentValue")
            
            if value is not None:
                reading = PointReading(
                    point_name=point.name,
                    value=float(value),
                    timestamp=time.time(),
                    quality=self._parse_status_flags(props.get("statusFlags")),
                    device_id=point.device_id,
                )
                self.reading_buffer.append(reading)
                self.last_values[point.name] = value
                self.logger.debug(f"COV: {point.name} = {value}")
        except Exception as e:
            self.logger.error(f"Error handling COV for {point.name}: {e}")

    def _parse_status_flags(self, flags) -> str:
        """Parse BACnet status flags to quality string."""
        if not flags:
            return "good"
        # Status flags: [in_alarm, fault, overridden, out_of_service]
        try:
            if flags[1]:  # fault
                return "bad"
            if flags[3]:  # out_of_service
                return "uncertain"
            if flags[0]:  # in_alarm
                return "uncertain"
        except (IndexError, TypeError):
            pass
        return "good"

    # ── Polling ───────────────────────────────────────────────────────────────

    async def _poll_loop(self):
        """Periodically poll points that don't have COV."""
        while self._running:
            try:
                await asyncio.sleep(self.config.poll_interval)
                await self._poll_all_points()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Poll loop error: {e}")
                await asyncio.sleep(5)

    async def _poll_all_points(self):
        """Poll all configured points."""
        for point in self.config.points:
            try:
                value = await self._read_point(point)
                if value is not None:
                    reading = PointReading(
                        point_name=point.name,
                        value=float(value),
                        timestamp=time.time(),
                        device_id=point.device_id,
                    )
                    self.reading_buffer.append(reading)
                    self.last_values[point.name] = value
            except Exception as e:
                self.logger.warning(f"Failed to poll {point.name}: {e}")

    async def _read_point(self, point: PointConfig) -> Optional[float]:
        """Read current value of a point.

        Note: we do NOT generate fake values unless simulate=True.
        If BACnet is unavailable, we return None so the cloud can mark points stale/offline.
        """
        if not self.bacnet:
            if self.config.simulate:
                # Explicit simulation - generate fake values
                import random
                base = self.last_values.get(point.name, 70.0)
                return base + random.uniform(-0.5, 0.5)
            return None

        device = self.devices.get(point.device_id)
        if not device:
            return None

        try:
            return await self._read_property(
                device, point.object_type, point.object_instance, "presentValue"
            )
        except Exception as e:
            self.logger.warning(f"Read failed for {point.name}: {e}")
            return None

    async def _read_property(self, device, obj_type: str, instance: int, prop: str) -> Any:
        """Read a property from a BACnet object."""
        try:
            return await device.read(f"{obj_type}:{instance} {prop}")
        except Exception:
            return None

    # ── COV Renewal ───────────────────────────────────────────────────────────

    async def _cov_renewal_loop(self):
        """Periodically renew COV subscriptions."""
        renewal_interval = self.config.cov_lifetime * 0.8  # Renew at 80% of lifetime
        
        while self._running:
            try:
                await asyncio.sleep(renewal_interval)
                await self._renew_cov_subscriptions()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"COV renewal error: {e}")
                await asyncio.sleep(10)

    async def _renew_cov_subscriptions(self):
        """Renew all active COV subscriptions."""
        if not self.config.use_cov:
            return

        self.logger.debug("Renewing COV subscriptions...")
        for device_id, device in self.devices.items():
            points = [p for p in self.config.points if p.device_id == device_id]
            for point in points:
                try:
                    await self._subscribe_cov(device, point)
                except Exception as e:
                    self.logger.warning(f"COV renewal failed for {point.name}: {e}")

    # ── Cloud Push ────────────────────────────────────────────────────────────

    async def _push_loop(self):
        """Periodically push buffered readings to cloud."""
        while self._running:
            try:
                await asyncio.sleep(self.config.push_interval)
                await self._push_readings()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Push loop error: {e}")
                await asyncio.sleep(5)

    async def _push_readings(self):
        """Push buffered readings to SensorGuard cloud."""
        if not self.reading_buffer:
            return

        if not self.config.api_token:
            # No cloud connection, just log
            count = len(self.reading_buffer)
            self.reading_buffer.clear()
            self.logger.debug(f"Discarded {count} readings (no API token)")
            return

        # Drain buffer
        readings = []
        while self.reading_buffer and len(readings) < self.config.max_batch_size:
            readings.append(self.reading_buffer.popleft())

        # Build payload
        payload = {
            "building_id": self.config.building_id,
            "timestamp": time.time(),
            "readings": [r.to_dict() for r in readings],
        }

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self.config.api_url}/api/buildings/{self.config.building_id}/live-data",
                    headers={
                        "Authorization": f"Bearer {self.config.api_token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=30,
                )
                if resp.status_code == 200:
                    result = resp.json()
                    self.logger.info(
                        f"Pushed {len(readings)} readings → "
                        f"{result.get('faults_detected', 0)} faults detected"
                    )
                else:
                    self.logger.warning(f"Push failed: {resp.status_code} - {resp.text}")
                    # Re-queue readings on failure
                    for r in reversed(readings):
                        self.reading_buffer.appendleft(r)

        except Exception as e:
            self.logger.error(f"Push failed: {e}")
            # Re-queue readings
            for r in reversed(readings):
                self.reading_buffer.appendleft(r)

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self):
        """Send periodic heartbeat to cloud."""
        while self._running:
            try:
                await asyncio.sleep(60)  # Every minute
                await self._send_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.debug(f"Heartbeat failed: {e}")

    async def _send_heartbeat(self):
        """Send heartbeat with collector status."""
        if not self.config.api_token:
            return

        status = {
            "building_id": self.config.building_id,
            "timestamp": time.time(),
            "devices_connected": len(self.devices),
            "points_monitored": len(self.config.points),
            "buffer_size": len(self.reading_buffer),
            "last_values": dict(list(self.last_values.items())[:20]),  # Sample
        }

        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{self.config.api_url}/api/collectors/heartbeat",
                    headers={"Authorization": f"Bearer {self.config.api_token}"},
                    json=status,
                    timeout=10,
                )
        except Exception:
            pass  # Heartbeat failures are not critical


# ══════════════════════════════════════════════════════════════════════════════
# CLI Entry Point
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="SensorGuard BACnet Collector")
    parser.add_argument("--config", "-c", help="Path to YAML config file")
    parser.add_argument("--discover", action="store_true", help="Discover devices and exit")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--simulate", action="store_true", help="Run in explicit simulation mode (no BACnet required)")
    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Load config
    if args.config:
        config = CollectorConfig.from_yaml(args.config)
    else:
        config = CollectorConfig.from_env()

    # CLI override
    if args.simulate:
        config.simulate = True

    # Create collector
    collector = BACnetCollector(config)

    # Discovery mode
    if args.discover:
        await collector._init_bacnet()
        devices = await collector.discover_devices()
        print(f"\nDiscovered {len(devices)} devices:")
        for d in devices:
            print(f"  [{d.device_id}] {d.name or 'Unknown'} @ {d.address}")
            print(f"       Vendor: {d.vendor}, Model: {d.model}")
            print(f"       Objects: {len(d.objects)}")
        return

    # Normal operation
    def signal_handler():
        collector._running = False

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.add_signal_handler(signal.SIGTERM, signal_handler)

    await collector.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
