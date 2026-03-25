"""
Tor circuit management for free IP rotation.

Manages the Tor process and rotates circuits to get fresh IPs.
Supports running multiple Tor instances on different ports for
parallel scraping with independent identities.
"""

import os
import time
import socket
import logging
import subprocess
import tempfile
from typing import Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


def is_tor_installed() -> bool:
    """Check if Tor is installed on the system."""
    try:
        result = subprocess.run(["which", "tor"], capture_output=True, text=True)
        return result.returncode == 0
    except Exception:
        return False


def is_port_available(port: int) -> bool:
    """Check if a port is available."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex(("127.0.0.1", port))
            return result != 0
    except Exception:
        return True


class TorInstance:
    """
    Manages a single Tor instance with its own SOCKS port and control port.
    Each instance gets an independent circuit = independent IP.
    """

    def __init__(self, socks_port: int = 9050, control_port: int = 9051):
        self.socks_port = socks_port
        self.control_port = control_port
        self.process: Optional[subprocess.Popen] = None
        self.data_dir: Optional[str] = None
        self._managed = False  # Whether we started this Tor process

    @property
    def proxy_url(self) -> str:
        return f"socks5://127.0.0.1:{self.socks_port}"

    def start(self, timeout: int = 60) -> bool:
        """
        Start a Tor instance. If Tor is already running on the default port,
        use that instead of starting a new one.
        """
        # Check if Tor is already running on this port
        if not is_port_available(self.socks_port):
            logger.info(f"Tor already running on port {self.socks_port}")
            return True

        if not is_tor_installed():
            logger.error("Tor is not installed. Install with: brew install tor")
            return False

        # Create a temporary data directory for this instance
        self.data_dir = tempfile.mkdtemp(prefix=f"tor_{self.socks_port}_")

        # Write a torrc config
        torrc_path = os.path.join(self.data_dir, "torrc")
        with open(torrc_path, "w") as f:
            f.write(f"SocksPort {self.socks_port}\n")
            f.write(f"ControlPort {self.control_port}\n")
            f.write(f"DataDirectory {self.data_dir}/data\n")
            f.write("CookieAuthentication 1\n")
            # Optimize for scraping
            f.write("CircuitBuildTimeout 10\n")
            f.write("LearnCircuitBuildTimeout 0\n")
            f.write("MaxCircuitDirtiness 600\n")  # New circuit every 10 min
            f.write("NewCircuitPeriod 30\n")

        try:
            self.process = subprocess.Popen(
                ["tor", "-f", torrc_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._managed = True

            # Wait for Tor to bootstrap
            start_time = time.time()
            while time.time() - start_time < timeout:
                if not is_port_available(self.socks_port):
                    logger.info(f"Tor started on SOCKS port {self.socks_port}")
                    return True
                time.sleep(1)

            logger.error(f"Tor failed to start within {timeout}s")
            self.stop()
            return False

        except Exception as e:
            logger.error(f"Failed to start Tor: {e}")
            return False

    def rotate_circuit(self) -> bool:
        """
        Request a new Tor circuit (new exit IP) via the control port.
        Uses the NEWNYM signal.
        """
        try:
            from stem import Signal
            from stem.control import Controller

            with Controller.from_port(port=self.control_port) as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
                logger.debug(f"Tor circuit rotated on port {self.socks_port}")
                # Tor needs a brief moment to build the new circuit
                time.sleep(3)
                return True

        except ImportError:
            # Fallback: use raw socket to send NEWNYM
            return self._rotate_raw()
        except Exception as e:
            logger.warning(f"Circuit rotation failed: {e}")
            return False

    def _rotate_raw(self) -> bool:
        """Rotate circuit using raw socket (no stem dependency)."""
        try:
            # Read the control auth cookie
            cookie_path = os.path.join(self.data_dir or "", "data", "control_auth_cookie")
            if not os.path.exists(cookie_path):
                return False

            with open(cookie_path, "rb") as f:
                cookie = f.read()

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(("127.0.0.1", self.control_port))
                s.send(b'AUTHENTICATE ""\r\n')
                response = s.recv(256)
                s.send(b"SIGNAL NEWNYM\r\n")
                response = s.recv(256)
                if b"250" in response:
                    time.sleep(3)
                    return True
            return False
        except Exception:
            return False

    def stop(self):
        """Stop the Tor instance if we started it."""
        if self.process and self._managed:
            try:
                self.process.terminate()
                self.process.wait(timeout=10)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass

    def __del__(self):
        self.stop()


class TorPool:
    """
    Manages multiple Tor instances for parallel scraping.
    Each instance has its own circuit = own IP address.
    """

    def __init__(self, num_instances: int = 3, base_socks_port: int = 9150, base_control_port: int = 9151):
        self.instances: List[TorInstance] = []
        self.num_instances = num_instances
        self.base_socks_port = base_socks_port
        self.base_control_port = base_control_port
        self._current = 0

    def start(self) -> int:
        """
        Start all Tor instances. Returns the number that started successfully.
        """
        started = 0
        for i in range(self.num_instances):
            socks_port = self.base_socks_port + (i * 2)
            control_port = self.base_control_port + (i * 2)

            instance = TorInstance(socks_port=socks_port, control_port=control_port)
            if instance.start():
                self.instances.append(instance)
                started += 1
            else:
                logger.warning(f"Failed to start Tor instance {i+1}")

        # Also try to use the default Tor instance if running
        default = TorInstance(socks_port=9050, control_port=9051)
        if not is_port_available(9050):
            self.instances.insert(0, default)
            started += 1

        logger.info(f"Tor pool: {started}/{self.num_instances} instances running")
        return started

    def get_next_proxy(self) -> Optional[str]:
        """Round-robin through available Tor proxies."""
        if not self.instances:
            return None
        instance = self.instances[self._current % len(self.instances)]
        self._current += 1
        return instance.proxy_url

    def rotate_all(self):
        """Rotate circuits on all instances."""
        for instance in self.instances:
            instance.rotate_circuit()

    def get_all_proxies(self) -> List[str]:
        """Get proxy URLs for all instances."""
        return [inst.proxy_url for inst in self.instances]

    def stop(self):
        """Stop all managed Tor instances."""
        for instance in self.instances:
            instance.stop()
        self.instances.clear()
