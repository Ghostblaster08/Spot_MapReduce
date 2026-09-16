import signal
import threading
import logging

log = logging.getLogger("worker.sentinel")

class PreemptionHandler:
    def __init__(self):
        self.is_preempting = threading.Event()
        self.is_shutdown = threading.Event()
        self._install_handlers()

    def _install_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_preemption)
        signal.signal(signal.SIGUSR1, self._handle_preemption)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        log.info("Installed signal handlers for SIGTERM, SIGUSR1, and SIGINT")

    def _handle_preemption(self, signum, frame):
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGUSR1"
        log.warning(f"[{sig_name}] Preemption warning signal received! Setting is_preempting flag.")
        self.is_preempting.set()

    def _handle_shutdown(self, signum, frame):
        log.warning("[SIGINT] Shutdown signal received. Stopping worker.")
        self.is_shutdown.set()
        self.is_preempting.set()
