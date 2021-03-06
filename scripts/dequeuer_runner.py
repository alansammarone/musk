import sys
import concurrent.futures
import logging
import multiprocessing
import signal
import time
from typing import List

from musk.config import DequeuerConfig
from musk.core import Dequeuer
from musk.percolation import (
    P1LProcessor,
    P1LQueue,
    P1LStatsProcessor,
    P1LStatsQueue,
    P2SProcessor,
    P2SQueue,
    P2SStatsProcessor,
    P2SStatsQueue,
    P2MQueue,
    P2MProcessor,
    P2MStatsProcessor,
    P2MStatsQueue,
)
from musk.misc.logging import setup_logging

setup_logging()  # This will also be called from child processes


class GracefulKiller:
    kill_now = False

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self._logger.info("Received signal to exit gracefully.")
        self.kill_now = True


def async_wrapper(dequeuers: List[Dequeuer]):
    logger = logging.getLogger("root.async_wrapper")
    for dequeuer in dequeuers:
        try:
            dequeuer.dequeue()
        except:
            logger.exception("Exception in worker: ")


class DequeuerRunner:

    MP_CONTEXT = "spawn"

    def __init__(self, dequeuers: List[Dequeuer]):
        self._logger = logging.getLogger(__name__)

        self._dequeuers = dequeuers
        self._config = DequeuerConfig

        self._process_count = self._config.PROCESS_COUNT
        self._processes = []

        self._killer = GracefulKiller()

        multiprocessing.set_start_method(self.MP_CONTEXT)

    def dequeue(self) -> None:

        while True:

            # If we still didnt reach the number of desired processes, spawn a new one
            if len(self._processes) < self._process_count:
                self._processes.append(self._spawn_process())
                self._logger.debug("Spawning new process")

            alive_processes = []
            for process in self._processes:
                if process.is_alive():
                    alive_processes.append(process)
                else:
                    self._logger.debug("Process %s is dead.", process.pid)
                    process.close()

            self._processes = alive_processes

            # If we received a SIGTERM, wait on child processes and then quit
            if self._killer.kill_now:
                for process in self._processes:
                    self._logger.debug("Waiting on PID %s", process.pid)
                    process.join()

                break

            time.sleep(2)

    def _spawn_process(self) -> multiprocessing.Process:
        process = multiprocessing.Process(target=async_wrapper, args=(self._dequeuers,))
        process.start()
        return process


if __name__ == "__main__":
    from musk.core import Dequeuer

    queue_env = DequeuerConfig.ENV

    dequeuers = [
        Dequeuer(P2MQueue(queue_env), P2MProcessor()),
        Dequeuer(P2MStatsQueue(queue_env), P2MStatsProcessor()),
        # Dequeuer(P2SStatsQueue(queue_env), P2SStatsProcessor()),
        # Dequeuer(P2SQueue(queue_env), P2SProcessor()),
        # Dequeuer(P2SStatsQueue(queue_env), P2SStatsProcessor()),
        # Dequeuer(P1LQueue(queue_env), P1LProcessor()),
        # Dequeuer(P1LStatsQueue(queue_env), P1LStatsProcessor()),
    ]
    DequeuerRunner(dequeuers).dequeue()
