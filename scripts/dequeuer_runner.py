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
)
from musk.misc.logging import setup_logging

setup_logging()  # This will also be called from child processes


class GracefulKiller:
    kill_now = False

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self._logger.info("Received signal to exit gracefully.")
        self.kill_now = True


def async_wrapper(dequeuers: List[Dequeuer]):
    logger = logging.getLogger("root.async_wrapper")
    sleep_for_seconds = 10
    number_of_reads = 0
    MAX_READS = 1
    # killer = GracefulKiller()
    # while killer.kill_now is False and number_of_reads < MAX_READS:
    #     for dequeuer in dequeuers:
    #         try:
    #             dequeuer.dequeue()
    #         except:
    #             logger.exception("Exception in worker: ")
    #             sys.exit(0)
    #     number_of_reads += 1
    #     time.sleep(sleep_for_seconds)

    for dequeuer in dequeuers:
        try:
            dequeuer.dequeue()
        except:
            logger.exception("Exception in worker: ")
            sys.exit(0)

    logger.info("Exiting gracefully.")


class DequeuerRunner:

    MP_CONTEXT = "spawn"

    def __init__(self, dequeuers: List[Dequeuer]):
        self._dequeuers = dequeuers
        self._config = DequeuerConfig
        self._cpu_count = self._config.PROCESS_COUNT
        self._logger = logging.getLogger(__name__)

    def dequeue(self) -> None:

        self._logger.debug("Starting pool")
        pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=self._cpu_count,
            mp_context=multiprocessing.get_context(self.MP_CONTEXT),
        )
        for _ in range(self._cpu_count):
            future = pool.submit(async_wrapper, (self._dequeuers))

        pool.shutdown(wait=True)


if __name__ == "__main__":
    from musk.core import Dequeuer

    queue_env = DequeuerConfig.ENV

    dequeuers = [
        Dequeuer(P2SQueue(queue_env), P2SProcessor()),
        Dequeuer(P2SStatsQueue(queue_env), P2SStatsProcessor()),
        # Dequeuer(P1LQueue(queue_env), P1LProcessor()),
        # Dequeuer(P1LStatsQueue(queue_env), P1LStatsProcessor()),
    ]
    DequeuerRunner(dequeuers).dequeue()
