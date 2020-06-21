import concurrent.futures
import logging
import multiprocessing
from typing import List

from musk.config import DequeuerConfig
from musk.core import Dequeuer
from musk.percolation import PS2Processor, PS2Queue, PS2StatsProcessor, PS2StatsQueue
from musk.misc.logging import setup_logging

setup_logging()  # This will also be called from child processes


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
        self._dequeuers = dequeuers
        self._config = DequeuerConfig
        self._cpu_count = self._config.PROCESS_COUNT
        self._logger = logging.getLogger(__file__)

    def dequeue(self) -> None:
        while True:
            self._logger.debug("Restarting pool.")
            pool = concurrent.futures.ProcessPoolExecutor(
                max_workers=self._cpu_count,
                mp_context=multiprocessing.get_context(self.MP_CONTEXT),
            )
            for _ in range(self._cpu_count):
                future = pool.submit(async_wrapper, (self._dequeuers))

            pool.shutdown(wait=True)
            break


if __name__ == "__main__":
    from musk.core import Dequeuer

    queue_env = DequeuerConfig.ENV

    dequeuers = [
        # Dequeuer(PS2Queue(queue_env), PS2Processor()),
        Dequeuer(PS2StatsQueue(queue_env), PS2StatsProcessor()),
    ]
    DequeuerRunner(dequeuers).dequeue()
