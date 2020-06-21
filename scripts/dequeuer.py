import concurrent.futures
from musk.core.sqs import SQSQueue
from musk.lattices import Square2DLattice
import mysql.connector
from datetime import datetime, timedelta
import json
from environs import Env
import bz2
import logging
import multiprocessing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__file__)
"""
--------
"""

from decimal import Decimal
from base64 import b64encode, b64decode
from json import dumps, loads, JSONEncoder

"""
-----
"""


class Percolation2DSimulation:

    probability: float
    size: int

    took: timedelta
    created: datetime

    _has_run = False
    _tablename = "percolation_2d_square"

    def __init__(self, probability: float, size: int):
        self.probability = probability
        self.size = size

        self.created = datetime.now()

    @property
    def model(self) -> dict:
        if not self._has_run:
            raise ValueError("Simulation still did not run.")

        return Percolatation2DSquareModel(
            probability=self.probability,
            size=self.size,
            observables=self._observables,
            took=self.took.total_seconds(),
            created=self.created,
        )

    def run(self):

        lattice = Square2DLattice(self.size)
        lattice.fill_randomly(
            [0, 1], state_weights=[1 - self.probability, self.probability]
        )
        clusters = lattice.get_clusters_with_state(1)
        return dict(clusters=clusters)

    def execute(self):
        start = datetime.now()
        self._observables = self.run()
        end = datetime.now()
        self.took = end - start
        self._has_run = True

    def get_query(self):
        query = f"""
            INSERT INTO {self._tablename}
            (probability, size, observables, took, created)
            VALUES (%(probability)s, %(size)s, %(observables)s, %(took)s, %(created)s)
        """
        return query


class Percolation2DSquareQueue(SQSQueue):
    name = "percolation_2d_square"


class Percolation2DSquareStatsQueue(SQSQueue):
    name = "percolation_2d_square_stats"


from pydantic.dataclasses import dataclass
from dataclasses import asdict
from typing import Optional


@dataclass
class Percolatation2DSquareModel:

    probability: float
    size: int
    created: datetime
    took: float
    observables: dict

    id: Optional[int] = None

    @classmethod
    def from_db(cls, row: dict):
        row["observables"] = bz2.decompress(row["observables"])
        row["observables"] = json.loads(
            row["observables"], object_hook=as_python_object
        )
        return cls(**row)

    def to_db(self):
        observables_string = json.dumps(
            self.observables, cls=PythonObjectEncoder
        ).encode("utf-8")

        observables_compressed = bz2.compress(bytes(observables_string))
        model_dict = asdict(self)
        model_dict.update(dict(observables=observables_compressed))
        return model_dict


from musk.config import DequeuerConfig
from musk.core.sqs import SQSMessage
from collections import namedtuple
from mysql.connector.errors import IntegrityError
import numpy


class SQSMessageProcessor:
    pass


class Percolation2DSimulationProcessor(SQSMessageProcessor):
    def process(self, message):

        parameters = message.body["parameters"]
        repeat = message.body["repeat"]

        for index in range(repeat):
            simulation = Percolation2DSimulation(**parameters)
            simulation.execute()
            mysql = MySQL()
            mysql.execute(simulation.get_query(), simulation.model.to_db())
            mysql = None


class PercolationDequeuer:
    def __init__(self):

        self._config = DequeuerConfig
        self._queue_env = self._config.ENV
        self._queues_processors = [
            (
                Percolation2DSquareQueue(self._queue_env),
                Percolation2DSimulationProcessor(),
            ),
            (
                Percolation2DSquareStatsQueue(self._queue_env),
                Percolation2DSquareStatsProcessor(),
            ),
        ]

    def _send_message_to_processor(self, message, processor):

        logger.info(f"Processing message ({message.id}) : {message.body}")
        start = datetime.now()
        processor.process(message)
        message.delete()
        end = datetime.now()
        took = (end - start).total_seconds()
        took = round(took, 3)
        logger.info(f"Success processing message ({message.id}), took {took}s")

    def dequeue(self):
        for queue, processor in self._queues_processors:
            logger.info(f"Reading queue {queue.get_queue_name()}...")
            number_of_messages_per_read = self._config.MESSAGES_PER_READ
            messages = queue.read(number_of_messages_per_read)
            for message in messages:
                self._send_message_to_processor(message, processor)


def async_wrapper():
    try:
        PercolationDequeuer().dequeue()
    except:
        logger.exception("Exception in worker: ")


class Dequeuer:

    MP_CONTEXT = "spawn"

    def __init__(self):
        self._config = DequeuerConfig
        self._cpu_count = self._config.PROCESS_COUNT

    def dequeue(self):
        while True:
            logger.info("Restarting pool...")
            pool = concurrent.futures.ProcessPoolExecutor(
                max_workers=self._cpu_count,
                mp_context=multiprocessing.get_context(Dequeuer.MP_CONTEXT),
            )
            for _ in range(self._cpu_count):
                future = pool.submit(async_wrapper)

            pool.shutdown(wait=True)
            break


if __name__ == "__main__":
    Dequeuer().dequeue()
