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
import pickle


"""
-----
"""

from musk.core.sql import MySQL
from musk.config.config import MySQLConfig


class PythonObjectEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, frozenset):
            return {"_python_frozenset": str(obj)}
        else:
            return super().default(obj)


def as_python_object(dct):
    if "_python_frozenset" in dct:
        return eval(dct["_python_frozenset"])
    return dct


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


class Percolation2DSquareStatsProcessor(SQSMessageProcessor):
    def _get_fetch_query(self):
        return """
            SELECT
                id,
                probability,
                size,
                observables,
                took,
                created
            FROM percolation_2d_square
            WHERE round(probability, 6) = %(probability)s
            ORDER BY RAND()
        """

    def _get_write_query(self):
        return """
            INSERT INTO percolation_2d_square_stats (
                percolation_2d_square_id,
                size,
                probability,
                has_percolated,
                cluster_size_histogram,
                average_cluster_size,
                created,
                took
            )
            VALUES
            (
                %(percolation_2d_square_id)s,
                %(size)s,
                %(probability)s,
                %(has_percolated)s,
                %(cluster_size_histogram)s,
                %(average_cluster_size)s,
                %(created)s,
                %(took)s
            )
            ON DUPLICATE KEY UPDATE
            has_percolated = %(has_percolated)s,
            cluster_size_histogram = %(cluster_size_histogram)s,
            average_cluster_size = %(average_cluster_size)s

        """

    def _get_has_percolated(self, model) -> bool:
        clusters = model.observables["clusters"]

        lattice = Square2DLattice(model.size)
        boundaries = lattice.get_boundaries()
        top_boundary, bottom_boundary = list(boundaries)
        has_percolated = False
        for cluster in clusters:
            if (cluster & top_boundary) and (cluster & bottom_boundary):
                has_percolated = True
                break
        return has_percolated

    def _get_cluster_size_histogram(self, model) -> list:
        bins = 10000
        clusters = model.observables["clusters"]
        number_of_nodes = model.size ** 2  # LATTICE SPECIFIC!
        cluster_sizes = map(lambda cluster: len(cluster) / number_of_nodes, clusters)
        hist, bin_edges = numpy.histogram(list(cluster_sizes), bins=bins, range=(0, 1))
        cluster_size_histogram = hist.tolist()
        cluster_size_histogram = {
            size: count
            for size, count in enumerate(cluster_size_histogram)
            if count > 0
        }
        return cluster_size_histogram

    def _get_average_cluster_size(self, model) -> float:

        average_size = 0
        clusters = model.observables["clusters"]
        cluster_sizes = map(lambda cluster: len(cluster), clusters)
        average_size = sum(cluster_sizes) / len(clusters)
        return average_size / model.size ** 2  # LATTICE SPECIFIC!

    def _get_stats_for_model(self, model):

        has_percolated = self._get_has_percolated(model)
        cluster_size_histogram = self._get_cluster_size_histogram(model)
        average_cluster_size = self._get_average_cluster_size(model)
        return dict(
            has_percolated=self._get_has_percolated(model),
            cluster_size_histogram=json.dumps(cluster_size_histogram),
            average_cluster_size=average_cluster_size,
        )

    def _map_row_to_model(self, row):
        return Percolatation2DSquareModel.from_db(row)

    def _insert_stats_models(self, models):
        mysql = MySQL()
        start = datetime.now()
        for model in models:
            self._try_and_insert_model(model, mysql)
        end = datetime.now()
        took = round((end - start).total_seconds(), 2)

        logger.info(f"Inserting chunk took {took}s.")

    def _try_and_insert_model(self, model, mysql):
        try:

            mysql.execute(self._get_write_query(), model)
        except IntegrityError as err:
            # Never reached since we have ON DUPLICATE KEY
            if int(err.errno) == 1062:
                logger.info(
                    "Parent ID %s already exists. Skipping.",
                    model["percolation_2d_square_id"],
                )
            else:
                raise

    def process(self, message):
        parameters = message.body["parameters"]

        mysql = MySQL()
        query = self._get_fetch_query()
        query_start = datetime.now()
        mysql_rows = mysql.fetch(query, parameters)
        query_end = datetime.now()
        query_took = (query_end - query_start).total_seconds()
        logger.info(f"Stats query took {query_took}s.")
        models = map(self._map_row_to_model, mysql_rows)
        stats_models_chunk = []
        chunk_size = 0
        total_count = 0
        max_chunk_size = 1000
        chunk_took = 0

        for model in models:
            if total_count % 500 == 0:
                logger.info("Read 500 rows.")
            start = datetime.now()
            stats_field = self._get_stats_for_model(model)
            end = datetime.now()
            took = (end - start).total_seconds()
            chunk_took += took
            stats_model = dict(
                percolation_2d_square_id=model.id,
                size=model.size,
                probability=model.probability,
                created=datetime.now(),
                took=took,
                **stats_field,
            )
            stats_models_chunk.append(stats_model)
            chunk_size += 1
            total_count += 1

            if chunk_size >= max_chunk_size:
                logger.info(f"Chunk processing took {chunk_took:.2f}s.")
                self._insert_stats_models(stats_models_chunk)
                stats_models_chunk = []
                chunk_size = 0

        self._insert_stats_models(stats_models_chunk)
        logger.info(f"Chunk processing took {chunk_took:.2f}s.")

        logger.info("Processed %s input models.", total_count)


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
