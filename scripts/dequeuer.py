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


class PythonObjectEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, frozenset):
            return {"_python_frozenset": str(obj)}
        else:
            return super().default(obj)


def as_python_object(dct):
    if "_python_object" in dct:
        return eval(dct["_python_object"])
    return dct


# data = [
#     1,
#     2,
#     3,
#     set(["knights", "who", "say", "ni"]),
#     {"key": "value"},
#     Decimal("3.14"),
# ]
# j = dumps(data, cls=PythonObjectEncoder)
# print(loads(j, object_hook=as_python_object))


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

        s = bytes(
            json.dumps(self._observables, cls=PythonObjectEncoder).encode("utf-8")
        )

        c = bz2.compress(s)
        return dict(
            probability=self.probability,
            size=self.size,
            observables=bz2.compress(
                bytes(
                    json.dumps(self._observables, cls=PythonObjectEncoder).encode(
                        "utf-8"
                    )
                )
            ),
            took=self.took.total_seconds(),
            created=self.created.strftime("%Y-%m-%d %H:%M:%S"),
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


class MySQL:

    _connection_timeout = 5

    @staticmethod
    def _get_connection_config():

        env = Env()
        with env.prefixed("MYSQL_"):
            host = env("HOST", "localhost")
            port = env.int("PORT", 3306)
            user = env("USER", "root")
            password = env("PASSWORD", None)
            database = env("DATABASE", "musk")

        return dict(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connection_timeout=MySQL._connection_timeout,
        )

    @staticmethod
    def get_new_connection():
        config = MySQL._get_connection_config()
        print(config)
        connection = mysql.connector.connect(**config)
        return connection


class Percolation2DSquareQueue(SQSQueue):
    name = "percolation_2d_square"


def run_simulation(parameters, connection):
    simulation = Percolation2DSimulation(**parameters)
    cursor = connection.cursor()

    try:

        simulation.execute()
        cursor.execute(simulation.get_query(), simulation.model)
        connection.commit()

    except:
        connection.rollback()
        raise
    finally:
        cursor.close()
        del simulation


def process_message(message):
    connection = None
    try:
        repeat = message.body["repeat"]
        parameters = message.body["parameters"]
        logger.info(f"Processing message ({message.id}) : {message.body}")
        connection = MySQL.get_new_connection()
        start = datetime.now()
        for _ in range(repeat):
            run_simulation(parameters, connection)
        end = datetime.now()
        took = (end - start).total_seconds()
        took = round(took, 3)
        message.delete()
        logger.info(f"Success processing message ({message.id}), took {took}s")
    except:
        message.requeue()
        logger.exception("Exception(process_message):")
    finally:
        if connection:
            connection.close()
            del connection


def listen_to_queue():
    try:
        queue = Percolation2DSquareQueue("dev")
        for message in queue.read(1):
            process_message(message)
    except:
        logger.exception("Exception(listen_to_queue):")


listen_to_queue()
if __name__ == "__main__":

    cpu_count = multiprocessing.cpu_count()
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=cpu_count, mp_context=multiprocessing.get_context("spawn"),
    ) as executor:
        futures = []
        for _ in range(cpu_count):
            futures.append(executor.submit(listen_to_queue))

# messages = list(queue.read())
# for message in messages:
#     print(message.id)
# for message in queue.read_forever():
#     print(message.id)
#     message.requeue()
# results = executor.map(process_message, messages)
# for result in results:
#     print(result)


# from collections import namedtuple

# Message = namedtuple("Message", ["id", "body", "delete", "requeue"])
# sample_body = dict(repeat=1, parameters={"probability": 0.59, "size": 256})
# sample_message = Message("myid", sample_body, lambda: None, lambda: None)
# process_message(sample_message)
