import bz2
import itertools
import json
import numpy
import pymysql
import warnings

from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Optional


from musk.core import Message, MySQL, Processor, Simulation, SQSQueue
from musk.misc.json import PythonObjectEncoder, as_python_object
from musk.misc.misc import Misc
from pydantic.dataclasses import dataclass


# warnings.simplefilter("ignore")
# warnings.simplefilter("default")


@dataclass
class PercolationModel:

    probability: float
    size: int
    created: datetime
    took: float
    observables: dict

    id: Optional[int] = None

    @classmethod
    def get_insert_query(cls):
        query = f"""
            INSERT INTO {cls._tablename}
            (probability, size, observables, took, created)
            VALUES (%(probability)s, %(size)s, %(observables)s, %(took)s, %(created)s)
        """
        return query

    @classmethod
    def get_by_size_probability_ids(cls, ids):
        query = f"""
            SELECT id, probability, size, observables, took, created
            FROM {cls._tablename}
            WHERE 
                size = %(size)s AND 
                round(probability, 6) = %(probability)s AND 
                id IN ({", ".join(map(str, ids))})
        """
        return query

    @classmethod
    def get_select_query_with_filters(cls, min_id, limit, probability=None, size=None):

        if probability:
            probability_filter = "round(probability, 6) = %(probability)s"
        else:
            probability_filter = ""

        if size:
            size_filter = "size = %(size)s"
        else:
            size_filter = ""

        if size_filter and probability_filter:
            where_clause = f"WHERE {size_filter} AND {probability_filter}"
        elif size_filter and not probability_filter:
            where_clause = f"WHERE {size_filter}"
        elif not size_filter and probability_filter:
            where_clause = f"WHERE {probability_filter}"
        else:
            raise ValueError("Unexpected combination")

        where_clause = f"{where_clause} AND id > {min_id}"
        limit_clause = f"LIMIT {limit}"

        return f"""
            SELECT id, probability, size, observables, took, created
            FROM {cls._tablename}
            {where_clause}
            {limit_clause}
        """

    @classmethod
    def from_db(cls, row: dict):
        row = row.copy()  # Don't change original row
        row["observables"] = bz2.decompress(row["observables"])
        row["observables"] = json.loads(
            row["observables"], object_hook=as_python_object
        )
        return cls(**row)

    def to_db(self):
        observables_string = json.dumps(
            self.observables, cls=PythonObjectEncoder
        ).encode("utf-8")

        observables_compressed = pymysql.Binary(bz2.compress(bytes(observables_string)))
        model_dict = asdict(self)
        model_dict.update(dict(observables=observables_compressed))
        return model_dict


@dataclass
class PercolationStatsModel:

    simulation_id: int

    probability: float
    size: int
    has_percolated: bool
    average_cluster_size: float
    cluster_size_histogram: dict
    average_correlation_length: float
    took: float
    created: datetime

    id: Optional[int] = None

    @classmethod
    def get_insert_query(cls):
        return f"""
            INSERT INTO {cls._tablename} (simulation_id, size, probability,
                has_percolated, cluster_size_histogram, average_cluster_size, average_correlation_length, created, took)
            VALUES (%(simulation_id)s, %(size)s, %(probability)s, %(has_percolated)s,
                %(cluster_size_histogram)s, %(average_cluster_size)s, %(average_correlation_length)s,
                %(created)s, %(took)s)
            ON DUPLICATE KEY UPDATE
            has_percolated = %(has_percolated)s,
            cluster_size_histogram = %(cluster_size_histogram)s,
            average_cluster_size = %(average_cluster_size)s,
            average_correlation_length = %(average_correlation_length)s
        """

    @classmethod
    def get_update_query(cls, key: tuple, attributes: dict) -> str:

        key_name, key_value = key
        update_strings = [f"{key} = %({key})s" for key in attributes]
        update_string = ",\n".join(update_strings)

        query = f"""
            UPDATE {cls._tablename}
            SET {update_string}
            WHERE {key_name} = %({key_name})s
        """
        return query

    def to_db(self):

        model_dict = asdict(self)
        model_dict["cluster_size_histogram"] = json.dumps(
            model_dict["cluster_size_histogram"]
        )
        return model_dict


class PercolationSimulation(Simulation):

    probability: float
    size: int

    took: timedelta
    created: datetime

    _has_run = False

    def __init__(self, probability: float, size: int):
        self.probability = probability
        self.size = size
        self.created = datetime.now()

    @property
    def model(self) -> dict:
        if not self._has_run:
            raise ValueError("Simulation still did not run.")

        ModelClass = self._get_model_class()
        return ModelClass(
            probability=self.probability,
            size=self.size,
            observables=self._observables,
            took=self.took.total_seconds(),
            created=self.created,
        )

    def _get_model_class(self):
        return self.model_class

    def _get_lattice_class(self):
        return self.lattice_class

    def _get_insert_query(self) -> str:
        return self._get_model_class().get_insert_query()

    def _get_update_query(self) -> str:
        return self._get_model_class().get_update_query()

    def run(self):
        LatticeClass = self._get_lattice_class()
        lattice = LatticeClass(self.size)
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


class PercolationProcessor(Processor):
    def _get_simulation_class(self):
        return self.simulation_class

    def process(self, message: Message):
        parameters = message.body["parameters"]
        repeat = message.body["repeat"]

        for index in range(repeat):
            SimulationClass = self._get_simulation_class()
            simulation = SimulationClass(**parameters)
            simulation.execute()
            mysql = MySQL()
            mysql.execute(simulation._get_insert_query(), simulation.model.to_db())
            mysql = None
