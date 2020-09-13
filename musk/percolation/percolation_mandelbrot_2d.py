import bz2
import itertools

import math
import json
import pymysql
from dataclasses import asdict
from datetime import datetime
from typing import Optional
from pydantic.dataclasses import dataclass
from musk.core import Message, MySQL, SQSQueue
from musk.lattices import Square2DPeriodicLattice
from musk.misc.json import PythonObjectEncoder, as_python_object
from musk.percolation.base import (
    PercolationModel,
    PercolationProcessor,
    PercolationSimulation,
    PercolationStatsModel,
)

from musk.percolation.stats import PercolationStatsProcessor


class P2MQueue(SQSQueue):
    name = "percolation_2d_mandelbrot"


class P2MStatsQueue(SQSQueue):
    name = "percolation_2d_mandelbrot_stats"


@dataclass
class P2MModel:

    _tablename = "p2m"

    probability: float
    size: int
    index: int
    initial_size: int
    created: datetime
    observables: dict

    id: Optional[int] = None

    @classmethod
    def get_insert_query(cls):
        query = f"""
            INSERT INTO {cls._tablename}
            (probability, size, initial_size, `index`, observables, created)
            VALUES (%(probability)s, %(size)s, %(initial_size)s, %(index)s, %(observables)s, %(created)s)
        """
        return query

    @classmethod
    def get_by_size_probability_ids(cls, ids):
        query = f"""
            SELECT id, probability, size, initial_size, `index`, observables, created
            FROM {cls._tablename}
            WHERE 
                size = %(size)s AND 
                round(probability, 6) = %(probability)s AND 
                id IN ({", ".join(map(str, ids))})
        """
        return query

    @classmethod
    def get_select_query_with_filters(cls, min_id, limit, obability=None, size=None):

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
            SELECT id, probability, size, observables, created
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
class P2MStatsModel:

    _tablename = "p2m_stats"

    simulation_id: int

    probability: float
    size: int
    initial_size: int
    index: int
    has_percolated: bool
    average_cluster_size: float
    cluster_size_histogram: dict
    average_correlation_length: float
    created: datetime

    id: Optional[int] = None

    @classmethod
    def get_insert_query(cls):
        return f"""
            INSERT INTO {cls._tablename} (simulation_id, size, index, probability,
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
        non_key_attributes = {
            key: value for key, value in attributes.items() if key != key_name
        }
        column_names = "`, `".join(attributes)
        value_strings = [f"%({key})s" for key in attributes]
        value_string = ", ".join(value_strings)
        update_strings = [f"`{key}` = %({key})s" for key in non_key_attributes]
        update_string = ",\n".join(update_strings)

        query = f"""
            INSERT INTO {cls._tablename} (`{column_names}`)
            VALUES ({value_string})
            ON DUPLICATE KEY UPDATE
            {update_string}

        """
        return query

    def to_db(self):

        model_dict = asdict(self)
        model_dict["cluster_size_histogram"] = json.dumps(
            model_dict["cluster_size_histogram"]
        )
        return model_dict


class P2MSimulation(PercolationSimulation):

    model_class = P2MModel
    lattice_class = Square2DPeriodicLattice

    def __init__(self, probability: float, initial_size: int, n_divisions: int):

        self.probability = probability
        self.initial_size = initial_size
        self.n_divisions = n_divisions
        self.created = datetime.now()
        self._models = []

    def _get_model_from_lattice(self, lattice):
        division_index = math.log2(lattice.get_size())
        return P2MModel(
            probability=self.probability,
            size=lattice.get_size(),
            index=division_index,
            initial_size=self.initial_size,
            observables=dict(clusters=lattice.get_clusters_with_state(1)),
            created=self.created,
        )

    def run(self):
        models = []
        LatticeClass = self._get_lattice_class()
        lattice = LatticeClass(self.initial_size)
        lattice.fill_randomly(
            [0, 1], state_weights=[1 - self.probability, self.probability]
        )

        models.append(self._get_model_from_lattice(lattice))

        for index in range(self.n_divisions):
            lattice.divide()
            lattice.change_state_with_probability(0, 1, self.probability)
            models.append(self._get_model_from_lattice(lattice))

        """
        print("----" * 100)
        from musk.misc.lattice_state_image import LatticeStateImage
        import random

        # LatticeStateImage(lattice).save(f"/tmp/test{random.choice(range(50000))}.png")
        LatticeStateImage(lattice).save(f"/tmp/test.png")
        """

        return models

    def execute(self):
        start = datetime.now()
        self._models = self.run()
        end = datetime.now()
        self._has_run = True

    @property
    def models(self) -> list:
        if not self._has_run:
            raise ValueError("Simulation still did not run.")

        return self._models

    @property
    def model(self):
        # For mandelbrot percolation, we generate multiple models per simulation.
        # So we use .models, not .model
        raise NotImplementedError


class P2MProcessor(PercolationProcessor):
    simulation_class = P2MSimulation

    def process(self, message: Message):
        parameters = message.body["parameters"]
        repeat = message.body["repeat"]
        for index in range(repeat):
            SimulationClass = self._get_simulation_class()
            simulation = SimulationClass(**parameters)
            simulation.execute()
            mysql = MySQL()
            for model in simulation.models:
                mysql.execute(simulation._get_insert_query(), model.to_db())
            mysql = None


class P2MStatsProcessor(PercolationStatsProcessor):

    simulation_model_class = P2MModel
    stats_model_class = P2MStatsModel
    lattice_class = Square2DPeriodicLattice
