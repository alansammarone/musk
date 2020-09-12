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
