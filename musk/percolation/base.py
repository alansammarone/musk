import bz2
import itertools
import json
import warnings
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Optional

import numpy
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
    def get_select_query_with_filters(cls, probability=None, size=None):

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

        return f"""
            SELECT id, probability, size, observables, took, created
            FROM {cls._tablename}
            {where_clause}
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

        observables_compressed = bz2.compress(bytes(observables_string))
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


class PercolationStatsProcessor(Processor):

    CHUNK_SIZE = 100

    def _get_has_percolated(self, model) -> bool:
        clusters = model.observables["clusters"]
        boundaries = self.lattice.get_boundaries()
        top_boundary, bottom_boundary = list(boundaries)
        has_percolated = False
        for cluster in clusters:
            if (cluster & top_boundary) and (cluster & bottom_boundary):
                has_percolated = True
                break
        return has_percolated

    def _get_cluster_correlation_length(self, cluster) -> float:
        correlation_length, n_combinations = 0, 0
        combinations = itertools.combinations(cluster, 2)

        for node1, node2 in combinations:
            x1, y1 = node1
            x2, y2 = node2
            distance = (y2 - y1) ** 2 + (x2 - x1) ** 2
            correlation_length += distance
            n_combinations += 1

        return correlation_length / n_combinations

    def _get_average_correlation_length(self, model) -> float:
        clusters = model.observables["clusters"]
        top_boundary, bottom_boundary = self.lattice.get_boundaries()
        average = 0
        for cluster in clusters:
            cluster_size = len(cluster)

            has_percolated = bool(
                cluster_size >= self.lattice.get_size()
                and (cluster & top_boundary)
                and (cluster & bottom_boundary)
            )
            if has_percolated:
                continue
            if cluster_size == 1:
                continue
            average += self._get_cluster_correlation_length(cluster)

        return average / len(clusters)

    def _get_cluster_size_histogram(self, model) -> list:
        bins = 10000
        clusters = model.observables["clusters"]
        number_of_nodes = self.lattice.get_number_of_nodes()
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
        try:
            # Warning: We're assuming that no clusters
            # means average_size = 0
            average_size = sum(cluster_sizes) / len(clusters)
            number_of_nodes = self.lattice.get_number_of_nodes()
            average_size = average_size / number_of_nodes
        except ZeroDivisionError:
            pass
        return average_size

    def _get_stats_for_model(self, model):

        has_percolated = self._get_has_percolated(model)
        cluster_size_histogram = self._get_cluster_size_histogram(model)
        average_cluster_size = self._get_average_cluster_size(model)
        average_correlation_length = self._get_average_correlation_length(model)
        return dict(
            has_percolated=has_percolated,
            cluster_size_histogram=cluster_size_histogram,
            average_cluster_size=average_cluster_size,
            average_correlation_length=average_correlation_length,
        )

    def _get_simulation_model_class(self):
        return self.simulation_model_class

    def _get_stats_model_class(self):
        return self.stats_model_class

    def _get_lattice_class(self):
        return self.lattice_class

    def _map_row_to_model(self, row):
        return self._get_simulation_model_class().from_db(row)

    def _insert_stats_models(self, models):
        mysql = MySQL()
        start = datetime.now()
        StatsModelClass = self._get_stats_model_class()
        for model in models:
            mysql.execute(StatsModelClass.get_insert_query(), model.to_db())
        end = datetime.now()
        took = round((end - start).total_seconds(), 2)

        self._logger.info(f"Inserting chunk took {took}s.")

    def _get_simulation_rows(self, parameters):
        mysql = MySQL()
        SimulationModelClass = self._get_simulation_model_class()
        query = SimulationModelClass.get_select_query_with_filters(**parameters)
        query_start = datetime.now()
        mysql_rows = mysql.fetch(query, parameters)
        query_end = datetime.now()
        query_took = (query_end - query_start).total_seconds()
        self._logger.debug(f"Stats query took {query_took}s.")
        return mysql_rows

    def _process_model_chunk(self, chunk, parameters):
        chunk_took = 0
        stats_models = []
        StatsModelClass = self._get_stats_model_class()
        for model in chunk:
            start = datetime.now()
            stats_field = self._get_stats_for_model(model)
            end = datetime.now()
            took = (end - start).total_seconds()
            stats_model = StatsModelClass(
                simulation_id=model.id,
                size=model.size,
                probability=model.probability,
                created=datetime.now(),
                took=took,
                **stats_field,
            )
            stats_models.append(stats_model)
        self._logger.info(f"Chunk processing took {chunk_took:.2f}s.")
        self._insert_stats_models(stats_models)

    def process(self, message: Message):
        parameters = message.body["parameters"]
        LatticeClass = self._get_lattice_class()
        self.lattice = LatticeClass(parameters["size"])
        simulation_rows = self._get_simulation_rows(parameters)
        simulation_models = map(self._map_row_to_model, simulation_rows)
        stats_models_chunk = Misc.chunkenize(simulation_models, self.CHUNK_SIZE)
        total_count = 0
        for chunk in stats_models_chunk:
            self._process_model_chunk(chunk, parameters)
            total_count += len(chunk)

        self._logger.info("Processed %s input models.", total_count)
