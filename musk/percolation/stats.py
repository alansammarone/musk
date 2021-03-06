import itertools
import json
import numpy
import random

from collections import defaultdict
from datetime import datetime

from musk.core import MySQL, Processor


class HasPercolatedHelper:
    def __init__(self, lattice):
        self._lattice = lattice
        self._clusters = {}

    def has_percolated(self, cluster):
        try:
            return self._clusters[cluster]
        except KeyError:
            self._clusters[cluster] = self._has_percolated(cluster)
            return self._clusters[cluster]

    def _has_percolated(self, cluster):
        return self._has_percolated_infinite(cluster)

    def _has_percolated_finite(self, cluster):
        cluster_size = len(cluster)
        top_boundary, bottom_boundary = self._lattice.get_boundaries()
        has_percolated = bool(
            cluster_size >= self._lattice.get_size()
            and (cluster & top_boundary)
            and (cluster & bottom_boundary)
        )
        return has_percolated

    def _has_percolated_infinite(self, cluster):
        # Method used to compute has_percolated in an infinite lattice.
        # Idea is to count number of different nodes in each direction
        # and check if this is larger than the size of the lattice.

        # WARNING: 2D specific

        seen_i = set([i for (i, j) in cluster])
        if len(seen_i) >= self._lattice.get_size():
            return True

        seen_j = set([j for (i, j) in cluster])
        if len(seen_j) >= self._lattice.get_size():
            return True

        return False


class StatsCalculation:
    def __init__(self, lattice, model, has_percolated_helper):
        self.lattice = lattice
        self.model = model
        self.has_percolated_helper = has_percolated_helper

    def _encode_list_as_dict(self, list_: list) -> dict:
        return {size: round(count, 4) for size, count in enumerate(list_) if count > 0}

    def encode_for_db(self, value):
        return value


class HasPercolatedCalculation(StatsCalculation):
    def calculate(self) -> bool:
        clusters = self.model.observables["clusters"]
        has_percolated = False
        for cluster in clusters:
            if self.has_percolated_helper.has_percolated(cluster):
                has_percolated = True
                break
        return has_percolated


class CorrelationFunctionCalculation(StatsCalculation):

    SAMPLES = 2 ** 17  # 128K
    BINS = 10000

    def __init__(self, lattice, model, has_percolated_helper):
        self.lattice = lattice
        self.model = model
        self.has_percolated_helper = has_percolated_helper
        self.node_to_cluster_map = self._get_node_to_cluster_map()
        self.lattice_size = self.lattice.get_size()
        self.half_lattice_size = int(self.lattice_size / 2)

    def _get_node_to_cluster_map(self) -> dict:

        map_ = dict()
        clusters = self.model.observables["clusters"]
        for index, cluster in enumerate(clusters):

            # Single node clusters are useless here
            if len(cluster) == 1:
                continue
            # Only take finite clusters into account
            if self.has_percolated_helper.has_percolated(cluster):
                continue
            for node in cluster:
                map_[node] = index

        return map_

    def _nodes_belong_to_same_cluster(self, first_node, second_node) -> bool:
        try:
            return bool(
                self.node_to_cluster_map[first_node]
                == self.node_to_cluster_map[second_node]
            )
        except KeyError:
            return False

    def _compute_distance_vector(self, node1, node2):

        x1, y1 = node1
        x2, y2 = node2

        dx = abs(x2 - x1)
        dy = abs(y2 - y1)

        if dx > self.half_lattice_size:
            dx = self.lattice_size - dx

        if dy > self.half_lattice_size:
            dy = self.lattice_size - dy

        return dx, dy

    def calculate(self):
        all_nodes = list(self.lattice.get_all_nodes())
        correlation_function = defaultdict(list)
        correlation_function[(0, 0)] = [
            1
        ]  # Nodes at zero distance are always in the same cluster

        for _ in range(self.SAMPLES):
            first_node, second_node = random.choices(all_nodes, k=2)
            if first_node == second_node:
                continue
            belong_to_same_cluster = self._nodes_belong_to_same_cluster(
                first_node, second_node
            )
            # 2D Specific!
            distance_vector = self._compute_distance_vector(first_node, second_node)
            correlation_function[distance_vector].append(belong_to_same_cluster)

        for distance_vector in correlation_function:
            observations = correlation_function[distance_vector]
            correlation_function[distance_vector] = round(
                sum(observations) / len(observations), 3
            )

        correlation_function = {
            key: value for key, value in correlation_function.items() if value > 0
        }  # Remove keys whose value is 0

        return correlation_function

    def _encode_set_keys_as_str(self, value):
        encoded = dict()
        for key in value:
            strkey = f"{key[0]}_{key[1]}"
            encoded[strkey] = value[key]
        return encoded

    def encode_for_db(self, value):
        return json.dumps(self._encode_set_keys_as_str(value))


class ClusterSizeHistogramCalculation(StatsCalculation):

    BIN_COUNT = 10000

    def calculate(self) -> list:

        clusters = self.model.observables["clusters"]
        number_of_nodes = self.lattice.get_number_of_nodes()
        cluster_sizes = map(lambda cluster: len(cluster) / number_of_nodes, clusters)

        hist, bin_edges = numpy.histogram(
            list(cluster_sizes), bins=self.BIN_COUNT, range=(0, 1)
        )
        cluster_size_histogram = hist.tolist()
        return cluster_size_histogram

    def encode_for_db(self, value):
        return json.dumps(self._encode_list_as_dict(value))


class MeanClusterSizeCalculation(StatsCalculation):

    # Warning: We're assuming that no clusters
    # means average_size = 0

    def calculate(self) -> float:

        clusters = self.model.observables["clusters"]
        clusters = filter(
            lambda cluster: not self.has_percolated_helper.has_percolated(cluster),
            clusters,
        )  # Remove percolating clusters
        cluster_sizes = list(map(lambda cluster: len(cluster), clusters))

        mean_size = 0
        try:
            mean_size = sum(cluster_sizes) / len(cluster_sizes)
        except ZeroDivisionError:
            pass
        return mean_size


class PercolatingClusterStrengthCalculation(StatsCalculation):
    def calculate(self) -> float:
        clusters = self.model.observables["clusters"]
        top_boundary, bottom_boundary = self.lattice.get_boundaries()
        percolating_cluster_size = 0

        for cluster in clusters:
            if self.has_percolated_helper.has_percolated(cluster):
                percolating_cluster_size += len(cluster)

        return percolating_cluster_size / self.lattice.get_number_of_nodes()


class PercolationStatsProcessor(Processor):

    STATS_CLASS_MAP = {
        "has_percolated": HasPercolatedCalculation,
        "cluster_size_histogram": ClusterSizeHistogramCalculation,
        "mean_cluster_size": MeanClusterSizeCalculation,
        "correlation_function": CorrelationFunctionCalculation,
        "percolating_cluster_strength": PercolatingClusterStrengthCalculation,
    }

    def _get_simulation_model_class(self):
        return self.simulation_model_class

    def _get_stats_model_class(self):
        return self.stats_model_class

    def _get_lattice_class(self):
        return self.lattice_class

    def _map_row_to_model(self, row):
        return self._get_simulation_model_class().from_db(row)

    def _get_stats_dict(self, message, model):

        stats_to_compute = message.body["stats"]
        size = message.body["parameters"]["size"]
        probability = message.body["parameters"]["probability"]
        LatticeClass = self._get_lattice_class()
        lattice = LatticeClass(size)
        has_percolated_helper = HasPercolatedHelper(lattice)
        result = dict(
            simulation_id=model.id, probability=model.probability, size=model.size
        )

        try:
            result["initial_size"] = model.initial_size
            result["index"] = model.index
        except:
            pass

        for stats in stats_to_compute:
            StatsClass = self.STATS_CLASS_MAP[stats]
            stats_instance = StatsClass(lattice, model, has_percolated_helper)
            stats_value = stats_instance.calculate()
            encoded_stats_value = stats_instance.encode_for_db(stats_value)

            result[stats] = encoded_stats_value

        return result

    def process(self, message):
        simulation_rows = self._get_simulation_rows(message)
        stats_dicts = []
        for row in simulation_rows:
            simulation_model = self._map_row_to_model(row)
            stats_dict = self._get_stats_dict(message, simulation_model)
            stats_dicts.append(stats_dict)

        self._upsert_stats_rows(stats_dicts)

    def _get_simulation_rows(self, message):

        ids = message.body["ids"]
        size = message.body["parameters"]["size"]
        probability = message.body["parameters"]["probability"]

        ModelClass = self._get_simulation_model_class()
        mysql = MySQL()
        return mysql.fetch(
            ModelClass.get_by_size_probability_ids(ids),
            dict(size=size, probability=probability),
        )

    def _upsert_stats_rows(self, stats_dicts):
        mysql = MySQL()
        start = datetime.now()
        StatsModelClass = self._get_stats_model_class()
        for model_dict in stats_dicts:

            key = "simulation_id"
            key_value = model_dict[key]
            query = StatsModelClass.get_update_query((key, key_value), model_dict)
            mysql.execute(query, model_dict)


# class CorrelationFunctionCalculation(StatsCalculation):

#     SAMPLES = 2 ** 16  # 128K
#     BINS = 10000

#     def __init__(self, lattice, model, has_percolated_helper):
#         self.lattice = lattice
#         self.model = model
#         self.has_percolated_helper = has_percolated_helper
#         self.node_to_cluster_map = self._get_node_to_cluster_map()

#     def _get_pair_distance(self, first_node, second_node) -> float:
#         x1, y1 = first_node
#         x2, y2 = second_node
#         distance = ((y2 - y1) ** 2 + (x2 - x1) ** 2) ** 0.5
#         return distance

#     def _get_node_to_cluster_map(self) -> dict:

#         map_ = dict()
#         clusters = self.model.observables["clusters"]
#         for index, cluster in enumerate(clusters):

#             # Single node clusters are useless here
#             if len(cluster) == 1:
#                 continue
#             # Only take finite clusters into account
#             if self.has_percolated_helper.has_percolated(cluster):
#                 continue
#             for node in cluster:
#                 map_[node] = index

#         return map_

#     def _nodes_belong_to_same_cluster(self, first_node, second_node) -> bool:
#         try:
#             return bool(
#                 self.node_to_cluster_map[first_node]
#                 == self.node_to_cluster_map[second_node]
#             )
#         except KeyError:
#             return False

#     def calculate(self):
#         all_nodes = list(self.lattice.get_all_nodes())
#         bins = [[] for _ in range(self.BINS)]
#         max_distance = self.lattice.get_max_distance()

#         for _ in range(self.SAMPLES):
#             first_node, second_node = random.choices(all_nodes, k=2)
#             if first_node == second_node:
#                 continue
#             belong_to_same_cluster = self._nodes_belong_to_same_cluster(
#                 first_node, second_node
#             )
#             distance = self._get_pair_distance(first_node, second_node)
#             distance_bin = int(round(self.BINS * distance / max_distance))
#             bins[distance_bin].append(belong_to_same_cluster)

#         for index, same_cluster_observations in enumerate(bins):
#             if same_cluster_observations:
#                 bins[index] = sum(same_cluster_observations) / len(
#                     same_cluster_observations
#                 )
#             else:
#                 bins[index] = 0

#         return bins

#     def encode_for_db(self, value):
#         return json.dumps(self._encode_list_as_dict(value))
