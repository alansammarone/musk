from datetime import datetime
import itertools


from musk.core import MySQL, Processor


class StatsCalculation:
    def __init__(self, lattice, model):
        self.lattice = lattice
        self.model = model


class HasPercolatedCalculation(StatsCalculation):
    def calculate(self) -> bool:
        clusters = self.model.observables["clusters"]
        boundaries = self.lattice.get_boundaries()
        top_boundary, bottom_boundary = list(boundaries)
        has_percolated = False
        for cluster in clusters:
            if (cluster & top_boundary) and (cluster & bottom_boundary):
                has_percolated = True
                break
        return has_percolated


class AverageCorrelationLengthCalculation(StatsCalculation):
    def _get_pair_distance(self, nodes):
        node1, node2 = nodes
        x1, y1 = node1
        x2, y2 = node2
        distance = ((y2 - y1) ** 2 + (x2 - x1) ** 2) ** 0.5
        return distance

    def _get_cluster_correlation_length(self, cluster) -> float:
        correlation_length, n_combinations = 0, 0
        combinations = itertools.combinations(cluster, 2)
        distances = list(map(self._get_pair_distance, combinations))

        average_distance = sum(distances) / len(cluster)
        return average_distance

    def calculate(self) -> float:
        clusters = self.model.observables["clusters"]
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

            if cluster_size == 2:
                average += 1
            else:
                average += self._get_cluster_correlation_length(cluster)

        return average / len(clusters)


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
        cluster_size_histogram = {
            size: count
            for size, count in enumerate(cluster_size_histogram)
            if count > 0
        }

        return cluster_size_histogram


class AverageClusterSizeCalculation(StatsCalculation):

    # Warning: We're assuming that no clusters
    # means average_size = 0

    def calculate(self) -> float:
        average_size = 0
        clusters = self.model.observables["clusters"]
        cluster_sizes = map(lambda cluster: len(cluster), clusters)
        try:
            average_size = sum(cluster_sizes) / len(clusters)
            number_of_nodes = self.lattice.get_number_of_nodes()
            average_size = average_size / number_of_nodes
        except ZeroDivisionError:
            pass
        return average_size


class PercolatingClusterStrengthCalculation(StatsCalculation):
    def calculate(self) -> float:
        clusters = self.model.observables["clusters"]
        top_boundary, bottom_boundary = self.lattice.get_boundaries()
        for cluster in clusters:
            cluster_size = len(cluster)

            has_percolated = bool(
                cluster_size >= self.lattice.get_size()
                and (cluster & top_boundary)
                and (cluster & bottom_boundary)
            )
            if not has_percolated:
                continue

            return cluster_size / self.lattice.get_number_of_nodes()
        return 0


class PercolationStatsProcessor(Processor):

    STATS_CLASS_MAP = {
        "has_percolated": HasPercolatedCalculation,
        "cluster_size_histogram": ClusterSizeHistogramCalculation,
        "average_cluster_size": AverageClusterSizeCalculation,
        "average_correlation_length": AverageCorrelationLengthCalculation,
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
        result = dict(simulation_id=model.id)
        for stats in stats_to_compute:
            StatsClass = self.STATS_CLASS_MAP[stats]
            stats_value = StatsClass(lattice, model).calculate()
            result[stats] = stats_value
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
        # end = datetime.now()
        # took = round((end - start).total_seconds(), 2)
        # self._logger.info(f"Inserted rows in {took}s.")


# class PercolationStatsProcessor(Processor):

#     CHUNK_SIZE = 1024

#     STATS_CLASS_MAP = {
#         "has_percolated": HasPercolatedCalculation,
#         "cluster_size_histogram": ClusterSizeHistogramCalculation,
#         "average_cluster_size": AverageClusterSizeCalculation,
#         "average_correlation_length": AverageCorrelationLengthCalculation,
#         "percolating_cluster_strength": PercolatingClusterStrengthCalculation,
#     }

#     def _get_stats_for_model(self, model):
#         return dict(
#             # has_percolated=self._get_has_percolated(model),
#             # cluster_size_histogram=self._get_cluster_size_histogram(model),
#             # average_cluster_size=self._get_average_cluster_size(model),
#             # average_correlation=self._get_average_correlation_length(model),
#             percolating_cluster_strength=self._get_percolating_cluster_strength(model)
#         )

#     def _get_simulation_model_class(self):
#         return self.simulation_model_class

#     def _get_stats_model_class(self):
#         return self.stats_model_class

#     def _get_lattice_class(self):
#         return self.lattice_class

#     def _map_row_to_model(self, row):
#         return self._get_simulation_model_class().from_db(row)

#     def _insert_stats_models(self, models):
#         mysql = MySQL()
#         start = datetime.now()
#         StatsModelClass = self._get_stats_model_class()
#         for model in models:
#             mysql.execute(StatsModelClass.get_insert_query(), model.to_db())
#         end = datetime.now()
#         took = round((end - start).total_seconds(), 2)
#         self._logger.info(f"Inserting chunk took {took}s.")

#     def _update_stats_models(self, models_as_dict):
#         mysql = MySQL()
#         start = datetime.now()
#         StatsModelClass = self._get_stats_model_class()
#         for model_dict in models_as_dict:
#             mysql.execute(
#                 StatsModelClass.get_update_query(
#                     (["simulation_id", model_dict["simulation_id"]]), model_dict
#                 ),
#                 model_dict,
#             )
#         end = datetime.now()
#         took = round((end - start).total_seconds(), 2)
#         self._logger.info(f"Update chunk took {took}s.")

#     def _get_simulation_rows(self, parameters, min_id):
#         mysql = MySQL()
#         SimulationModelClass = self._get_simulation_model_class()
#         query = SimulationModelClass.get_select_query_with_filters(
#             min_id, self.CHUNK_SIZE, **parameters
#         )
#         query_start = datetime.now()
#         mysql_rows = list(mysql.fetch(query, parameters))
#         query_end = datetime.now()
#         query_took = (query_end - query_start).total_seconds()
#         self._logger.debug(f"Stats simulation query took {query_took}s.")
#         return mysql_rows

#     def _process_simulation_chunk(self, chunk, parameters):
#         chunk_took = 0
#         stats_models = []
#         StatsModelClass = self._get_stats_model_class()
#         for result in chunk:
#             start = datetime.now()
#             model = self._map_row_to_model(result)
#             stats_field = self._get_stats_for_model(model)
#             stats_field["simulation_id"] = model.id
#             end = datetime.now()
#             took = (end - start).total_seconds()
#             # stats_model = StatsModelClass(
#             #     simulation_id=model.id,
#             #     size=model.size,
#             #     probability=model.probability,
#             #     created=datetime.now(),
#             #     took=took,
#             #     **stats_field,
#             # )
#             stats_models.append(stats_field)
#             chunk_took += took
#             stats_models.append(stats_field)
#         self._logger.info(f"Chunk processing took {chunk_took:.2f}s.")
#         self._update_stats_models(stats_models)

#     def process(self, message: Message):
#         parameters = message.body["parameters"]
#         LatticeClass = self._get_lattice_class()
#         self.lattice = LatticeClass(parameters["size"])
#         max_id_seen = 0
#         total_count = 0
#         results_chunk = list(self._get_simulation_rows(parameters, max_id_seen))
#         while results_chunk:

#             max_id_seen = max(map(lambda result: result["id"], results_chunk))
#             self._process_simulation_chunk(results_chunk, parameters)
#             total_count += len(results_chunk)
#             self._logger.info("Processed %s input models.", total_count)
#             if len(results_chunk) < self.CHUNK_SIZE:
#                 break
#             results_chunk = list(self._get_simulation_rows(parameters, max_id_seen))

#         self._logger.info("Finished %s input models.", total_count)
