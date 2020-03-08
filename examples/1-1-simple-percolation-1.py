from musk.experiments import SimplePercolationExperiment
from musk.metas import ExperimentMeta

experiment = SimplePercolationExperiment(
    ExperimentMeta("exp1", {"occupation_probability": [0.5], "lattice_size": [32],},)
)

experiment.run()
for simulation in experiment.get_simulations():
    clusters = simulation.get_clusters()
    average_cluster_size = sum([len(cluster) for cluster in clusters]) / len(clusters)
    print(average_cluster_size)
    cluster_size_ratio = average_cluster_size / simulation.get_lattice().meta.size ** 2


# "occupation_probability": [p / 100 for p in range(10, 101, 10)],
# "lattice_size": [16] * 20 + [32] * 20,
