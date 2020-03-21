import matplotlib as mpl

from musk.experiments import SimplePercolationExperiment
from musk.metas import ExperimentMeta


experiment = SimplePercolationExperiment(
    ExperimentMeta(
        "exp1", {"occupation_probability": [0.2, 0.4, 0.6, 0.8], "lattice_size": [32],},
    )
)


experiment.run()
# print(simulation.get_lattice().get_state())
# print(experiment._simulations[0].get_lattice().get_state())


for simulation in experiment.get_simulations():
    # clusters = simulation.get_clusters()
    # average_cluster_size = sum([len(cluster) for cluster in clusters]) / len(clusters)
    # cluster_size_ratio = average_cluster_size / simulation.get_lattice().meta.size ** 2

    X.append(simulation.meta.occupation_probability)
    Y.append(cluster_size_ratio)


mpl.plot(X, Y)
mpl.show()
