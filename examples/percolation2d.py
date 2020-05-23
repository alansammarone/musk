import string
import matplotlib.pyplot as plt
import numpy as np
import seaborn
from collections import Counter
from musk import Experiment, Misc, Simulation, Square2DLattice
from musk.core.observables import Observables
from musk.core.plot import Plot


def tanh(X, a, b):
    return (np.tanh(a * (X - b)) + 1) / 2


def exp_with_constant(X, a, b, c):
    return a * np.exp(b * X) + c


class Percolation2DSimulation(Simulation):
    def run(self, p=None, lattice_size=None):

        lattice = Square2DLattice(lattice_size)
        lattice.fill_randomly([0, 1], state_weights=[1 - p, p])
        clusters = lattice.get_clusters_with_state(1)

        boundaries = lattice.get_boundaries()
        top_boundary, bottom_boundary = list(boundaries)
        has_percolated = False
        for cluster in clusters:
            if (cluster & top_boundary) and (cluster & bottom_boundary):
                has_percolated = True
                break

        n_clusters = len(clusters)
        average_cluster_size = sum([len(cluster) for cluster in clusters]) / n_clusters

        return dict(
            clusters=clusters,
            average_cluster_size=average_cluster_size,
            has_percolated=has_percolated,
        )


class ClusterSizePlot(Plot):

    title = "2D percolation (N={lattice_size})"
    ylabel = "Average cluster size"
    xlabel = "Occupation probability"
    filename = "perc_2d_cluster_size.png"
    figure_quality = 3


class ClusterSizePlotAnalyzer:
    def run(self, groups):
        X, Y = [], []
        for group in groups:
            X.append(group.parameters["p"])
            Y.append(
                Observables.get_observable_group_average(group, "average_cluster_size")
            )
        X, Y = np.array(X), np.array(Y)
        ClusterSizePlot(
            X, Y, parameters=group.parameters, fit_fn=exp_with_constant
        ).save()


class PercolationProbabilityPlot(Plot):

    title = "2D percolation (N={lattice_size})"
    ylabel = "Probability of percolation"
    xlabel = "Occupation probability"
    filename = "perc_2d_prob.png"
    figure_quality = 1


class PercolationPlotAnalyzer:
    def run(self, groups):
        X, Y = [], []
        for group in groups:
            X.append(group.parameters["p"])
            Y.append(Observables.get_observable_group_average(group, "has_percolated"))

        PercolationProbabilityPlot(
            X, Y, parameters=group.parameters, fit_fn=tanh
        ).save()


class PercolationProbabilityExperiment(Experiment):

    simulation = Percolation2DSimulation
    parameter_range = {
        "p": [i / 100 for i in range(40, 80, 1)] * 10,
        "lattice_size": [32],
    }

    def analyze(self, groups):

        analyzer = PercolationPlotAnalyzer()
        analyzer.run(groups)


class ClusterSizeExperiment(Experiment):

    simulation = Percolation2DSimulation
    parameter_range = {
        "p": [i / 100 for i in range(35, 75, 1)] * 800,
        "lattice_size": [32],
    }

    def analyze(self, groups):

        analyzer = PercolationPlotAnalyzer()
        analyzer.run(groups)


if __name__ == "__main__":

    experiment = ClusterSizeExperiment(
        storage_folder="data/perc_cluster", delete_artifacts=False,
    )
    experiment.run_simulations()
    experiment.run_analysis()
