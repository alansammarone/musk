import collections
import string
import matplotlib.pyplot as plt
import numpy as np
import seaborn
from collections import Counter
from musk import Experiment, Simulation, Square2DLattice
from musk.core.observables import Observables
from musk.core.plot import Plot
import random


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
    filename = "images/perc_2d_cluster_size.png"
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


class ClusterSizeExperiment(Experiment):

    simulation = Percolation2DSimulation
    parameter_range = {
        "p": [i / 100 for i in range(35, 75, 1)] * 800,
        "lattice_size": [32],
    }

    def analyze(self, groups):

        analyzer = ClusterSizePlotAnalyzer()
        analyzer.run(groups)


class FixedSizePercolationProbabilityPlot(Plot):

    title = "2D percolation (N={lattice_size})"
    ylabel = "Probability of percolation"
    xlabel = "Occupation probability"
    filename = "images/perc_2d_prob_fixed_size.png"
    figure_quality = 3


class FixedSizePercolationPlotAnalyzer:
    def run(self, groups):
        X, Y = [], []
        for group in groups:
            X.append(group.parameters["p"])
            Y.append(Observables.get_observable_group_average(group, "has_percolated"))

        FixedSizePercolationProbabilityPlot(
            X, Y, parameters=group.parameters, fit_fn=tanh
        ).save()


class FixedSizePercolationProbabilityExperiment(Experiment):

    simulation = Percolation2DSimulation
    parameter_range = {
        "p": [i / 100 for i in range(45, 75, 1)] * 10,
        "lattice_size": [32],
    }

    def analyze(self, groups):

        analyzer = FixedSizePercolationPlotAnalyzer()
        analyzer.run(groups)


class PercolationProbabilityPlot(Plot):

    title = "2D percolation"
    ylabel = "Critical probability"
    xlabel = "Lattice size"
    filename = "images/perc_2d_prob.png"
    figure_quality = 2


class PercolationPlotAnalyzer:
    def run(self, groups):

        plot = collections.defaultdict(list)
        plot2 = PercolationProbabilityPlot()
        for group in groups:
            size = group.parameters["lattice_size"]
            plot[size].append(group)

        for size, groups in plot.items():
            X, Y = [], []
            for group in groups:
                X.append(group.parameters["p"])
                Y.append(
                    Observables.get_observable_group_average(group, "has_percolated")
                )
        #     plot2.plot(X, Y, f"N={size}", fit_fn=tanh)

        # plot2.save()

        # if size in [32, 64]:
        #     plt.plot(X, Y)
        # plt.show()

        # if size in plot:

        # plot[size].append(
        #     Observables.get_observable_group_average(group, "has_percolated")
        # )

        # for lattice_size, has_percolated_observations in plot.items():
        #     plt.plot()

        # p_critical =

        # X, Y = [], []
        # for group in groups:
        #     X.append(group.parameters["p"])
        #     Y.append(Observables.get_observable_group_average(group, "has_percolated"))

        # PercolationProbabilityPlot(
        #     X, Y, parameters=group.parameters, fit_fn=tanh
        # ).save()


class PercolationProbabilityExperiment(Experiment):

    simulation = Percolation2DSimulation
    parameter_range = {
        # "lattice_size": [16, 32, 64, 96],
        "lattice_size": [256] * 64,
        # "p": [i / 100 for i in range(50, 70, 1)] * 200,
        "p": [0.59],
    }

    def analyze(self, groups):

        analyzer = PercolationPlotAnalyzer()
        analyzer.run(groups)


if __name__ == "__main__":

    experiment = PercolationProbabilityExperiment(
        storage_folder="data/perc_prob", delete_artifacts=True,
    )
    import random

    random.seed(3323)
    # import cProfile

    # pr = cProfile.Profile()
    # pr.enable()
    experiment.run_simulations()
    # pr.disable()
    # pr.dump_stats("my_prof.prof")

    experiment.run_analysis()
