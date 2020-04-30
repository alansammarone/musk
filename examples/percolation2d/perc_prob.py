import matplotlib.pyplot as plt

from musk import Experiment, Simulation, Square2DLattice, Misc
from collections import Counter
import seaborn
import numpy as np
from scipy.optimize import curve_fit


def tanh(X, a, b):
    return (np.tanh(a * (X - b)) + 1) / 2


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
        return {"clusters": clusters, "has_percolated": has_percolated}


class Percolation2DExperiment(Experiment):
    def analyze(self, groups):

        X, Y = [], []
        for parameters, simulation_group in groups:

            p = parameters["p"]
            lattice_size = parameters["lattice_size"]

            n_simulations = 0
            percolation_probability = 0
            for simulation in simulation_group:
                observables = simulation["observables"]
                percolation_probability += observables["has_percolated"]
                n_simulations += 1
            percolation_probability /= n_simulations
            X.append(p)
            Y.append(percolation_probability)

        parameters, _ = curve_fit(tanh, X, Y)
        a, b = parameters

        plt.figure(figsize=(16, 9), dpi=200)
        plt.scatter(X, Y, label="Observation")
        X, Y = np.array(X), np.array(Y)
        plt.plot(X, tanh(X, a, b), "--", label=f"Fit: a={a:.2f}, b={b:.2f}")
        plt.title(f"2D percolation (N={lattice_size})")
        plt.ylabel("Probability of percolation")
        plt.xlabel("Occupation probability")
        plt.legend()
        plt.savefig("perc_2d_prob.png")


if __name__ == "__main__":
    parameter_range = {
        "p": [i / 100 for i in range(40, 80, 1)] * 1000,
        # "p": [0.97],
        "lattice_size": [32],
    }
    experiment = Percolation2DExperiment(
        Percolation2DSimulation,
        parameter_range=parameter_range,
        storage_folder="data/perc_prob",
        delete_artifacts=False,
    )
    experiment.run_simulations()
    experiment.run_analysis()
